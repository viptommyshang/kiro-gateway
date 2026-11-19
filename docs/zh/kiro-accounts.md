# kiro-accounts.sh 使用说明

`scripts/kiro-accounts.sh` 是生产宿主机上管理 Kiro 账号的脚本：加账号、停用、恢复、重启并重置状态、查看状态。
下文的命令、输出格式和报错都取自脚本本身。

---

## 1. 用途

网关扫描 `accounts/` 目录，**目录里每个 sqlite 文件就是一个账号**。增删账号就是增删文件再重启，
不用改 `credentials.json`。脚本把这几步收成一条命令，并做以下检查：

- 新文件必须是 kiro-cli 的 sqlite 凭证：有 `auth_kv` 表，且有 key 含 `token` 的记录（只数记录数，不读 token 值）；
- 拒绝与 `accounts/` 里任一文件内容完全相同（sha256）的文件；
- 停用最后一个账号要加 `--force`；
- 任何输出都不包含 token。

| 命令 | 作用 |
|------|------|
| `list` | 列出在用账号、退役文件、`state.json` 里各账号的失败数和请求统计 |
| `add <data.sqlite3> [名字]` | 校验后加入为 `accounts/acct-<名字>.sqlite3`，然后 `restart` |
| `disable <名字> [原因]` | 移到 `$BASE/acct-<名字>.sqlite3.<原因>`（原因默认 `disabled`），然后 `restart` |
| `enable <名字>` | 把退役文件移回 `accounts/`，然后 `restart` |
| `restart` | 停容器 → 备份并删除 `state.json` → 启动 → 等健康 → 打印账号加载日志 |
| `status` | 不重启：健康状态、本次启动的账号加载日志、容器的 `ACCOUNT_*` 环境变量 |

| 选项 | 作用 |
|------|------|
| `--dry-run` | 只打印将执行的命令（`+ mv ...`、`+ docker stop ...`），不做修改 |
| `--force` | 允许 `disable` 最后一个账号 |
| `--move` | 只用于 `add`：移动源文件而不是复制，源文件随后不再存在（见第 4 节） |
| `-h`, `--help` | 显示帮助，退出码 0 |

选项可以放在命令前或后（`--dry-run add x` 与 `add x --dry-run` 相同）。
名字可写成 `c`、`acct-c` 或 `acct-c.sqlite3`，只能包含字母、数字、`-`、`_`。

可用环境变量覆盖的路径（默认值对应生产部署）：

| 变量 | 默认值 | 含义 |
|------|--------|------|
| `BASE` | `/opt/docker/kiro-gateway` | `accounts/`、`state/`、退役文件所在目录 |
| `COMPOSE_DIR` | `$BASE/kiro-gateway` | compose 项目目录（含 `.env`），`status` 用它比对 `.env` |
| `CONTAINER` | `kiro-gateway` | 容器名 |
| `WAIT_TIMEOUT` | `60` | 重启后等待就绪的秒数 |

例：`CONTAINER=kiro-gateway-test WAIT_TIMEOUT=120 bash scripts/kiro-accounts.sh restart`

### 退出码与拒绝情况

所有错误都打印 `错误: ...` 到 stderr，退出码 **1**。常见的拒绝：

| 报错 | 原因 |
|------|------|
| `账号目录不存在` | `BASE` 不对 |
| `<文件>-wal 存在，主文件可能不完整` | kiro-cli 还开着这个库，或没正常退出 |
| `不是 kiro-cli 的 sqlite 凭证` / `没有 token 记录` | 文件不对或登录没完成 |
| `与已在用的 ... 完全相同` | 同一份凭证已在 `accounts/` 里 |
| `... 已存在，换个名字` / `已有同名退役文件` | 名字被在用文件或退役文件占用 |
| `acct-a ~ acct-z 都已占用` | 自动命名用完，显式给名字 |
| `是最后一个账号` | 需要 `--force` |
| `有多个退役文件，请用完整文件名指定` | `enable` 时同名有多个原因后缀 |
| `--move 只能用于 add` | `--move` 用在了别的命令上 |
| `未知命令` | 同时打印帮助 |

不带命令运行时打印帮助并退出 1。`restart` 后容器在 `WAIT_TIMEOUT` 内没就绪，会打印账号加载日志和最近 30 行日志，退出 1。

### restart 对 state.json 做了什么

1. `docker stop kiro-gateway`：网关退出时会把内存状态写回 `state.json`，所以必须先停再删；
2. `cp -p state.json state.json.bak`（覆盖上一次的 `.bak`），然后删除 `state.json`；
3. `docker start kiro-gateway`，等 docker 健康检查为 `healthy` 或容器内 `/health` 返回 200。

结果：各账号的 `failures`、请求统计、`current_account_index`（sticky 索引）、模型到账号的映射全部从零开始。
round_robin 的轮转位置和 429 冷却本来就只在内存里，重启后也是从零开始。
`docker stop/start` **不会重新读取 `.env`**，也不会用新代码，见第 6 节。

---

## 2. 前提

- 在生产宿主机上以 **root** 执行（目录属主是 root，容器 `user: "0:0"`）。
- 服务器上的仓库代码包含本脚本（`git pull`，或单独拷 `scripts/kiro-accounts.sh`）。
- 宿主机有 `sqlite3` 或 `python3`（用于校验文件），`docker` 可用。

目录布局：

```
/opt/docker/kiro-gateway/
├── accounts/                  # 挂载为 /app/accounts，里面每个文件都是一个账号，必须可写
│   └── acct-c.sqlite3
├── state/
│   ├── state.json             # 运行时状态，网关自动写
│   └── state.json.bak         # restart 留下的备份
├── credentials.json           # 挂载为 /app/credentials.json:ro
├── acct-b.sqlite3.revoked     # 退役文件：在 accounts/ 之外，不会被加载
└── kiro-gateway/              # 仓库 checkout = compose 目录，含 .env
```

`credentials.json` 只指向目录，不列具体文件：

```json
[
  {"type": "sqlite", "path": "/app/accounts/"}
]
```

扫描不递归，`accounts/` 里的每个文件都会被检查，不是凭证的会出现 `Skipping invalid credentials file`。

---

## 3. 获取新账号凭证 data.sqlite3

新账号的凭证是 kiro-cli 登录后生成的 `data.sqlite3`。

| 系统 | 位置 |
|------|------|
| Linux（含生产宿主机 root） | `~/.local/share/kiro-cli/data.sqlite3`，root 即 `/root/.local/share/kiro-cli/data.sqlite3` |
| macOS | `~/Library/Application Support/kiro-cli/data.sqlite3`（在 kiro-cli 3.5.0 上看到的位置，不是 `~/.local/share`） |

kiro-cli 没有指定凭证位置的环境变量（见 `Linux部署.md` 第 3 步），同一台机器同一用户只有这一个文件，
所以一次只能登录一个账号，登完就要把文件移走。

在笔记本上登录后拷到服务器也可以（`scp` 到 `/root/` 再 `add`）。但拷贝会留下两份同一凭证，
推荐直接在生产宿主机上登录并用 `add --move`，见下一节。

---

## 4. 在生产宿主机上登录

**可以。** 宿主机没有浏览器，用 device flow：终端打印一个 URL 和一个 code，在你笔记本的浏览器里打开 URL、
输入 code 完成授权，终端随后自动完成登录。依据：

- `kiro-cli login --help` 有 `--use-device-flow`（"Useful for instances where browser redirects cannot be handled"）；
- kiro-cli 的 GitHub issue [#10885](https://github.com/kirodotdev/Kiro/issues/10885)：在 EC2 / SSH 等无浏览器环境里，
  `kiro-cli login` 直接失败，`--use-device-flow` 可用；
- `Linux部署.md` 第 3 步就是在服务器上这样登录的。

Google / GitHub 社交登录据 issue [#4398](https://github.com/kirodotdev/Kiro/issues/4398) 不支持远程环境，
在宿主机上请用 Builder ID 或 IAM Identity Center（`--license pro --identity-provider <start URL> --region <region>`）。

### 步骤

```bash
# 0. 安装 kiro-cli（已装可跳过），见 Linux部署.md 第 1 步
kiro-cli --version

# 1. 先看宿主机上有没有旧的 kiro-cli 登录文件
ls -la /root/.local/share/kiro-cli/
pgrep -af kiro-cli          # 不应有 kiro-cli 进程在跑
```

如果已有 `data.sqlite3`，它可能就是某个在用账号当初的来源。**不要对它运行 `kiro-cli logout` 或 `whoami`**，
直接移到一边（第 7 节解释原因）：

```bash
mkdir -p /root/kiro-cli-old && chmod 700 /root/kiro-cli-old
mv /root/.local/share/kiro-cli/data.sqlite3* /root/kiro-cli-old/
```

```bash
# 2. 登录新账号（Identity Center 账号加上 --license pro --identity-provider ... --region ...）
kiro-cli login --use-device-flow
#    把打印的 URL 复制到笔记本浏览器的无痕窗口打开，输入 code，用新账号登录

# 3. 确认身份：Email 必须是新用户，不能是 accounts/ 里已有账号的用户
kiro-cli whoami

# 4. 确认 kiro-cli 已退出，然后把文件"移"进 accounts/
pgrep -af kiro-cli
cd /opt/docker/kiro-gateway/kiro-gateway
bash scripts/kiro-accounts.sh --dry-run add --move /root/.local/share/kiro-cli/data.sqlite3 d
bash scripts/kiro-accounts.sh add --move /root/.local/share/kiro-cli/data.sqlite3 d

# 5. 不要再执行 kiro-cli logout
```

`--move` 与普通 `add` 做同样的校验（`auth_kv`、token 记录、`-wal`、sha256 重复），区别只是用 `mv` 代替 `cp`，
`--dry-run` 下同样只打印。移走之后宿主机上只剩网关那一份凭证。

登第二个账号：重复第 2～4 步。因为上一个 `data.sqlite3` 已经移走，这次登录会生成新文件，不会覆盖上一个账号。
浏览器那边一定要用新的无痕窗口：device flow 的身份取自浏览器里的 IAM Identity Center 会话，
浏览器还登着上一个用户时会直接用旧身份批准（见 `Linux部署.md` 第 3 步）。

如果 `add` 报 `-wal 存在`：先确认 kiro-cli 已退出；仍然存在就用
`sqlite3 /root/.local/share/kiro-cli/data.sqlite3 ".backup /root/new.sqlite3"` 导出，
`add --move /root/new.sqlite3 d`，然后删掉原文件 `rm /root/.local/share/kiro-cli/data.sqlite3*`（不要 logout）。

### 风险与不确定的地方

- **refresh_token 轮换。** 网关刷新 token 后会把新的 refresh_token 写回自己那份 sqlite（`kiro/auth.py` 的
  `_save_credentials_to_sqlite`），kiro-cli 自己那份不会更新。两份从此分叉：哪一份先刷新，另一份就可能作废。
  所以原文件交给网关后不能再被 kiro-cli 使用，优先用 `--move` 而不是复制。
  AWS SSO OIDC 每次刷新是否一定轮换 refresh_token，这里没有验证过；代码在返回新 token 时才替换。
- **`kiro-cli logout` 可能作废会话。** 没有验证过 logout 只删本地文件还是会在服务端注销这次登录。
  如果是后者，"拷走再 logout"会让刚加进网关的账号失效。所以文件移走后不再 logout。
- **同一台机器登录第二个账号会覆盖同一个 `data.sqlite3`。** 两次登录之间必须把文件移走。
- 移走 `data.sqlite3` 后再 `kiro-cli login` 会新建一个干净的库、不报 `Already logged in`，这是预期行为，本仓库没有实测。
  如果仍报 `Already logged in`，先停下来查 kiro-cli 读的是哪个文件，不要直接 logout。
- `data.sqlite3` 里也有 kiro-cli 的其它本地状态，移走后宿主机上的 kiro-cli 相当于全新安装。生产宿主机上一般无所谓。
- 本节基于 kiro-cli 3.5.0 的 `--help` 和仓库文档；生产宿主机上的 kiro-cli 版本可能不同，先看 `kiro-cli login --help`。

---

## 5. 常用场景

以下命令都在 compose 目录执行：

```bash
cd /opt/docker/kiro-gateway/kiro-gateway
```

### 加账号

```bash
bash scripts/kiro-accounts.sh --dry-run add /root/acct-d.sqlite3 d    # 先看会做什么
bash scripts/kiro-accounts.sh add /root/acct-d.sqlite3 d              # 复制
bash scripts/kiro-accounts.sh add --move /root/.local/share/kiro-cli/data.sqlite3 d   # 移动
```

不给名字时自动取下一个没用过的字母（退役文件占用的字母也算用过）。输出示例：

```
==> 将加入账号: /root/acct-d.sqlite3 → /opt/docker/kiro-gateway/accounts/acct-d.sqlite3（属主 0:0，权限 600），然后重启
  + cp /root/acct-d.sqlite3 /opt/docker/kiro-gateway/accounts/acct-d.sqlite3.tmp
  + chown 0:0 /opt/docker/kiro-gateway/accounts/acct-d.sqlite3.tmp
  + chmod 600 /opt/docker/kiro-gateway/accounts/acct-d.sqlite3.tmp
  + mv /opt/docker/kiro-gateway/accounts/acct-d.sqlite3.tmp /opt/docker/kiro-gateway/accounts/acct-d.sqlite3
==> 将执行：停止容器 → 备份并删除 state.json → 启动容器 → 等待健康
  （先停再删：网关退出时会把内存里的状态写回 state.json）
  + docker stop kiro-gateway
  + cp -p /opt/docker/kiro-gateway/state/state.json /opt/docker/kiro-gateway/state/state.json.bak
  + rm -f /opt/docker/kiro-gateway/state/state.json
  + docker start kiro-gateway
==> 等待容器就绪（最长 60s）
  健康: running/healthy（8s）
==> 账号加载日志
... Scanning folder for credentials: /app/accounts/
... Loaded 2 account(s) from credentials
... Initialized account: /app/accounts/acct-c.sqlite3 (21 models)
```

### 停用被回收 / 没额度的账号

```bash
bash scripts/kiro-accounts.sh disable c revoked    # → /opt/docker/kiro-gateway/acct-c.sqlite3.revoked
bash scripts/kiro-accounts.sh disable b noquota
bash scripts/kiro-accounts.sh --force disable c    # 最后一个账号，确认要停才加 --force
```

| 现象 | 含义 | 处理 |
|------|------|------|
| `403` + 反复 `refreshing token` | 账号被回收 / 无权限 | `disable <名字> revoked` |
| `402` / `MONTHLY_REQUEST_COUNT` | 月额度用完 | `disable <名字> noquota`，下月 `enable` |
| `429` | 限流 | 一般不用停用 |
| `504` / 超时 | 网络或上游问题 | 别急着换账号 |

### 恢复账号

```bash
bash scripts/kiro-accounts.sh enable b                        # 只有一个 acct-b.sqlite3.* 时
bash scripts/kiro-accounts.sh enable acct-b.sqlite3.noquota   # 有多个时写完整文件名
```

恢复前同样做 sha256 重复检查。

### 更换账号（新账号替换旧账号）

先加新的、确认能用，再停旧的，中间不会出现零账号：

```bash
bash scripts/kiro-accounts.sh add --move /root/.local/share/kiro-cli/data.sqlite3 e
bash scripts/kiro-accounts.sh status       # 看到 Loaded 2 account(s)
bash scripts/kiro-accounts.sh disable c revoked
```

每条改动命令都会重启一次容器。确认新账号稳定后可以删掉退役文件。

### 查看状态

```bash
bash scripts/kiro-accounts.sh list
```

```
==> 在用账号（/opt/docker/kiro-gateway/accounts）
  acct-c.sqlite3  77824 字节  修改于 11-19 01:58:12
  acct-d.sqlite3  77824 字节  修改于 11-19 02:10:05
  共 2 个
==> 退役文件（/opt/docker/kiro-gateway/*.sqlite3.*，不会被加载）
  acct-b.sqlite3.revoked
==> state.json（/opt/docker/kiro-gateway/state/state.json）
  粘性索引 current_account_index = 0
  acct-c.sqlite3           failures=0   上次失败=11-18 23:44:20  总=47182 成功=11849 失败=35318
```

```bash
bash scripts/kiro-accounts.sh status
```

```
==> 容器 kiro-gateway: running/healthy
  启动时间: 2026-11-19T02:10:03.123Z
==> 账号加载日志
... Scanning folder for credentials: /app/accounts/
... Loaded 2 account(s) from credentials
... Initialized account: /app/accounts/acct-c.sqlite3 (21 models)
==> 容器环境变量 ACCOUNT_*
ACCOUNTS_CONFIG_FILE=/app/credentials.json
ACCOUNTS_STATE_FILE=/app/state/state.json
ACCOUNT_SYSTEM=true
```

`.env` 里的 `ACCOUNT_SELECTION` 与容器里不一致时，`status` 会多打印一行
`注意: .env 中 ACCOUNT_SELECTION='...'，容器中为 '...'`。

账号是按需初始化的：启动时只初始化第一个能用的账号，其余账号第一次被选中时才出现 `Initialized account`。
过一段时间再 `list`，各账号的请求统计都在增长，才说明流量确实分到了多个账号。

---

## 6. 账号选择模式

由 `.env` 的 `ACCOUNT_SELECTION` 控制（需要 `ACCOUNT_SYSTEM=true`）。

| 模式 | 行为 |
|------|------|
| `round_robin`（**默认**） | 每个请求从下一个账号开始，流量平均分到各账号。某账号 429 时冷却 `ACCOUNT_RATE_LIMIT_COOLDOWN` 秒（默认 30），同一请求立刻换下一个账号，不在原账号上重试。所有可用账号都在冷却时返回 `429` + `Retry-After` |
| `sticky` | 所有请求走上一次成功的账号，它出错（429/402/403/网络错误）才切到下一个，配额一个接一个用完 |

没写 `ACCOUNT_SELECTION` 或写了无法识别的值，都按 `round_robin`。容器环境变量里没有 `ACCOUNT_SELECTION` 行也是 round_robin。

**只有一个账号时两种模式行为相同**，可以放心保持默认：

- 每个请求都用这个账号，忽略熔断计数和冷却；
- 429 在同一账号上按 1s、2s、4s 退避重试 3 次，仍是 429 就把 Kiro 的真实 429 返回给客户端；
- 不设 30 秒冷却，下一个请求照常发出，不会出现 "All available accounts are rate limited"；
- 402 / 403 也原样返回真实状态码和错误信息。

有两个以上账号但其中一个已失效（比如被回收还没 `disable`）时，round_robin 下另一个账号 429 后会进冷却，
冷却期内的请求可能直接拿到 `429 + Retry-After`。失效账号要及时 `disable`。

### 修改模式

在 `/opt/docker/kiro-gateway/kiro-gateway/.env` 写：

```bash
ACCOUNT_SELECTION=round_robin     # 或 sticky
ACCOUNT_RATE_LIMIT_COOLDOWN=30
```

然后**必须用 `docker compose up -d`**，`restart`（`docker stop/start`）不会重新读取 `.env`。
默认值是代码里的，镜像是构建时拷代码进去的，所以第一次用到 round_robin 默认值的版本要重新构建：

```bash
cd /opt/docker/kiro-gateway/kiro-gateway
docker compose build          # 代码更新后才需要
docker compose up -d
bash scripts/kiro-accounts.sh status
```

旧镜像里默认是 sticky；只有重新构建后，不写 `ACCOUNT_SELECTION` 才是 round_robin。

### 取舍

- round_robin 下所有账号的配额**同速消耗**，可能在同一时间一起用完；sticky 是一个接一个地用完。
- round_robin 下每个账号都在被使用，refresh_token 持续刷新，不会有备用账号闲置过期。
- 刚启动时各账号第一次被选中要初始化（刷 token），最初几个请求稍慢。
- 多个身份并行使用可能被 Kiro 视为账号共享，请自行确认服务条款。

---

## 7. 注意事项

- **只加不同的用户。** 判断依据是 `kiro-cli whoami` 的 Email。同一用户登录两次会得到不同的 token，
  但共用同一份配额，加进来不增加额度，还会一起撞限流。
- **同一份凭证不要有两份。** 两份拷贝共用一个 refresh_token，一份刷新后另一份就可能失效。
  脚本只能拦住内容完全相同的文件；网关刷新过 token 后文件内容已经变了，从别处拷来的旧副本拦不住，要自己避免。
  这也是推荐 `add --move` 的原因。
- **`accounts/` 必须可写。** token 刷新后会写回 sqlite。容器 `user:` 必须与目录属主一致（生产是 root），
  否则重启后用的是已作废的旧 refresh_token，认证失败。
- **不要打印 token。** 不要 `cat`、`sqlite3 ... SELECT value` 凭证文件，也不要把 `auth_kv` 的内容贴到聊天或工单里。
  需要核对是否同一份凭证时，用 `Linux部署.md` 里的 refresh_token 指纹脚本（只打印 sha256 前 16 位）。
- 不要手动往 `accounts/` 里拷文件或改名，交给脚本，它会做校验、设权限、重置状态。
- 退役文件放在 `accounts/` 之外，不会被加载；确认不再需要后可以删除。

---

## 8. 排障

```bash
docker logs --since 1h kiro-gateway 2>&1 | grep -E "<下表中的模式>"
```

| 日志 | 含义 |
|------|------|
| `Loaded N account(s) from credentials` | 加载了 N 个文件，应等于 `list` 里的在用数 |
| `Skipping invalid credentials file` | `accounts/` 里有不是凭证的文件 |
| `Initialized account:` / `Failed to initialize account` | 账号初始化成功 / 失败（凭证坏了或 token 刷新失败） |
| `Account ... rate-limited: reason=..., cooldown=30s` | round_robin 多账号：该账号 429，进入冷却 |
| `Received 429, not retrying on this account (failover enabled)` | round_robin 多账号：429 不在原账号重试，换下一个 |
| `Received 429, waiting 1.0s (attempt 1/3)` | 在同一账号上退避重试（单账号或 sticky） |
| `Retries exhausted for HTTP 429` | 重试用完，把真实 429 返回 |
| `All available accounts are rate limited. Retry after Ns.` | 所有可用账号都在冷却，返回 429 + `Retry-After` |
| `Account ... failure #N: status=..., reason=...` | 熔断计数增加（402/403/网络错误，sticky 下也含 429） |
| `Half-Open state for` / `Probabilistic retry for broken account` | 熔断中的账号被重新尝试 |
| `No available accounts for this model.` / `All accounts failed after full circle.` | 多账号全部不可用，返回 503 |
| `Received 403, refreshing token` | token 过期或账号被回收；反复出现就 `disable ... revoked` |
| `Token refresh failed with 400, reloading credentials from SQLite` / `AWS SSO OIDC refresh failed` | refresh_token 已作废，常见原因是同一凭证有两份 |
| `Connection pool exhausted, not retrying` | 网关本地连接池满，不是账号问题 |

常见问题：

| 现象 | 处理 |
|------|------|
| 加了账号但 `list` 里只有一个账号的统计在增长 | `status` 看 `ACCOUNT_SELECTION`；sticky 或旧镜像下就是这样，见第 6 节 |
| `status` 提示 `.env` 与容器不一致 | 在 compose 目录执行 `docker compose up -d` |
| `login` 报 `Already logged in` | 已有 `data.sqlite3`；按第 4 节第 1 步把它移走，不要 logout |
| 登录后 `whoami` 还是旧账号 | 用新的无痕窗口重开设备 URL |
| `add` 报 `-wal` 存在 | 先退出 kiro-cli，或用 `.backup` 导出，见第 4 节 |
| `profileArn is required` | `.env` 的 `PROFILE_ARN` 没配，见 `Linux部署.md` 第 7 步 |
