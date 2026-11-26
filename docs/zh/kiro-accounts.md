# Kiro 账号切换与发布

两部分：一、切换账号（`scripts/kiro-accounts.sh`）；二、发布（把 kiro-gateway 新代码更新到生产）。
所有命令都在生产宿主机上以 root 执行，默认在 compose 目录：

```bash
cd /opt/docker/kiro-gateway/kiro-gateway
```

---

## 一、切换账号

网关扫描 `/opt/docker/kiro-gateway/accounts/`，目录里**每个 sqlite 文件就是一个账号**，不用改 `credentials.json`。
默认 `round_robin`，请求轮流分到各账号；只有一个账号时行为与以前相同（429 原样返回，不进冷却）。

### 登录新账号（在生产宿主机上）

宿主机没有浏览器，用 device flow：终端打印 URL 和 code，在笔记本浏览器里完成授权。

```bash
# 1. 已有旧的 kiro-cli 登录文件就先移开（不要 logout）
pgrep -af kiro-cli                       # 不应有 kiro-cli 进程
mkdir -p /root/kiro-cli-old && chmod 700 /root/kiro-cli-old
mv /root/.local/share/kiro-cli/data.sqlite3* /root/kiro-cli-old/ 2>/dev/null

# 2. 登录新账号（IAM Identity Center 账号加 --license pro --identity-provider <start URL> --region <region>）
kiro-cli login --use-device-flow
#    把 URL 复制到笔记本浏览器的【无痕窗口】打开，输入 code，用新账号登录

# 3. 确认身份：Email 必须是 accounts/ 里还没有的用户
kiro-cli whoami

# 4. 把凭证移进 accounts/（会校验并重启网关）
bash scripts/kiro-accounts.sh add --move /root/.local/share/kiro-cli/data.sqlite3 d
```

再登一个账号就重复第 2～4 步。注意：

- **只加不同的用户**（看 `whoami` 的 Email）。同一用户登两次共用一份配额，加了没用。
- **浏览器用新的无痕窗口**，否则 device flow 会沿用浏览器里上一个用户的会话。
- **用 `--move`，不要保留两份同一凭证**。网关刷新 token 后，另一份可能作废。
- **移走后不要执行 `kiro-cli logout`**。
- 生产上的 kiro-cli 版本可能不同，先看 `kiro-cli login --help`。

未验证：`kiro-cli logout` 是否会在服务端注销会话；每次刷新是否一定轮换 refresh_token。所以按上面最保守的做法操作。

### 常用命令

```bash
bash scripts/kiro-accounts.sh list                       # 在用账号、退役文件、state.json 统计
bash scripts/kiro-accounts.sh add <data.sqlite3> [名字]   # 校验后复制为 accounts/acct-<名字>.sqlite3，然后 restart
bash scripts/kiro-accounts.sh add --move <文件> [名字]    # 同上，但移动文件（推荐）
bash scripts/kiro-accounts.sh disable <名字> [原因]       # 移出 accounts/ 成 acct-<名字>.sqlite3.<原因>，然后 restart
bash scripts/kiro-accounts.sh enable <名字>               # 把退役文件移回 accounts/，然后 restart
bash scripts/kiro-accounts.sh status                     # 不重启：健康状态、账号加载日志、ACCOUNT_* 变量
bash scripts/kiro-accounts.sh restart                    # 停容器 → 备份并清空 state.json → 启动 → 等健康
# 选项：--dry-run 只打印要执行的命令；--force 允许 disable 最后一个账号
```

`restart` 会把 `state/state.json` 备份成 `state.json.bak` 再删除，失败计数和统计从零开始。
它只是 `docker stop/start`，**不会重新读取 `.env`**。名字可写 `c`、`acct-c` 或 `acct-c.sqlite3`。

### 账号池（自动剔除 + 一键重置）

`accounts/` 里的所有账号组成账号池，请求按 `round_robin` 在**池内**账号之间轮流分配。网关会自动维护池子：

| 情况 | 处理 | 何时回池 |
|---|---|---|
| 402 / `MONTHLY_REQUEST_COUNT` 等额度用完 | 立即剔除，不再分请求（也不做试探） | `ACCOUNT_QUOTA_EXHAUSTED_TTL` 秒后（默认 6 小时）自动回池试一次，仍无额度再剔除 |
| 403（刷新 token 后仍被拒）/ 初始化失败 | 标记凭证失效并剔除 | `ACCOUNT_INVALID_TTL` 秒后（默认 1 小时） |
| 429 限流 | 短暂冷却 `ACCOUNT_RATE_LIMIT_COOLDOWN` 秒，不剔除 | 冷却结束 |
| 被剔除的账号又成功了 | 立即回池 | — |

剔除状态写进 `state.json`，网关重启后仍然生效。池子空了时，接口返回 503，说明每个账号的原因、最早回池时间，以及重置方法。

```bash
bash scripts/kiro-accounts.sh pool          # 看池子：每个账号 可用 / 限流冷却 / 无额度(已剔除) / 凭证失效(已剔除)、多久后回池
bash scripts/kiro-accounts.sh reset         # 一键重置：所有已登录账号恢复为可用（不重启、不丢统计）
bash scripts/kiro-accounts.sh reset --all   # 同上，并把 disable 退役的账号文件全部移回 accounts/（会重启）
bash scripts/kiro-accounts.sh login d       # 本机 kiro-cli 设备码登录一个新账号并直接加入池子，可连续执行
```

`reset` 适合月初额度刷新后、或手工确认账号恢复后使用。也可以直接调接口：
`GET /admin/accounts`、`POST /admin/accounts/reset`（`Authorization: Bearer <PROXY_API_KEY>`，不返回任何凭证）。

`login` 会先把本机原有的 kiro-cli 登录文件移开（不执行 logout），登录完成后把新凭证移进 `accounts/`，再把原来的登录移回。设备码 URL 要用浏览器**无痕窗口**打开。

### 典型场景

加账号：

```bash
bash scripts/kiro-accounts.sh --dry-run add --move /root/.local/share/kiro-cli/data.sqlite3 d
bash scripts/kiro-accounts.sh add --move /root/.local/share/kiro-cli/data.sqlite3 d
```

下线失效或没额度的账号（403 反复刷 token 用 `revoked`，402 月额度用完用 `noquota`，下月 `enable c`）：

```bash
bash scripts/kiro-accounts.sh disable c noquota
```

换账号（先加新的再停旧的，中间不会零账号）：

```bash
bash scripts/kiro-accounts.sh add --move /root/.local/share/kiro-cli/data.sqlite3 e
bash scripts/kiro-accounts.sh disable c revoked
```

确认生效：

```bash
bash scripts/kiro-accounts.sh status     # 日志里应有 Loaded N account(s)，N = 在用账号数
bash scripts/kiro-accounts.sh list       # 过一段时间各账号的请求统计都在增长
```

### 账号选择模式

- `ACCOUNT_SELECTION=round_robin`（默认）：每个请求换下一个账号；某账号 429 后冷却 `ACCOUNT_RATE_LIMIT_COOLDOWN` 秒（默认 30），同一请求立刻换账号。各账号配额同速消耗。
- `ACCOUNT_SELECTION=sticky`：一直用同一个账号，出错才切下一个，配额一个接一个用完。
- 改 `.env` 后执行 `docker compose up -d` 才生效（`kiro-accounts.sh restart` 不读 `.env`）。

### 排障

```bash
docker logs --since 1h kiro-gateway 2>&1 | grep -E "Loaded .* account|Initialized account|Failed to initialize"
docker logs --since 1h kiro-gateway 2>&1 | grep -E "rate-limited|All available accounts are rate limited"
docker logs --since 1h kiro-gateway 2>&1 | grep -E "Received 403|Token refresh failed|AWS SSO OIDC refresh failed"
docker logs --since 1h kiro-gateway 2>&1 | grep -c "PoolTimeout"
```

依次对应：账号加载、429 冷却、凭证失效（`disable ... revoked` 后重新登录）、本地连接池耗尽（不是账号问题）。

---

## 二、发布（更新 kiro-gateway 到生产）

生产布局：compose 目录 `/opt/docker/kiro-gateway/kiro-gateway`（本仓库的 git checkout，含 `.env`，容器名 `kiro-gateway`），
挂载 `/opt/docker/kiro-gateway/{accounts,state,credentials.json}`。代码是 `docker compose build` 时拷进镜像的，改代码必须重新构建。

生产代码按 `Linux部署.md` 第 4 步从上游 `https://github.com/Jwadow/kiro-gateway.git` clone，第 5 步再在本地改 `kiro/config.py`。
我们的修复只在开发机上，上游没有，所以**不能 `git pull`**，用补丁发布。

### 1. 备份并记下当前版本

```bash
cd /opt/docker/kiro-gateway
cp -a accounts accounts.bak-$(date +%F)
cp -a state state.bak-$(date +%F)
cd kiro-gateway
git log -1 --oneline        # 记下 <生产commit>，回滚用
git status --short          # 看本地改动（通常是第 5 步改的 kiro/config.py）
```

### 2. 打补丁

开发机上（`<生产commit>` 用上一步的值，必须在开发机历史里）：

```bash
git format-patch <生产commit>..HEAD -o /tmp/kiro-patches
scp -r /tmp/kiro-patches root@<生产主机>:/tmp/
```

生产上。仓库里已提交了第 5 步对 `kiro/config.py` 的修改，先把本地改动收起来，否则 `git am` 会冲突：

```bash
git stash
git am /tmp/kiro-patches/*.patch      # 失败就 git am --abort，再 git stash pop 恢复
```

### 3. 检查 .env

```bash
grep -E '^(ACCOUNT_|FIRST_TOKEN_)' .env
```

- 必须有 `ACCOUNT_SYSTEM=true`；`ACCOUNT_SELECTION` 可不写（默认 round_robin）。
- 生产当前是 `FIRST_TOKEN_MAX_RETRIES=1`、`FIRST_TOKEN_TIMEOUT=90`。修复连接池泄漏后，等重度客户端遇到 429 会退避了，可以把重试改回 2～3。

### 4. 构建并启动

```bash
docker compose build && docker compose up -d
```

Kiro 请求会中断约 10 秒。`up -d` 会重建容器并读取新的 `.env`；`docker restart` 和 `kiro-accounts.sh restart` 都不会。

### 5. 验证

```bash
bash scripts/kiro-accounts.sh status                              # healthy，Loaded N account(s)
curl -s -o /dev/null -w "%{http_code}\n" localhost:8000/health     # 200
docker logs --since 10m kiro-gateway 2>&1 | grep -c PoolTimeout   # 应为 0 或很少
```

### 6. 回滚

```bash
git reset --hard <生产commit>     # 回到第 1 步记下的版本
git stash pop                     # 恢复原来的本地改动
docker compose build && docker compose up -d
```

如果 `git am` 中途失败，先 `git am --abort`，代码就没有变化，不用重新构建。
