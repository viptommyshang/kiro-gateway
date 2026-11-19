# Kiro Gateway 部署（双账号）

内网访问，不对外暴露，端口不做绑定限制。

---

## 1. 安装 kiro-cli

```bash
curl -fsSL https://cli.kiro.dev/install | KIRO_CLI_SKIP_SETUP=1 bash
export PATH="$HOME/.local/bin:$PATH"
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
kiro-cli --version
```

## 2. 建目录

```bash
sudo mkdir -p /opt/docker/kiro-gateway/{accounts,state}
sudo chown -R $(id -u):$(id -g) /opt/docker/kiro-gateway
id -u; id -g          # 记下这两个值，第 7 步用
```

## 3. 登录两个账号

> 这里是首次部署。上线后新增、停用、恢复账号用 `scripts/kiro-accounts.sh`，步骤见 `账号更换.md`。

kiro-cli **没有**指定凭证位置的环境变量 —— `XDG_DATA_HOME` 对它无效（实测 macOS 和
Linux 都忽略，二进制里也没有任何 `KIRO_CLI_*DIR` 之类的变量）。所以只能串行登录、
用 `logout` 切换。凭证始终写在 `~/.local/share/kiro-cli/data.sqlite3`，登录后立刻拷走。

终端会打印 Code 和 URL，在浏览器打开并输入 Code 授权。

```bash
# 若当前已是登录态，先退出（否则 login 会报 Already logged in）
kiro-cli logout

# 账号 A
kiro-cli login --use-device-flow
kiro-cli whoami                     # 必须显示账号信息，否则登录未完成
cp ~/.local/share/kiro-cli/data.sqlite3 /opt/docker/kiro-gateway/accounts/acct-a.sqlite3

# 切换（务必先拷完再 logout）
kiro-cli logout

# 账号 B
kiro-cli login --use-device-flow
kiro-cli whoami                     # 确认是另一个账号，不是 A
cp ~/.local/share/kiro-cli/data.sqlite3 /opt/docker/kiro-gateway/accounts/acct-b.sqlite3

ls -la /opt/docker/kiro-gateway/accounts/     # 应有 2 个 .sqlite3
```

### ⚠ 关键：device flow 的身份取自浏览器会话

终端只显示一个 code 并等待结果，**真正的身份认证发生在浏览器里**。打开设备 URL 时，
如果该域名下已有活跃的 IAM Identity Center 会话，它会**直接用那个身份批准，不问密码**。

`kiro-cli logout` 只清除服务器上的本地凭证，**动不了浏览器里的 IdC 会话** —— 所以
第二次登录默认还是同一个账号。

登账号 B 时，必须换掉浏览器会话，三种做法任选：

| 做法 | 操作 |
|------|------|
| 无痕窗口（推荐） | 把设备 URL 复制到无痕/隐私窗口打开，会要求重新登录 |
| 浏览器登出 IdC | 先访问 `https://d-xxxxxxxxx.awsapps.com/start/` 退出，再开设备 URL |
| 换浏览器 | 用另一个浏览器或另一个浏览器 profile |

两个 `whoami` 输出里的 **Email 行**是唯一可靠判据，必须不同。若都是同一个邮箱，
两个文件就是同一账号，故障转移形同虚设 —— 共用同一份配额，acct-a 撞限流时 acct-b
也一样撞。

`whoami` 还会打印 profile ARN，例如
`arn:aws:codewhisperer:us-east-1:901635709197:profile/XXXXXXXXXX`。
后面 gateway 若报 `profileArn is required`，填这个值。

### 核对两个文件是否真的是不同账号

```bash
for f in /opt/docker/kiro-gateway/accounts/*.sqlite3; do
  echo "=== $(basename "$f")"
  python3 - "$f" <<'PYCHK'
import hashlib, json, sqlite3, sys
c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
rows = dict(c.execute("SELECT key, value FROM auth_kv").fetchall())
for k in ("kirocli:social:token", "kirocli:odic:token", "codewhisperer:odic:token"):
    if k not in rows:
        continue
    t = json.loads(rows[k])
    print("  token key =", k)
    for name in ("startUrl", "start_url", "region"):
        if t.get(name):
            print(f"  {name} = {t[name]}")
    for name in ("refresh_token", "refreshToken"):
        if t.get(name):
            print("  refresh_token 指纹 =",
                  hashlib.sha256(t[name].encode()).hexdigest()[:16])
    break
else:
    print("  ⚠ 未找到 token，该文件不是完整凭证")
PYCHK
done
```

指纹**相同** = 同一份凭证的两份拷贝，需要重做账号 B。

指纹**不同**只能说明是两次独立的登录事件，**不代表是两个不同身份** —— 同一个用户
登录两次同样会拿到不同的 refresh_token。判断身份的唯一可靠依据是每次登录后
`kiro-cli whoami` 输出的 **Email 行**。

若当时没记录，可临时挂载凭证补测（自行控制备份与还原时机）：

```bash
cd ~/.local/share/kiro-cli
cp data.sqlite3 data.sqlite3.keep
cp /opt/docker/kiro-gateway/accounts/acct-a.sqlite3 data.sqlite3
kiro-cli whoami
cp data.sqlite3.keep data.sqlite3 && rm data.sqlite3.keep
```

确认两个文件是同一账号时，故障转移没有实际作用（共用同一份配额），不如只留一个并
去掉 `ACCOUNT_SYSTEM=true` 走单账号模式 —— 代码里单账号会绕过熔断器，直接透传真实
的 Kiro 错误，排查更直观。

## 4. 拉代码

克隆位置随意，后续第 5~9 步的命令都在**仓库根目录**执行。

```bash
cd /opt/docker/kiro-gateway          # 或任意目录
git clone https://github.com/Jwadow/kiro-gateway.git
cd kiro-gateway
```

第 7 步的 `volumes` 里 `/opt/docker/kiro-gateway/...` 是绝对路径，不受克隆位置影响；
`./debug_logs` 相对于 compose 文件所在目录，即仓库根目录。

## 5. 改一行源码

上游默认把 `auto` 在 `/v1/models` 里显示成 `auto-kiro`（作者为规避 Cursor IDE 的
`auto` 模型冲突而造的别名，Kiro 本身没有这个模型）。该别名**实测无法调用** ——
出现在列表里，调用返回 `400 Invalid model ID`。

```bash
python3 - <<'EOF'
import pathlib, re
p = pathlib.Path("kiro/config.py")
s = p.read_text(encoding="utf-8")
s, n1 = re.subn(r"MODEL_ALIASES: Dict\[str, str\] = \{[^}]*\}",
                "MODEL_ALIASES: Dict[str, str] = {}", s, count=1)
s, n2 = re.subn(r"HIDDEN_FROM_LIST: List\[str\] = \[[^\]]*\]",
                "HIDDEN_FROM_LIST: List[str] = []", s, count=1)
p.write_text(s, encoding="utf-8")
print(f"替换计数 MODEL_ALIASES={n1} HIDDEN_FROM_LIST={n2}")
EOF
```

两个计数都应为 1。若为 0，说明已经改过了（幂等，重复执行无害）。

也可以直接编辑 `kiro/config.py`，把这两处改成：

```python
MODEL_ALIASES: Dict[str, str] = {}
HIDDEN_FROM_LIST: List[str] = []
```

校验（**不要用 `import kiro.config`** —— 它会触发 `dotenv` 等依赖，
纯 Docker 部署时宿主机上没有这些包）：

```bash
grep -E "^(MODEL_ALIASES|HIDDEN_FROM_LIST)" kiro/config.py
grep -c '"modelId"' kiro/config.py     # FALLBACK_MODELS 条数
```

期望前两行是 `{}` 和 `[]`。要更严谨可以用 AST 解析，同样不依赖第三方包：

```bash
python3 - <<'EOF'
import ast, pathlib
tree = ast.parse(pathlib.Path("kiro/config.py").read_text(encoding="utf-8"))
want = ("MODEL_ALIASES", "HIDDEN_FROM_LIST", "FALLBACK_MODELS")
found = {}
for node in tree.body:
    target = getattr(node, "target", None) or (node.targets[0] if getattr(node, "targets", None) else None)
    name = getattr(target, "id", None)
    if name in want:
        found[name] = ast.literal_eval(node.value)
print("MODEL_ALIASES    =", found.get("MODEL_ALIASES"))
print("HIDDEN_FROM_LIST =", found.get("HIDDEN_FROM_LIST"))
models = found.get("FALLBACK_MODELS") or []
print("FALLBACK_MODELS  =", len(models), "个模型")
ids = [m["modelId"] for m in models]
print("  含 auto      :", "auto" in ids)
print("  含 auto-kiro :", "auto-kiro" in ids, "(应为 False)")
EOF
```

> 必须在 `docker compose build` 之前改 —— 代码是构建时拷进镜像的。

### 关于 `/v1/models` 列表（可选）

`/v1/models` 返回的是 `kiro/config.py` 里硬编码的 `FALLBACK_MODELS`（新版 runtime
端点不提供 `/ListAvailableModels`），由上游作者手工维护，落后于 Kiro 的发布节奏。

**但它只影响展示，不影响调用** —— 模型名直接透传给 Kiro 后端，gateway 不做校验
（`account_manager.py` 里那段校验是注释掉的，注释写明 `let Kiro API decide`）。
实测列表里没有的模型照样能调通。

所以：LLMGateway 那边手工配模型名的话，这段可以跳过。若它要自动从 `/v1/models`
拉取注册，把 `FALLBACK_MODELS` 换成当前实际可用的 19 个：

```python
FALLBACK_MODELS: List[Dict[str, str]] = [
    {"modelId": "auto"},
    {"modelId": "claude-opus-5"},
    {"modelId": "claude-sonnet-5"},
    {"modelId": "claude-opus-4.8"},
    {"modelId": "gpt-5.6-sol"},
    {"modelId": "gpt-5.6-terra"},
    {"modelId": "gpt-5.6-luna"},
    {"modelId": "claude-opus-4.7"},
    {"modelId": "claude-opus-4.6"},
    {"modelId": "claude-sonnet-4.6"},
    {"modelId": "claude-opus-4.5"},
    {"modelId": "claude-sonnet-4.5"},
    {"modelId": "claude-sonnet-4"},
    {"modelId": "claude-haiku-4.5"},
    {"modelId": "deepseek-3.2"},
    {"modelId": "minimax-m2.5"},
    {"modelId": "minimax-m2.1"},
    {"modelId": "glm-5"},
    {"modelId": "qwen3-coder-next"},
]
```

以后想核对最新列表，在任一 kiro-cli 可用的机器上跑 `kiro-cli chat --list-models`
（还会显示额度倍率，倍率越低越省 credit）。

## 6. credentials.json

```bash
cat > /opt/docker/kiro-gateway/credentials.json <<'EOF'
[
  {"type": "sqlite", "path": "/app/accounts/"}
]
EOF
```

## 7. 配置

```bash
cp .env.example .env
chmod 600 .env
python3 -c 'import secrets; print(secrets.token_urlsafe(32))'    # 生成 key
```

`.env` 里改 / 加这些（**值不加引号**）：

```bash
PROXY_API_KEY=上一步生成的串
LOG_LEVEL=INFO

# 必填：kiro-cli 登录产生的 sqlite 里不含 profile_arn，gateway 取值链是
# auth_manager.profile_arn or PROFILE_ARN or ""，不配就是空串，
# runtime.kiro.dev 会拒绝 profileArn is required。
# 取值：kiro-cli whoami 输出的 arn:aws:codewhisperer:... 那行
PROFILE_ARN=arn:aws:codewhisperer:us-east-1:901635709197:profile/X49MVAEQMRV9

ACCOUNT_SYSTEM=true
ACCOUNTS_CONFIG_FILE=/app/credentials.json
ACCOUNTS_STATE_FILE=/app/state/state.json
ACCOUNT_RECOVERY_TIMEOUT=60
ACCOUNT_PROBABILISTIC_RETRY_CHANCE=0.1
```

想把流量平均分到多个账号，再加 `ACCOUNT_SELECTION=round_robin`（默认 `sticky`），
取舍见 `账号更换.md` 第 6 节。

同一组织下的账号 ARN 通常相同，配一次即可。若两个账号 ARN 不同，改用
`credentials.json` 的 per-account `profile_arn` 字段分别指定。

编辑 `docker-compose.yml`，改两处（`ports` 保持默认 `"8000:8000"` 不动）：

```yaml
    container_name: kiro-gateway
    user: "UID:GID"                   # ← 新增，填第 2 步 id -u; id -g 的输出

    volumes:                          # ← 改成这四行
      - /opt/docker/kiro-gateway/accounts:/app/accounts
      - /opt/docker/kiro-gateway/credentials.json:/app/credentials.json:ro
      - /opt/docker/kiro-gateway/state:/app/state
      - ./debug_logs:/app/debug_logs
```

`user:` 的取值：

| 部署身份 | 填法 |
|---------|------|
| root | `user: "0:0"` |
| 普通用户 | `user: "1000:1000"`（用第 2 步 `id -u; id -g` 的实际输出） |

必须与 `/opt/docker/kiro-gateway` 的属主一致 —— 镜像内默认用户 UID 是 999，
不一致会导致 **token 刷新写回失败**（容器能正常跑，但重启后因旧 refresh_token
已被作废而认证失败）。

## 8. 启动

```bash
docker compose build
docker compose up -d
docker compose logs -f
```

日志应出现：

```
Loaded 2 account(s) from credentials
Initialized account: /app/accounts/acct-a.sqlite3 (19 models)
Initialized account: /app/accounts/acct-b.sqlite3 (19 models)
Account system initialized successfully
```

## 9. 验证

```bash
export PROXY_API_KEY=$(grep '^PROXY_API_KEY=' .env | cut -d= -f2-)

# 写回权限，必须输出「可写」
docker compose exec kiro-gateway sh -c \
  'touch /app/accounts/.w && rm /app/accounts/.w && echo 可写'

# 模型列表，应有 auto、无 auto-kiro
curl -s localhost:8000/v1/models -H "Authorization: Bearer $PROXY_API_KEY"

# 错误 key 应返回 401
curl -s -o /dev/null -w "%{http_code}\n" localhost:8000/v1/models \
  -H "Authorization: Bearer wrong-key"

# 调用
curl -s localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer $PROXY_API_KEY" -H "Content-Type: application/json" \
  -d '{"model":"claude-haiku-4.5","messages":[{"role":"user","content":"说OK"}],"max_tokens":10}'

# 流式，输出应逐步出现
curl -N -s localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer $PROXY_API_KEY" -H "Content-Type: application/json" \
  -d '{"model":"claude-haiku-4.5","messages":[{"role":"user","content":"数到20"}],"stream":true}'
```

### 状态持久化

```bash
ls -la /opt/docker/kiro-gateway/state/
```

应出现 `state.json`（记录当前粘性账号索引、各账号失败计数与统计）。没有它，
容器重启后熔断状态和粘性会丢失。

### 顺带验证 acct-b（可选）

默认账号选择是**粘性**的：请求会一直走 acct-a，只有它失败进冷却后才落到 acct-b。
所以正常验证只能证明 acct-a 可用，acct-b 处于"未被实际使用"状态
（开启 `ACCOUNT_SELECTION=round_robin` 后每个账号都会被轮到，见 `账号更换.md` 第 6 节）。

想确认 acct-b 也能工作，临时停用 acct-a（脚本会重启并重置 `state.json`）：

```bash
bash scripts/kiro-accounts.sh disable a test    # 日志应显示 Loaded 1 account(s)，请求能正常返回
bash scripts/kiro-accounts.sh enable a          # 测完还原
```

## 10. LLMGateway 配置

| 项 | 值 |
|----|----|
| Base URL | `http://<服务器IP>:8000/v1` |
| API Key | `.env` 的 `PROXY_API_KEY` |

若 LLMGateway 严格校验响应 schema，加到 `.env` 并重启：

```bash
FAKE_REASONING=false
```

---

## 排障

| 现象 | 处理 |
|------|------|
| 只加载到 1 个账号 | `credentials.json` 没挂载，或路径用了 `~` |
| 第 9 步没输出「可写」 | `user:` 的 UID 与 `/opt/docker/kiro-gateway` 属主不一致 |
| `profileArn is required` | `.env` 的 `PROFILE_ARN` 没配或值不对，见第 7 步 |
| `401` | 核对 `PROXY_API_KEY` |
| 流式一次性返回 | 中间层有缓冲，Nginx 加 `proxy_buffering off` |
| 请求体过大报错 | `.env` 加 `AUTO_TRIM_PAYLOAD=true` |

## 维护

### token 刷新是请求驱动的，没有后台任务

gateway 唯一的后台任务是每 10 秒把 `state.json` 落盘（`main.py:506`），
**与 token 刷新无关**。刷新链路是：

```
请求到达 → get_next_account() 选账号（粘性，通常一直是 acct-a）
        → get_access_token() → 距过期 <10 分钟才刷 → 写回该账号的 sqlite
```

所以**只有被选中的账号会刷新**。启动时两个账号都会 `Initialized account`
（读文件、建 auth manager），但不会主动刷 token。

用下面这段能看出差异 —— 在服务的账号 `expires_at` 是新的，备用账号还停在登录时刻：

```bash
for f in /opt/docker/kiro-gateway/accounts/*.sqlite3; do
  python3 - "$f" <<'PYEOF'
import datetime, json, os, sqlite3, sys
p = sys.argv[1]
c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
rows = dict(c.execute("SELECT key, value FROM auth_kv").fetchall())
t = json.loads(rows["kirocli:odic:token"])
mt = datetime.datetime.fromtimestamp(os.path.getmtime(p))
print(f"{os.path.basename(p):20} expires_at={t.get('expires_at')}  文件修改时间={mt:%m-%d %H:%M:%S}")
PYEOF
done
```

这也是验证「刷新 + 写回」是否正常的硬证据：发一个请求后 `expires_at` 前移、
文件修改时间更新，说明两者都成功。**只看日志里的 `Token refreshed` 不够** ——
那只证明拿到了新 token，写回失败是 `error` 级日志、成功是 `debug` 级，
`LOG_LEVEL=INFO` 下看不到成功记录。

### 备用账号会因长期闲置而失效

access_token 过期无所谓（用到时会刷），但**refresh_token 本身也有寿命**
（取决于 IdC 会话有效期配置）。备用账号长期不被选中就没人刷它，于是可能出现：

```
acct-a 撞限流 → 转到 acct-b → acct-b 的 refresh_token 也已过期 → 两个都不可用
```

而这恰好发生在最需要备用账号的时刻。

**至少在真正需要故障转移之前手动验过一次**（见第 9 步的 acct-b 验证）。
想自动化就每周轮换一次主账号，既验证可用性也顺便刷新 token：

```bash
sudo tee /etc/cron.weekly/kiro-rotate > /dev/null <<'EOF'
#!/bin/bash
set -u
cd /opt/docker/kiro-gateway/kiro-gateway || exit 1
A=/opt/docker/kiro-gateway/accounts/acct-a.sqlite3
KEY=$(grep '^PROXY_API_KEY=' .env | cut -d= -f2-)

mv "$A" /tmp/acct-a.sqlite3 || exit 1
docker compose restart && sleep 10
if curl -sf -m 30 localhost:8000/v1/chat/completions \
     -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
     -d '{"model":"claude-haiku-4.5","messages":[{"role":"user","content":"."}],"max_tokens":1}' \
     > /dev/null; then
  logger -t kiro-rotate "备用账号 acct-b 正常"
else
  logger -t kiro-rotate "备用账号 acct-b 异常，需检查"
fi

mv /tmp/acct-a.sqlite3 "$A"
rm -f /opt/docker/kiro-gateway/state/state.json
docker compose restart
EOF
sudo chmod +x /etc/cron.weekly/kiro-rotate
```

代价是每周两次约 10 秒的重启中断。删 `state.json` 是让粘性索引归零，
重启后从 acct-a 重新开始。

### 凭证彻底失效时

某账号的 refresh_token 过期、被回收或没额度后，按 `账号更换.md` 操作：
`kiro-cli` 重新登录拿到 `data.sqlite3`，`scripts/kiro-accounts.sh disable` 旧账号、
`add` 新账号。注意 device flow 的身份取自浏览器会话，换账号要换浏览器会话（见第 3 步的说明）。

### 模型更新

Kiro 上新模型时不需要动 gateway —— 模型名透传，直接调用即可。只有想让
`/v1/models` 列表跟上时才需要改 `FALLBACK_MODELS`（见第 5 步）并重新
`docker compose build`。
