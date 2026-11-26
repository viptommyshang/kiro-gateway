#!/usr/bin/env bash
# Kiro Gateway 账号管理脚本（在生产宿主机上以 root 运行）
#
# 网关扫描 accounts/ 目录，每个 sqlite 文件就是一个账号。
# 本脚本把「加账号 / 停用账号 / 恢复账号 / 重启并重置状态 / 查看状态」收成一条命令。
# 任何时候都不会打印 token 内容。
# 使用说明: docs/zh/kiro-accounts.md
#
# 可用环境变量覆盖的路径（默认值对应生产部署）：
#   BASE=/opt/docker/kiro-gateway          账号目录、state、退役文件所在
#   COMPOSE_DIR=$BASE/kiro-gateway         compose 项目目录（含 .env）
#   CONTAINER=kiro-gateway                 容器名
#   WAIT_TIMEOUT=60                        重启后等待健康的秒数

set -euo pipefail

BASE="${BASE:-/opt/docker/kiro-gateway}"
COMPOSE_DIR="${COMPOSE_DIR:-$BASE/kiro-gateway}"
CONTAINER="${CONTAINER:-kiro-gateway}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-60}"

ACCOUNTS_DIR="$BASE/accounts"
STATE_DIR="$BASE/state"
STATE_FILE="$STATE_DIR/state.json"
STARTUP_LOG_PATTERN='Scanning folder|Loaded [0-9]+ account|Initialized account|Skipping invalid|Failed to initialize'

DRY_RUN=0
FORCE=0
MOVE=0
ALL=0

usage() {
	cat <<EOF
用法: $(basename "$0") [--dry-run] [--force] [--move] <命令> [参数]

命令:
  list                         列出 accounts/ 中的账号、旁边的退役文件，以及 state.json 里的失败数/统计
  add <data.sqlite3> [名字]    校验并加入新账号为 accounts/acct-<名字>.sqlite3，然后重启
                               （不给名字时自动取下一个未用的字母，如 acct-d）
                               加 --move 时移动源文件而不是复制（源文件随后不再存在）
  disable <名字> [原因]        把 accounts/acct-<名字>.sqlite3 移到 \$BASE/acct-<名字>.sqlite3.<原因>
                               （原因默认 disabled，例如 revoked、noquota），然后重启
  enable <名字>                把退役文件移回 accounts/，然后重启
  restart                      备份并删除 state.json，重启容器，等待健康，打印账号加载日志
  pool                         查看账号池：每个账号的状态（可用/冷却/无额度/凭证失效）、何时自动回池、进行中请求数
  reset                        一键重置账号池：所有已登录账号恢复为可用（清空无额度/失效标记、失败计数、限流冷却），
                               不重启、不丢统计；网关没在运行时改为 restart
  reset --all                  同上，并先把 disable 退役的账号文件全部移回 accounts/（会重启）
  login [名字]                 用本机 kiro-cli 设备码登录一个新账号并直接加入池子
                               （自动备份/恢复本机原来的 kiro-cli 登录；可连续执行，一次一个账号）
  status                       不重启，只看健康状态、账号加载日志和容器的 ACCOUNT_* 环境变量

选项:
  --dry-run                    只打印将要执行的操作，不做任何修改
  --force                      允许停用最后一个账号
  --move                       只用于 add：移动而不是复制 data.sqlite3，避免留下第二份凭证
  --all                        只用于 reset：同时恢复所有退役账号文件
  -h, --help                   显示本帮助

名字可写成 c、acct-c 或 acct-c.sqlite3。
详细说明: docs/zh/kiro-accounts.md

当前路径: BASE=$BASE  COMPOSE_DIR=$COMPOSE_DIR  CONTAINER=$CONTAINER
EOF
}

die() {
	echo "错误: $*" >&2
	exit 1
}

info() {
	echo "==> $*"
}

# 打印并执行命令；--dry-run 时只打印
run() {
	echo "  + $*"
	if [ "$DRY_RUN" -eq 0 ]; then
		"$@"
	fi
}

sha256_of() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
	else
		shasum -a 256 "$1" | awk '{print $1}'
	fi
}

owner_of() {
	stat -c '%u:%g' "$1" 2>/dev/null || stat -f '%u:%g' "$1"
}

# 把 c / acct-c / acct-c.sqlite3 统一成 c
normalize_name() {
	local name="$1"
	name="${name##*/}"
	name="${name%.sqlite3}"
	name="${name#acct-}"
	case "$name" in
		'' | *[!A-Za-z0-9_-]*) die "名字只能包含字母、数字、- 和 _：'$1'" ;;
	esac
	echo "$name"
}

account_files() {
	find "$ACCOUNTS_DIR" -maxdepth 1 -type f | sort
}

retired_files() {
	find "$BASE" -maxdepth 1 -type f -name '*.sqlite3.*' | sort
}

require_accounts_dir() {
	[ -d "$ACCOUNTS_DIR" ] || die "账号目录不存在: ${ACCOUNTS_DIR}（检查 BASE）"
}

# 校验 sqlite 文件含 auth_kv 表且有 token 记录（只查计数，不读 token 值）
validate_sqlite() {
	local file="$1" result=""
	local sql="SELECT (SELECT count(*) FROM sqlite_master WHERE type='table' AND name='auth_kv') || ',' || (SELECT count(*) FROM auth_kv WHERE key LIKE '%token%');"
	if command -v sqlite3 >/dev/null 2>&1; then
		result="$(sqlite3 -readonly "$file" "$sql" 2>/dev/null || true)"
		# 没有 auth_kv 表时上面的 SQL 会报错，这里单独判断
		if [ -z "$result" ]; then
			result="$(sqlite3 -readonly "$file" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='auth_kv';" 2>/dev/null || echo err),0"
		fi
	elif command -v python3 >/dev/null 2>&1; then
		result="$(python3 - "$file" <<'PY' 2>/dev/null || echo err
import sqlite3, sys
c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
t = c.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='auth_kv'").fetchone()[0]
n = c.execute("SELECT count(*) FROM auth_kv WHERE key LIKE '%token%'").fetchone()[0] if t else 0
print(f"{t},{n}")
PY
)"
	else
		die "宿主机既没有 sqlite3 也没有 python3，无法校验文件（apt install sqlite3）"
	fi
	case "$result" in
		1,0) die "$file 有 auth_kv 表但没有 token 记录，不是完整的 kiro-cli 登录凭证" ;;
		1,*) return 0 ;;
		0,* | err*) die "$file 不是 kiro-cli 的 sqlite 凭证（缺少 auth_kv 表或文件损坏）" ;;
		*) die "$file 校验失败（${result}）" ;;
	esac
}

# 拒绝与 accounts/ 中任一文件完全相同（sha256）的文件
refuse_duplicate() {
	local file="$1" sum existing
	sum="$(sha256_of "$file")"
	while IFS= read -r existing; do
		[ -n "$existing" ] || continue
		if [ "$(sha256_of "$existing")" = "$sum" ]; then
			die "$file 与已在用的 $existing 完全相同。同一份凭证放两份会互相作废 refresh_token，已拒绝"
		fi
	done <<EOF
$(account_files)
EOF
}

next_free_name() {
	local letter
	for letter in a b c d e f g h i j k l m n o p q r s t u v w x y z; do
		if [ ! -e "$ACCOUNTS_DIR/acct-$letter.sqlite3" ] && [ -z "$(find "$BASE" -maxdepth 1 -name "acct-$letter.sqlite3.*" | head -n 1)" ]; then
			echo "$letter"
			return 0
		fi
	done
	die "acct-a ~ acct-z 都已占用，请显式指定名字"
}

container_python() {
	docker exec -i "$CONTAINER" python - "$@"
}

# 解析 state.json：宿主机有 python3 用宿主机，否则用容器里的 python
print_state() {
	local script
	script="$(cat <<'PY'
import datetime, json, os, sys
path = sys.argv[1]
try:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
except FileNotFoundError:
    print("  (state.json 不存在：刚重置过或还没有请求)")
    sys.exit(0)
print(f"  粘性索引 current_account_index = {data.get('current_account_index', 0)}")
accounts = data.get("accounts", {})
if not accounts:
    print("  (没有账号记录)")
for account_id, acc in accounts.items():
    stats = acc.get("stats", {})
    last = acc.get("last_failure_time") or 0
    last_text = datetime.datetime.fromtimestamp(last).strftime("%m-%d %H:%M:%S") if last else "-"
    print(f"  {os.path.basename(account_id):24} failures={acc.get('failures', 0):<3} 上次失败={last_text}  "
          f"总={stats.get('total_requests', 0)} 成功={stats.get('successful_requests', 0)} 失败={stats.get('failed_requests', 0)}")
PY
)"
	if command -v python3 >/dev/null 2>&1; then
		python3 -c "$script" "$STATE_FILE"
	elif [ ! -f "$STATE_FILE" ]; then
		echo "  (state.json 不存在：刚重置过或还没有请求)"
	elif docker exec "$CONTAINER" true >/dev/null 2>&1; then
		container_python /app/state/state.json <<<"$script"
	elif command -v jq >/dev/null 2>&1; then
		jq -r '"  粘性索引 current_account_index = \(.current_account_index // 0)",
			(.accounts // {} | to_entries[] | "  \(.key | split("/") | last)  failures=\(.value.failures // 0)  总=\(.value.stats.total_requests // 0) 成功=\(.value.stats.successful_requests // 0) 失败=\(.value.stats.failed_requests // 0)")' "$STATE_FILE"
	else
		echo "  (宿主机无 python3/jq，容器也未运行，无法解析 state.json)"
	fi
}

cmd_list() {
	require_accounts_dir
	local f count=0
	info "在用账号（${ACCOUNTS_DIR}）"
	while IFS= read -r f; do
		[ -n "$f" ] || continue
		count=$((count + 1))
		echo "  $(basename "$f")  $(wc -c <"$f" | tr -d ' ') 字节  修改于 $(date -r "$f" '+%m-%d %H:%M:%S')"
	done <<EOF
$(account_files)
EOF
	[ "$count" -gt 0 ] || echo "  (空)"
	echo "  共 $count 个"

	info "退役文件（$BASE/*.sqlite3.*，不会被加载）"
	count=0
	while IFS= read -r f; do
		[ -n "$f" ] || continue
		count=$((count + 1))
		echo "  $(basename "$f")"
	done <<EOF
$(retired_files)
EOF
	[ "$count" -gt 0 ] || echo "  (无)"

	info "state.json（${STATE_FILE}）"
	print_state
}

container_health() {
	docker inspect -f '{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$CONTAINER" 2>/dev/null || echo "missing/none"
}

# 等待容器就绪：docker 健康检查为 healthy，或容器内 /health 返回 200
wait_healthy() {
	local waited=0 health
	info "等待容器就绪（最长 ${WAIT_TIMEOUT}s）"
	while [ "$waited" -lt "$WAIT_TIMEOUT" ]; do
		health="$(container_health)"
		case "$health" in
			running/healthy)
				echo "  健康: ${health}（${waited}s）"
				return 0
				;;
			running/*)
				if docker exec "$CONTAINER" python -c "import httpx,sys; sys.exit(httpx.get('http://localhost:8000/health', timeout=3).status_code != 200)" >/dev/null 2>&1; then
					echo "  /health 返回 200（docker 健康状态: ${health#*/}，${waited}s）"
					return 0
				fi
				;;
			exited/* | dead/* | missing/*)
				echo "  容器未运行: $health" >&2
				return 1
				;;
		esac
		sleep 2
		waited=$((waited + 2))
	done
	echo "  ${WAIT_TIMEOUT}s 内未就绪，当前状态: $(container_health)" >&2
	return 1
}

print_startup_logs() {
	local since="$1"
	info "账号加载日志"
	docker logs --since "$since" "$CONTAINER" 2>&1 | grep -E "$STARTUP_LOG_PATTERN" || echo "  (没有匹配的日志行，可能仍在启动)"
}

cmd_restart() {
	info "将执行：停止容器 → 备份并删除 state.json → 启动容器 → 等待健康"
	echo "  （先停再删：网关退出时会把内存里的状态写回 state.json）"
	if [ "$DRY_RUN" -eq 1 ]; then
		run docker stop "$CONTAINER"
		if [ -f "$STATE_FILE" ]; then
			run cp -p "$STATE_FILE" "$STATE_FILE.bak"
			run rm -f "$STATE_FILE"
		fi
		run docker start "$CONTAINER"
		echo "（dry-run，未执行）"
		return 0
	fi
	command -v docker >/dev/null 2>&1 || die "找不到 docker 命令"
	run docker stop "$CONTAINER"
	if [ -f "$STATE_FILE" ]; then
		run cp -p "$STATE_FILE" "$STATE_FILE.bak"
		run rm -f "$STATE_FILE"
	else
		echo "  state.json 不存在，跳过备份"
	fi
	local since
	since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	run docker start "$CONTAINER"
	if wait_healthy; then
		print_startup_logs "$since"
	else
		print_startup_logs "$since"
		echo "--- 最近 30 行日志 ---" >&2
		docker logs --tail 30 "$CONTAINER" 2>&1 | tail -n 30 >&2
		exit 1
	fi
}

cmd_status() {
	command -v docker >/dev/null 2>&1 || die "找不到 docker 命令"
	local started
	info "容器 $CONTAINER: $(container_health)"
	started="$(docker inspect -f '{{.State.StartedAt}}' "$CONTAINER" 2>/dev/null || true)"
	[ -n "$started" ] || die "容器 $CONTAINER 不存在"
	echo "  启动时间: $started"
	print_startup_logs "$started"

	info "容器环境变量 ACCOUNT_*"
	docker exec "$CONTAINER" env | grep -E '^ACCOUNT' | sort || echo "  (无)"

	# .env 改了但没 up -d 时提醒
	local env_file="$COMPOSE_DIR/.env" in_file in_container
	if [ -f "$env_file" ]; then
		in_file="$(grep -E '^ACCOUNT_SELECTION=' "$env_file" | tail -n 1 | cut -d= -f2- | tr -d '"' || true)"
		in_container="$(docker exec "$CONTAINER" printenv ACCOUNT_SELECTION 2>/dev/null || true)"
		if [ "$in_file" != "$in_container" ]; then
			echo "  注意: .env 中 ACCOUNT_SELECTION='${in_file}'，容器中为 '${in_container}'。"
			echo "        改 .env 后需在 $COMPOSE_DIR 执行 docker compose up -d（restart 不会重新读取 .env）"
		fi
	fi
}

cmd_add() {
	[ $# -ge 1 ] || die "用法: add <data.sqlite3> [名字]"
	require_accounts_dir
	local src="$1" name dst owner
	[ -f "$src" ] || die "文件不存在: $src"
	if [ -s "$src-wal" ]; then
		die "$src-wal 存在，主文件可能不完整。先退出 kiro-cli，或执行 sqlite3 \"$src\" \".backup /tmp/new.sqlite3\" 后用 /tmp/new.sqlite3"
	fi
	if [ $# -ge 2 ]; then
		name="$(normalize_name "$2")"
	else
		name="$(next_free_name)"
	fi
	dst="$ACCOUNTS_DIR/acct-$name.sqlite3"
	[ ! -e "$dst" ] || die "$dst 已存在，换个名字"
	if [ -n "$(find "$BASE" -maxdepth 1 -name "acct-$name.sqlite3.*" | head -n 1)" ]; then
		die "已有同名退役文件 $BASE/acct-$name.sqlite3.*，换个名字（或用 enable 恢复它）"
	fi

	validate_sqlite "$src"
	refuse_duplicate "$src"
	owner="$(owner_of "$ACCOUNTS_DIR")"

	if [ "$MOVE" -eq 1 ]; then
		info "将移动账号文件: $src → ${dst}（属主 ${owner}，权限 600，源文件不再保留），然后重启"
		run mv "$src" "$dst.tmp"
	else
		info "将加入账号: $src → ${dst}（属主 ${owner}，权限 600），然后重启"
		run cp "$src" "$dst.tmp"
	fi
	run chown "$owner" "$dst.tmp"
	run chmod 600 "$dst.tmp"
	run mv "$dst.tmp" "$dst"
	cmd_restart
}

cmd_disable() {
	[ $# -ge 1 ] || die "用法: disable <名字> [原因]"
	require_accounts_dir
	local name reason src dst remaining
	name="$(normalize_name "$1")"
	reason="${2:-disabled}"
	case "$reason" in
		'' | *[!A-Za-z0-9_.-]*) die "原因只能包含字母、数字、. - _：'$reason'" ;;
	esac
	src="$ACCOUNTS_DIR/acct-$name.sqlite3"
	dst="$BASE/acct-$name.sqlite3.$reason"
	[ -f "$src" ] || die "账号文件不存在: ${src}（用 list 查看）"
	[ ! -e "$dst" ] || die "$dst 已存在，换个原因"
	remaining="$(account_files | grep -c . || true)"
	if [ "$remaining" -le 1 ] && [ "$FORCE" -eq 0 ]; then
		die "acct-$name 是最后一个账号，停用后网关将无账号可用。确需停用请加 --force"
	fi

	info "将停用账号: $src → ${dst}，然后重启"
	run mv "$src" "$dst"
	cmd_restart
}

cmd_enable() {
	[ $# -ge 1 ] || die "用法: enable <名字>"
	require_accounts_dir
	local name src dst matches count
	local base="${1##*/}"
	if [[ "$base" == acct-*.sqlite3.* ]] && [ -f "$BASE/$base" ]; then
		# 直接给了退役文件名，如 acct-a.sqlite3.revoked
		src="$BASE/$base"
		name="${base#acct-}"
		name="$(normalize_name "${name%%.sqlite3.*}")"
	else
		name="$(normalize_name "$1")"
		matches="$(find "$BASE" -maxdepth 1 -type f -name "acct-$name.sqlite3.*" | sort)"
		count="$(printf '%s' "$matches" | grep -c . || true)"
		[ "$count" -gt 0 ] || die "没有 acct-$name 的退役文件（用 list 查看）"
		if [ "$count" -gt 1 ]; then
			echo "acct-$name 有多个退役文件，请用完整文件名指定：" >&2
			echo "$matches" | while IFS= read -r m; do echo "  ${m##*/}"; done >&2
			exit 1
		fi
		src="$matches"
	fi
	dst="$ACCOUNTS_DIR/acct-$name.sqlite3"
	[ ! -e "$dst" ] || die "$dst 已存在"
	refuse_duplicate "$src"

	info "将恢复账号: $src → ${dst}，然后重启"
	run mv "$src" "$dst"
	cmd_restart
}

# 在容器内调用网关的账号池管理接口（PROXY_API_KEY 取容器自己的环境变量，不经过宿主机）
admin_api() {
	local method="$1" path="$2"
	container_python "$method" "$path" <<'PY'
import json, os, sys
import httpx
method, path = sys.argv[1], sys.argv[2]
r = httpx.request(method, f"http://localhost:8000{path}",
                  headers={"Authorization": f"Bearer {os.environ['PROXY_API_KEY']}"}, timeout=15)
if r.status_code != 200:
    print(f"HTTP {r.status_code}: {r.text[:300]}", file=sys.stderr)
    sys.exit(1)
data = r.json()
labels = {"active": "可用", "degraded": "可用(近期失败)", "not_initialized": "可用(未初始化)",
          "cooldown": "限流冷却", "exhausted": "无额度(已剔除)", "invalid": "凭证失效(已剔除)"}
if "reset" in data:
    s = data["reset"]
    print(f"  已重置 {s['accounts']} 个账号：无额度 {s['exhausted']}，凭证失效 {s['invalid']}，"
          f"熔断 {s['broken']}，限流冷却 {s['cooldown']}")
rows = data.get("accounts", [])
if "active" in data:
    print(f"  池内可用 {data['active']}/{data['total']}")
for a in rows:
    back = f"  {a['returns_in_seconds']}s 后自动回池" if a["returns_in_seconds"] else ""
    reason = f"（{a['reason']}）" if a["reason"] else ""
    print(f"  {a['id']:24} {labels.get(a['state'], a['state'])}{reason}{back}  "
          f"进行中={a['in_flight']} 总={a['total_requests']} 成功={a['successful_requests']} 失败={a['failed_requests']}")
PY
}

gateway_running() {
	docker exec "$CONTAINER" true >/dev/null 2>&1
}

cmd_pool() {
	gateway_running || die "容器 $CONTAINER 未运行"
	info "账号池状态"
	admin_api GET /admin/accounts
}

cmd_reset() {
	if [ "$ALL" -eq 1 ]; then
		require_accounts_dir
		local f name dst restored=0
		info "恢复所有退役账号文件到 ${ACCOUNTS_DIR}"
		while IFS= read -r f; do
			[ -n "$f" ] || continue
			name="${f##*/}"
			dst="$ACCOUNTS_DIR/${name%%.sqlite3.*}.sqlite3"
			if [ -e "$dst" ]; then
				echo "  跳过 ${name}：$dst 已存在"
				continue
			fi
			refuse_duplicate "$f"
			run mv "$f" "$dst"
			restored=$((restored + 1))
		done <<<"$(retired_files)"
		echo "  恢复了 $restored 个账号文件"
		# 新文件只有重启才会被加载；restart 会同时清空 state.json
		cmd_restart
		return
	fi
	if gateway_running; then
		info "一键重置账号池（不重启）"
		if [ "$DRY_RUN" -eq 1 ]; then
			echo "  + POST /admin/accounts/reset"
			return
		fi
		if admin_api POST /admin/accounts/reset; then
			return
		fi
		echo "  网关不支持 /admin/accounts/reset（旧版本？），改为 restart" >&2
	else
		echo "  容器未运行，改为 restart" >&2
	fi
	cmd_restart
}

cmd_login() {
	command -v kiro-cli >/dev/null 2>&1 || die "本机没有 kiro-cli（安装见 Linux部署.md 第 1 节）"
	local cli_dir="$HOME/.local/share/kiro-cli"
	local cli_db="$cli_dir/data.sqlite3"
	! pgrep -x kiro-cli >/dev/null 2>&1 || die "有 kiro-cli 进程在运行，先退出它"
	LOGIN_TMP="$(mktemp -d)"
	chmod 700 "$LOGIN_TMP"
	LOGIN_CLI_DIR="$cli_dir"
	LOGIN_HAS_BACKUP=0
	trap restore_cli_login EXIT
	cat <<EOF
==> 即将用设备码登录新账号
  ⚠ 用浏览器【无痕窗口】打开下面打印的 URL，否则会直接用浏览器里已登录的旧账号批准。
  本机原来的 kiro-cli 登录会先移开（不执行 logout，避免注销服务端会话），结束后自动移回。
EOF
	if [ "$DRY_RUN" -eq 1 ]; then
		echo "  + mv $cli_db* $LOGIN_TMP/  &&  kiro-cli login --use-device-flow  &&  add --move"
		return
	fi
	if ls "$cli_db"* >/dev/null 2>&1; then
		mv "$cli_db"* "$LOGIN_TMP/"
		LOGIN_HAS_BACKUP=1
	fi
	kiro-cli login --use-device-flow
	local who
	who="$(kiro-cli whoami 2>&1 | grep -i '^Email' || true)"
	[ -n "$who" ] || die "登录未完成（kiro-cli whoami 没有 Email）"
	info "新账号 $who"
	# 拷出一份完整的 sqlite（合并 WAL，避免拷到半截文件），再删掉本机这份，只保留池子里一份
	python3 - "$cli_db" "$LOGIN_TMP/new.sqlite3" <<'PY'
import sqlite3, sys
src = sqlite3.connect(sys.argv[1]); dst = sqlite3.connect(sys.argv[2])
src.backup(dst); dst.close(); src.close()
PY
	rm -f "$cli_db"*
	MOVE=1
	cmd_add "$LOGIN_TMP/new.sqlite3" "$@"
}

# login 结束（成功或失败）时把本机原来的 kiro-cli 登录移回，删除临时文件
restore_cli_login() {
	if [ "${LOGIN_HAS_BACKUP:-0}" -eq 1 ]; then
		rm -f "$LOGIN_CLI_DIR/data.sqlite3"*
		mv "$LOGIN_TMP"/data.sqlite3* "$LOGIN_CLI_DIR/" 2>/dev/null || true
		echo "  已恢复本机原来的 kiro-cli 登录"
	fi
	[ -z "${LOGIN_TMP:-}" ] || rm -rf "$LOGIN_TMP"
}


main() {
	local args=()
	while [ $# -gt 0 ]; do
		case "$1" in
			--dry-run) DRY_RUN=1 ;;
			--force) FORCE=1 ;;
			--move) MOVE=1 ;;
			--all) ALL=1 ;;
			-h | --help)
				usage
				exit 0
				;;
			*) args+=("$1") ;;
		esac
		shift
	done
	[ "${#args[@]}" -gt 0 ] || {
		usage
		exit 1
	}
	local cmd="${args[0]}"
	args=("${args[@]:1}")
	[ "$MOVE" -eq 0 ] || [ "$cmd" = "add" ] || die "--move 只能用于 add"
	[ "$ALL" -eq 0 ] || [ "$cmd" = "reset" ] || die "--all 只能用于 reset"
	[ "$DRY_RUN" -eq 0 ] || echo "*** dry-run：只打印操作，不做修改 ***"
	case "$cmd" in
		list) cmd_list ;;
		add) cmd_add ${args[@]+"${args[@]}"} ;;
		disable) cmd_disable ${args[@]+"${args[@]}"} ;;
		enable) cmd_enable ${args[@]+"${args[@]}"} ;;
		restart) cmd_restart ;;
		status) cmd_status ;;
		pool) cmd_pool ;;
		reset) cmd_reset ;;
		login) cmd_login ${args[@]+"${args[@]}"} ;;
		*)
			usage
			die "未知命令: $cmd"
			;;
	esac
}

main "$@"
