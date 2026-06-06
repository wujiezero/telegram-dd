#!/usr/bin/env bash
#
# telegram-dd 统一管理脚本
#
# 用法：
#   ./tdd.sh start            启动服务（后台）
#   ./tdd.sh stop             停止并移除容器
#   ./tdd.sh restart          重启服务
#   ./tdd.sh status           查看容器与健康状态
#   ./tdd.sh logs [-f]        查看日志（-f 持续跟随）
#   ./tdd.sh rebuild          无缓存重建镜像并重启
#   ./tdd.sh shell            进入容器交互式 shell
#   ./tdd.sh login            首次交互式登录（输入手机号 + 验证码）
#   ./tdd.sh test             在本地虚拟环境运行单元测试
#   ./tdd.sh health           仅探测 /healthz 健康检查端点
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

SERVICE="telegram-dd"
WEB_PORT="7373"

# ---------------------------------------------------------------------------
# 兼容新旧两种 compose 调用方式：优先 `docker compose`，回退 `docker-compose`
# 按需检测：help / test / health 等命令无需 Docker 也能运行。
# ---------------------------------------------------------------------------
DC=()
ensure_compose() {
  [[ ${#DC[@]} -gt 0 ]] && return 0
  if docker compose version >/dev/null 2>&1; then
    DC=(docker compose)
  elif command -v docker-compose >/dev/null 2>&1; then
    DC=(docker-compose)
  else
    echo "错误：未找到 docker compose / docker-compose，请先安装 Docker。" >&2
    exit 1
  fi
}

log()  { printf '\033[1;34m[tdd]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[tdd]\033[0m %s\n' "$*" >&2; }
err()  { printf '\033[1;31m[tdd]\033[0m %s\n' "$*" >&2; }

cmd_start() {
  ensure_compose
  log "启动服务 ${SERVICE} ..."
  "${DC[@]}" up -d
  cmd_status
}

cmd_stop() {
  ensure_compose
  log "停止并移除容器 ..."
  "${DC[@]}" down --remove-orphans
  log "已停止。"
}

cmd_restart() {
  ensure_compose
  log "重启服务 ..."
  "${DC[@]}" up -d --force-recreate
  cmd_status
}

cmd_status() {
  ensure_compose
  log "容器状态："
  "${DC[@]}" ps
  echo
  cmd_health || true
}

cmd_logs() {
  ensure_compose
  if [[ "${1:-}" == "-f" || "${1:-}" == "--follow" ]]; then
    "${DC[@]}" logs -f --tail=200 "${SERVICE}"
  else
    "${DC[@]}" logs --tail=200 "${SERVICE}"
  fi
}

cmd_rebuild() {
  ensure_compose
  log "[1/3] 停止并移除现有容器（含本地镜像）..."
  "${DC[@]}" down --remove-orphans --rmi local || true
  log "[2/3] 无缓存重建镜像 ..."
  "${DC[@]}" build --no-cache
  log "[3/3] 启动服务 ..."
  "${DC[@]}" up -d
  cmd_status
}

cmd_shell() {
  ensure_compose
  log "进入容器 ${SERVICE} 的 shell（exit 退出）..."
  "${DC[@]}" exec "${SERVICE}" /bin/bash || "${DC[@]}" exec "${SERVICE}" /bin/sh
}

cmd_login() {
  ensure_compose
  log "首次交互式登录：根据提示输入手机号与验证码 ..."
  log "登录成功（Signed in successfully）后按 Ctrl+C 退出，再执行 ./tdd.sh start。"
  "${DC[@]}" run --rm "${SERVICE}"
}

cmd_test() {
  local py="${SCRIPT_DIR}/.venv/bin/python"
  if [[ ! -x "${py}" ]]; then
    py="$(command -v python3 || command -v python)"
  fi
  log "使用 ${py} 运行单元测试 ..."
  if "${py}" -m pytest --version >/dev/null 2>&1; then
    "${py}" -m pytest tests/ -v
  else
    "${py}" -m unittest discover -s tests -v
  fi
}

cmd_health() {
  log "探测健康检查端点 http://127.0.0.1:${WEB_PORT}/healthz ..."
  if command -v curl >/dev/null 2>&1; then
    if curl -fsS "http://127.0.0.1:${WEB_PORT}/healthz" 2>/dev/null; then
      echo
      return 0
    fi
  else
    if python3 -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:${WEB_PORT}/healthz', timeout=3).status==200 else 1)" 2>/dev/null; then
      log "健康检查通过。"
      return 0
    fi
  fi
  warn "健康检查未通过（服务可能尚未就绪或未启动）。"
  return 1
}

usage() {
  cat <<'EOF'
telegram-dd 统一管理脚本

用法：
  ./tdd.sh start            启动服务（后台）
  ./tdd.sh stop             停止并移除容器
  ./tdd.sh restart          重启服务
  ./tdd.sh status           查看容器与健康状态
  ./tdd.sh logs [-f]        查看日志（-f 持续跟随）
  ./tdd.sh rebuild          无缓存重建镜像并重启
  ./tdd.sh shell            进入容器交互式 shell
  ./tdd.sh login            首次交互式登录（输入手机号 + 验证码）
  ./tdd.sh test             在本地虚拟环境运行单元测试
  ./tdd.sh health           仅探测 /healthz 健康检查端点
EOF
}

main() {
  local action="${1:-}"
  shift || true
  case "${action}" in
    start)    cmd_start "$@" ;;
    stop)     cmd_stop "$@" ;;
    restart)  cmd_restart "$@" ;;
    status)   cmd_status "$@" ;;
    logs)     cmd_logs "$@" ;;
    rebuild)  cmd_rebuild "$@" ;;
    shell)    cmd_shell "$@" ;;
    login)    cmd_login "$@" ;;
    test)     cmd_test "$@" ;;
    health)   cmd_health "$@" ;;
    ""|-h|--help|help) usage ;;
    *) err "未知命令：${action}"; echo; usage; exit 1 ;;
  esac
}

main "$@"
