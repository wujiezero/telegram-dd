#!/usr/bin/env bash
# 兼容旧入口：实际逻辑已统一到 tdd.sh，这里转发到 `tdd.sh rebuild`。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/tdd.sh" rebuild
