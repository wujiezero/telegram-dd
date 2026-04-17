#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "[1/3] Stopping and removing existing containers..."
docker compose down --remove-orphans --rmi local

echo "[2/3] Rebuilding image from current directory..."
docker compose build --no-cache

echo "[3/3] Starting services..."
docker compose up -d

echo "Done."
