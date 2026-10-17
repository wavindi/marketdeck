#!/usr/bin/env bash
set -euo pipefail
marketdeck_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ ! -x "$marketdeck_root/.venv/bin/python" ]]; then
  echo "Create .venv and install requirements.txt before launching MarketDeck." >&2
  exit 1
fi
cd "$marketdeck_root"
"$marketdeck_root/.venv/bin/python" -m marketdeck serve &
marketdeck_backend_pid=$!
trap 'kill "$marketdeck_backend_pid" 2>/dev/null || true' EXIT
cd "$marketdeck_root/web"
npm run dev
