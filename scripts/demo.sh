#!/usr/bin/env bash
# 10 分钟演示：基础设施 + 实时链路 + 展示层 + 回放 + 天气
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"

echo "=== 1/5 基础设施 ==="
"$SCRIPT_DIR/start-all.sh"
echo "=== 2/5 实时链路 ==="
"$SCRIPT_DIR/start-streaming.sh"
echo "=== 3/5 展示层 ==="
"$SCRIPT_DIR/start-app.sh"
echo "=== 4/5 天气 ==="
"$PROJECT_DIR/.venv/bin/python" -I "$PROJECT_DIR/collector/weather_collector.py" --once
echo "=== 5/5 回放 2019-01-01 ~ 2019-01-10 (speedup=1440) ==="
echo "大屏: http://localhost:5173/  （Ctrl+C 停止回放）"
"$PROJECT_DIR/.venv/bin/python" -I "$PROJECT_DIR/collector/flight_replay.py" \
  --start-date 2019-01-01 --end-date 2019-01-10 --speedup 1440
