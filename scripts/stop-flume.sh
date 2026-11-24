#!/usr/bin/env bash
# 停止 flightAgent 与 weatherAgent
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
LOGDIR=$PROJECT_DIR/data/logs

for name in flightAgent weatherAgent; do
  pidfile="$LOGDIR/$name.pid"
  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" && echo "stopped $name (pid $pid)"
    fi
    rm -f "$pidfile"
  fi
done

# 兜底：按 Application 参数里的 agent 名精确停止
for name in flightAgent weatherAgent; do
  pgrep -f "org.apache.flume.node.Application.*--name $name" | while read -r pid; do
    kill "$pid" 2>/dev/null && echo "stopped $name (pid $pid)"
  done
done
sleep 1
jps -l | grep 'flume.node.Application' || echo "no flume agents running"
