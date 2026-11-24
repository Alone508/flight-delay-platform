#!/usr/bin/env bash
# 停止展示层
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
LOGDIR=$PROJECT_DIR/data/logs

for name in api web hbase-rest; do
  pidfile="$LOGDIR/$name.pid"
  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    kill "$pid" 2>/dev/null && echo "stopped $name ($pid)"
    rm -f "$pidfile"
  fi
done

# 按主类/进程名兜底（避免误杀当前 shell）
PIDS=$(jps -l | awk '/flight-delay-api/ {print $1}')
[ -n "$PIDS" ] && kill $PIDS 2>/dev/null && echo "stopped API java"
PIDS=$(jps -l | awk '/org.apache.hadoop.hbase.rest.RESTServer/ {print $1}')
[ -n "$PIDS" ] && kill $PIDS 2>/dev/null && echo "stopped HBase REST"
pgrep -f 'node .*[.]bin/vite' | while read -r pid; do kill "$pid" 2>/dev/null && echo "stopped vite ($pid)"; done
pgrep -f '[n]pm run dev' | while read -r pid; do kill "$pid" 2>/dev/null && echo "stopped npm dev ($pid)"; done
sleep 1
ss -ltn | grep -E ':(8080|8085|8086|5173) ' || echo "app ports closed"
