#!/usr/bin/env bash
# 停止 Spark realtime_kpi、hbase_sink 与 HBase Thrift
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
LOGDIR=$PROJECT_DIR/data/logs

for name in realtime_kpi hbase_sink hbase-thrift; do
  pidfile="$LOGDIR/$name.pid"
  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null && echo "stopped $name (pid $pid)"
    fi
    rm -f "$pidfile"
  fi
done

# 兜底：按主类名精确停止
pgrep -f 'realtime_kpi.py' | while read -r pid; do kill "$pid" 2>/dev/null && echo "stopped realtime_kpi.py ($pid)"; done
pgrep -f 'spark/streaming/hbase_sink.py' | while read -r pid; do kill "$pid" 2>/dev/null && echo "stopped hbase_sink.py ($pid)"; done
pgrep -f 'org.apache.hadoop.hbase.thrift.ThriftServer' | while read -r pid; do kill "$pid" 2>/dev/null && echo "stopped ThriftServer ($pid)"; done
sleep 2
echo "streaming stopped"
