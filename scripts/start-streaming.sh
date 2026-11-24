#!/usr/bin/env bash
# 启动 HBase Thrift(9090) + Spark realtime_kpi + hbase_sink
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
LOGDIR=$PROJECT_DIR/data/logs
mkdir -p "$LOGDIR"

# 1) HBase Thrift
if ! ss -ltn 2>/dev/null | grep -q ':9090 '; then
  nohup "$HBASE_HOME/bin/hbase" thrift start -p 9090 > "$LOGDIR/hbase-thrift.out" 2>&1 &
  echo $! > "$LOGDIR/hbase-thrift.pid"
  for i in $(seq 1 40); do ss -ltn 2>/dev/null | grep -q ':9090 ' && break; sleep 1; done
fi
ss -ltn | grep -q ':9090 ' && echo "thrift 9090 OK" || { echo "thrift failed"; exit 1; }

# 2) Spark realtime KPI
if [ -f "$LOGDIR/realtime_kpi.pid" ] && kill -0 "$(cat "$LOGDIR/realtime_kpi.pid")" 2>/dev/null; then
  echo "realtime_kpi already running"
else
  nohup spark-submit --master 'local[4]' --driver-memory 2g \
    "$PROJECT_DIR/spark/streaming/realtime_kpi.py" \
    --checkpoint hdfs://localhost:9000/flight/checkpoint/realtime \
    --starting-offsets latest > "$LOGDIR/realtime_kpi.out" 2>&1 &
  echo $! > "$LOGDIR/realtime_kpi.pid"
  echo "realtime_kpi started (pid $!)"
fi

# 3) HBase sink
if [ -f "$LOGDIR/hbase_sink.pid" ] && kill -0 "$(cat "$LOGDIR/hbase_sink.pid")" 2>/dev/null; then
  echo "hbase_sink already running"
else
  nohup "$PROJECT_DIR/.venv/bin/python" -I -u "$PROJECT_DIR/spark/streaming/hbase_sink.py" \
    --starting-offsets latest > "$LOGDIR/hbase_sink.out" 2>&1 &
  echo $! > "$LOGDIR/hbase_sink.pid"
  echo "hbase_sink started (pid $!)"
fi

sleep 5
jps -l | grep -E 'SparkSubmit|ThriftServer' || true
echo "OK"
