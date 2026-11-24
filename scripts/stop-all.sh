#!/usr/bin/env bash
# 逆序停止：Kafka -> HBase -> HDFS（YARN/MySQL 仅在显式参数或已运行时处理）
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"

WITH_YARN=0
WITH_MYSQL=0
for a in "$@"; do
  [ "$a" = "--with-yarn" ] && WITH_YARN=1
  [ "$a" = "--with-mysql" ] && WITH_MYSQL=1
done

wait_gone() {
  local p=$1 n=${2:-60} i
  for ((i = 0; i < n; i++)); do
    ss -ltn 2>/dev/null | grep -q ":$p " || return 0
    sleep 1
  done
  echo "WARN: port $p still listening" >&2
  return 1
}

echo "[0] Streaming"
"$SCRIPT_DIR/stop-streaming.sh" || true

echo "[0.5] Flume"
"$SCRIPT_DIR/stop-flume.sh" || true

if [ "$WITH_YARN" = 1 ] || ss -ltn 2>/dev/null | grep -q ':8088 '; then
  echo "[1] YARN"
  "$HADOOP_HOME/sbin/stop-yarn.sh" || true
fi

echo "[2] Kafka broker"
"$KAFKA_HOME/bin/kafka-server-stop.sh" || true
wait_gone 9092 60 || true

echo "[3] Kafka ZooKeeper"
"$KAFKA_HOME/bin/zookeeper-server-stop.sh" || true
wait_gone 2182 60 || true

echo "[4] HBase"
"$HBASE_HOME/bin/stop-hbase.sh" || true
wait_gone 16010 60 || true

echo "[5] HDFS"
"$HADOOP_HOME/sbin/stop-dfs.sh" || true

if [ "$WITH_MYSQL" = 1 ]; then
  echo "[6] MySQL"
  sudo systemctl stop mysql || true
fi

echo "=== jps ==="
jps -l
