#!/usr/bin/env bash
# 启动顺序：MySQL -> HDFS -> HBase -> Kafka ZK(2182) -> Kafka broker(9092)
# 默认不启动 YARN（15 GiB 机器节省内存）；需要时加 --with-yarn。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"

WITH_YARN=0
WITH_FLUME=1
WITH_STREAMING=0
for a in "$@"; do
  case "$a" in
    --with-yarn) WITH_YARN=1 ;;
    --no-flume) WITH_FLUME=0 ;;
    --with-streaming) WITH_STREAMING=1 ;;
  esac
done

wait_port() {
  local p=$1 n=${2:-90} i
  for ((i = 0; i < n; i++)); do
    ss -ltn 2>/dev/null | grep -q ":$p " && return 0
    sleep 1
  done
  echo "ERROR: port $p not listening after ${n}s" >&2
  return 1
}

echo "[1/6] MySQL"
systemctl is-active --quiet mysql || sudo systemctl start mysql

echo "[2/6] HDFS"
if ! ss -ltn 2>/dev/null | grep -q ':9870 '; then
  "$HADOOP_HOME/sbin/start-dfs.sh"
fi
wait_port 9870 90
for i in $(seq 1 60); do
  hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes (1)' && break
  sleep 2
done
for i in $(seq 1 60); do
  hdfs dfsadmin -safemode get 2>/dev/null | grep -q 'Safe mode is OFF' && break
  sleep 2
done
hdfs dfs -mkdir -p /user/alone /tmp /warehouse /flight
hdfs dfs -chmod 1777 /tmp || true

echo "[3/6] HBase"
if ! ss -ltn 2>/dev/null | grep -q ':16010 '; then
  "$HBASE_HOME/bin/start-hbase.sh"
fi
wait_port 16010 90
for i in $(seq 1 45); do
  printf 'status\nexit\n' | "$HBASE_HOME/bin/hbase shell" -n 2>/dev/null | grep -q 'active master' && break
  sleep 2
done

echo "[4/6] Kafka ZooKeeper (2182)"
if ! ss -ltn 2>/dev/null | grep -q ':2182 '; then
  KAFKA_HEAP_OPTS="-Xms384m -Xmx384m" \
    "$KAFKA_HOME/bin/zookeeper-server-start.sh" -daemon "$KAFKA_HOME/config/zookeeper.properties"
fi
wait_port 2182 60

echo "[5/6] Kafka broker (9092)"
if ! ss -ltn 2>/dev/null | grep -q ':9092 '; then
  KAFKA_HEAP_OPTS="-Xms768m -Xmx768m" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$KAFKA_HOME/config/server.properties"
fi
wait_port 9092 60

if [ "$WITH_FLUME" = 1 ]; then
  echo "[6/8] Flume"
  "$SCRIPT_DIR/start-flume.sh"
else
  echo "[6/8] Flume: skipped (--no-flume)"
fi

if [ "$WITH_YARN" = 1 ]; then
  echo "[7/8] YARN"
  if ! ss -ltn 2>/dev/null | grep -q ':8088 '; then
    "$HADOOP_HOME/sbin/start-yarn.sh"
  fi
  wait_port 8088 60
else
  echo "[7/8] YARN: skipped (use --with-yarn)"
fi

if [ "$WITH_STREAMING" = 1 ]; then
  echo "[8/8] Streaming (Spark KPI + HBase sink + Thrift)"
  "$SCRIPT_DIR/start-streaming.sh"
else
  echo "[8/8] Streaming: skipped (use --with-streaming)"
fi

echo "=== jps ==="
jps -l
echo "=== ports ==="
ss -ltn | grep -E ':(9000|9870|9864|9868|2181|2182|9092|16000|16010|16020|16030|3306|8088) ' || true
