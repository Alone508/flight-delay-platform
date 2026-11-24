#!/usr/bin/env bash
# Step 12 验收：进程、端口、HDFS/HBase/Kafka/MySQL/Flume/Sqoop、磁盘与内存。
# 用法：verify_stack.sh [--with-spark]
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"

WITH_SPARK=0
[ "${1:-}" = "--with-spark" ] && WITH_SPARK=1

PASS=0
FAIL=0
pass() { printf 'PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
fail() { printf 'FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }
check() { local name=$1; shift; if "$@" >/dev/null 2>&1; then pass "$name"; else fail "$name"; fi; }

echo "=== processes ==="
JP=$(jps -l 2>/dev/null)
for p in NameNode DataNode SecondaryNameNode HMaster HRegionServer HQuorumPeer QuorumPeerMain kafka.Kafka; do
  if grep -q "$p" <<<"$JP"; then pass "process $p"; else fail "process $p"; fi
done
FLUME_N=$(grep -c 'flume.node.Application' <<<"$JP")
[ "$FLUME_N" -ge 2 ] && pass "Flume agents ($FLUME_N)" || fail "Flume agents ($FLUME_N, expect 2)"

echo "=== ports ==="
for p in 9000 9870 9864 2181 2182 9092 16000 16010 16020 16030 3306; do
  if ss -ltn 2>/dev/null | grep -q ":$p "; then pass "port $p"; else fail "port $p"; fi
done
if ss -ltn 2>/dev/null | grep -q ':8080 '; then
  if curl -s --noproxy '*' http://localhost:8080/api/realtime/kpi | grep -q '"code":0'; then
    pass "Spring Boot API 8080"
  else
    fail "port 8080 busy but API health check failed"
  fi
else
  echo "INFO port 8080 not listening (start-app.sh 可启动展示层)"
fi
if ss -ltn 2>/dev/null | grep -q ':8085 '; then
  if curl -s --noproxy '*' http://localhost:8085/version | grep -q 'rest 0.0.3'; then
    pass "HBase REST 8085"
  else
    fail "HBase REST 8085 health check"
  fi
else
  echo "INFO HBase REST 8085 not listening (start-app.sh 可启动)"
fi

echo "=== HDFS ==="
for i in $(seq 1 30); do
  hdfs dfsadmin -safemode get 2>/dev/null | grep -q 'Safe mode is OFF' && break
  sleep 2
done
if hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes (1)'; then pass "1 live datanode"; else fail "live datanode"; fi
if echo "verify-$RANDOM" > /tmp/verify_hdfs.txt &&
   hdfs dfs -put -f /tmp/verify_hdfs.txt /tmp/verify_hdfs.txt >/dev/null 2>&1 &&
   hdfs dfs -cat /tmp/verify_hdfs.txt 2>/dev/null | grep -q '^verify-' &&
   hdfs dfs -rm -f /tmp/verify_hdfs.txt >/dev/null 2>&1; then
  pass "HDFS put/cat/rm"
else
  fail "HDFS put/cat/rm"
fi
rm -f /tmp/verify_hdfs.txt

echo "=== HBase ==="
for i in $(seq 1 30); do
  printf 'status\nexit\n' | hbase shell -n 2>/dev/null | grep -q 'active master' && break
  sleep 2
done
hbase_smoke() {
  local T="verify_smoke_$RANDOM"
  hbase shell -n <<EOF 2>&1
create '$T', {NAME => 'cf', VERSIONS => 1}
put '$T','r1','cf:q','ok'
get '$T','r1'
disable '$T'
drop '$T'
exit
EOF
}
HB_OUT=$(hbase_smoke)
if ! grep -q 'value=ok' <<<"$HB_OUT"; then
  sleep 5
  HB_OUT=$(hbase_smoke)
fi
if grep -q 'value=ok' <<<"$HB_OUT"; then
  pass "HBase create/put/get/drop"
else
  fail "HBase smoke"
  tail -20 <<<"$HB_OUT"
fi

echo "=== Kafka ==="
KT=$("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list 2>/dev/null)
grep -qx 'flight-events' <<<"$KT" && pass "topic flight-events" || fail "topic flight-events"
grep -qx 'weather-events' <<<"$KT" && pass "topic weather-events" || fail "topic weather-events"
DESC=$("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --describe --topic flight-events 2>/dev/null)
grep -q 'PartitionCount: 3' <<<"$DESC" && pass "flight-events 3 partitions" || fail "flight-events partitions"

echo "=== MySQL ==="
if mysql -h127.0.0.1 -u"${MYSQL_FLIGHT_USER:-flight}" -p"${MYSQL_FLIGHT_PASSWORD}" \
     -N -e "SELECT VERSION();" flight_delay 2>/dev/null | grep -q '^8\.0\.'; then
  pass "MySQL 8.0 + flight user"
else
  fail "MySQL connection"
fi

echo "=== tools ==="
if "$FLUME_HOME/bin/flume-ng" version 2>/dev/null | grep -q '1\.11\.0'; then pass "Flume 1.11.0"; else fail "Flume version"; fi
if "$SQOOP_HOME/bin/sqoop" version 2>/dev/null | grep -q 'Sqoop 1.4.7'; then pass "Sqoop 1.4.7"; else fail "Sqoop version"; fi

if [ "$WITH_SPARK" = 1 ]; then
  echo "=== Spark ==="
  if spark-submit --master 'local[4]' --driver-memory 2g "$PROJECT_DIR/scripts/smoke_spark.py" 2>/dev/null | grep -q 'ROWS 100000'; then
    pass "PySpark smoke"
  else
    fail "PySpark smoke"
  fi
fi

echo "=== resources ==="
AVAIL_KB=$(df -Pk / | awk 'NR==2 {print $4}')
[ "$AVAIL_KB" -ge 5242880 ] && pass "disk free >= 5 GiB ($((AVAIL_KB / 1024 / 1024)) GiB)" || fail "disk free < 5 GiB"
MEM_AVAIL=$(free -m | awk '/^Mem:/ {print $7}')
[ "$MEM_AVAIL" -ge 2048 ] && pass "memory available >= 2 GiB (${MEM_AVAIL} MiB)" || fail "memory available < 2 GiB"

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
