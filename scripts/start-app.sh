#!/usr/bin/env bash
# 启动展示层：HBase REST(8085) + Spring Boot API(8080) + Vite(5173)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
LOGDIR=$PROJECT_DIR/data/logs
mkdir -p "$LOGDIR"
API_JAR=$PROJECT_DIR/api/target/flight-delay-api-1.0.0.jar

wait_port() {
  local p=$1 n=${2:-60} i
  for ((i = 0; i < n; i++)); do ss -ltn 2>/dev/null | grep -q ":$p " && return 0; sleep 1; done
  echo "ERROR: port $p not listening" >&2
  return 1
}

if ! ss -ltn 2>/dev/null | grep -q ':8085 '; then
  nohup "$HBASE_HOME/bin/hbase" rest start -p 8085 -i 8086 > "$LOGDIR/hbase-rest.out" 2>&1 &
  echo $! > "$LOGDIR/hbase-rest.pid"
fi
wait_port 8085 60
echo "HBase REST 8085 OK"

if [ ! -f "$API_JAR" ]; then
  (cd "$PROJECT_DIR/api" && mvn -q -DskipTests package)
fi
if ! ss -ltn 2>/dev/null | grep -q ':8080 '; then
  nohup java -jar "$API_JAR" > "$LOGDIR/api.out" 2>&1 &
  echo $! > "$LOGDIR/api.pid"
fi
wait_port 8080 60
echo "Spring Boot API 8080 OK"

if [ ! -d "$PROJECT_DIR/web/node_modules" ]; then
  (cd "$PROJECT_DIR/web" && npm install --registry=https://registry.npmmirror.com --no-audit --no-fund)
fi
if ! ss -ltn 2>/dev/null | grep -q ':5173 '; then
  (cd "$PROJECT_DIR/web" && nohup npm run dev > "$LOGDIR/web.out" 2>&1 & echo $! > "$LOGDIR/web.pid")
fi
wait_port 5173 60
echo "Vue dev 5173 OK"

echo
echo "大屏:   http://localhost:5173/"
echo "Swagger: http://localhost:8080/swagger-ui.html"
echo "API:    http://localhost:8080/api/realtime/kpi"
