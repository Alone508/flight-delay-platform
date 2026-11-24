#!/usr/bin/env bash
# 展示层接口验收
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
BASE=${API_BASE:-http://localhost:8080}
PASS=0
FAIL=0

check() {
  local name=$1 path=$2
  if curl -s --noproxy '*' "$BASE$path" | grep -q '"code":0'; then
    echo "PASS  $name"; PASS=$((PASS + 1))
  else
    echo "FAIL  $name"; FAIL=$((FAIL + 1))
  fi
}

check "realtime/kpi" "/api/realtime/kpi"
check "realtime/trend" "/api/realtime/trend?minutes=60"
check "airports/rank" "/api/airports/rank?limit=5"
check "airlines/rank" "/api/airlines/rank?limit=5"
check "delay/causes" "/api/delay/causes"
check "delay/heatmap" "/api/delay/heatmap"
check "routes/top" "/api/routes/top?limit=5"
check "airports" "/api/airports"
check "alerts" "/api/alerts?limit=5"
check "predictions/top" "/api/predictions/top?limit=5"
check "model/metrics" "/api/model/metrics"
check "flights list" "/api/flights?page=1&size=5"
check "flights detail (HBase)" "/api/flights/2019-01-01%7CIAG%7C647%7C0238"

if curl -s --noproxy '*' http://localhost:8085/version | grep -q 'rest 0.0.3'; then
  echo "PASS  HBase REST 8085"; PASS=$((PASS + 1))
else
  echo "FAIL  HBase REST 8085"; FAIL=$((FAIL + 1))
fi
if curl -s --noproxy '*' http://localhost:5173/ | grep -q '<div id="app">'; then
  echo "PASS  Vue 5173"; PASS=$((PASS + 1))
else
  echo "FAIL  Vue 5173"; FAIL=$((FAIL + 1))
fi

echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
