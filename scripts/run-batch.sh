#!/usr/bin/env bash
# 顺序执行离线作业。已有 DWD 时可加 --skip-dwd。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"

SKIP_DWD=0
[ "${1:-}" = "--skip-dwd" ] && SKIP_DWD=1

SPARK_CONF=(--master 'local[4]' --driver-memory 3g --conf spark.sql.shuffle.partitions=24)

if [ "$SKIP_DWD" = 0 ]; then
  echo "=== clean_dwd ==="
  spark-submit "${SPARK_CONF[@]}" "$PROJECT_DIR/spark/batch/clean_dwd.py"
fi
echo "=== ads_stats ==="
spark-submit "${SPARK_CONF[@]}" "$PROJECT_DIR/spark/batch/ads_stats.py"
echo "=== ads_flight_list ==="
spark-submit "${SPARK_CONF[@]}" "$PROJECT_DIR/spark/batch/ads_flight_list.py"
echo "=== train_model ==="
spark-submit "${SPARK_CONF[@]}" "$PROJECT_DIR/spark/batch/train_model.py" --models lr,rf,gbt --tree-sample 0.4
echo "batch done"
