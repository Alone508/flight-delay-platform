#!/usr/bin/env bash
# Sqoop export 演示：HDFS 文本 -> MySQL
# 说明：Sqoop 1.4.7 的 TextExportMapper 需要表对应的 SqoopRecord 类，
# 因此先用 sqoop codegen 生成并编译 SqoopExportDemo.jar，再通过 -libjars 传入。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"

CONN='jdbc:mysql://127.0.0.1:3306/flight_delay?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&characterEncoding=utf8'

echo "=== 1) 目标表 ==="
mysql -h127.0.0.1 -u"${MYSQL_FLIGHT_USER:-flight}" -p"${MYSQL_FLIGHT_PASSWORD}" flight_delay <<'SQL'
DROP TABLE IF EXISTS sqoop_export_demo;
CREATE TABLE sqoop_export_demo (id INT PRIMARY KEY, name VARCHAR(32), value DOUBLE);
SQL

echo "=== 2) HDFS 输入 ==="
printf '1,alpha,1.5\n2,beta,2.5\n3,gamma,3.5\n4,delta,4.5\n' > /tmp/sqoop_export.csv
hdfs dfs -mkdir -p /tmp/sqoop_export
hdfs dfs -put -f /tmp/sqoop_export.csv /tmp/sqoop_export/part-m-00000

echo "=== 3) 生成并编译 SqoopRecord ==="
rm -rf /tmp/sqoop_gen && mkdir -p /tmp/sqoop_gen
sqoop codegen --connect "$CONN" --username "${MYSQL_FLIGHT_USER:-flight}" \
  --password "${MYSQL_FLIGHT_PASSWORD}" --driver com.mysql.cj.jdbc.Driver \
  --table sqoop_export_demo --class-name SqoopExportDemo --outdir /tmp/sqoop_gen
JAR=$(find /tmp/sqoop-alone/compile -name 'SqoopExportDemo.jar' | head -1)
[ -n "$JAR" ] || { echo "codegen jar not found"; exit 1; }

echo "=== 4) sqoop export（local MR） ==="
sqoop export -D mapreduce.framework.name=local -libjars "$JAR" \
  --connect "$CONN" --username "${MYSQL_FLIGHT_USER:-flight}" \
  --password "${MYSQL_FLIGHT_PASSWORD}" --driver com.mysql.cj.jdbc.Driver \
  --class-name SqoopExportDemo --table sqoop_export_demo \
  --export-dir /tmp/sqoop_export --input-fields-terminated-by ',' --batch

echo "=== 5) 校验 ==="
mysql -h127.0.0.1 -u"${MYSQL_FLIGHT_USER:-flight}" -p"${MYSQL_FLIGHT_PASSWORD}" \
  -e "SELECT * FROM flight_delay.sqoop_export_demo ORDER BY id;"
