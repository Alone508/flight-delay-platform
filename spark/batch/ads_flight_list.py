#!/usr/bin/env python3
"""DWD -> MySQL ads_flight_list（前端航班明细列表索引）。

取回放窗口 2019-01-01 ~ 2019-01-10 的航班（约 1.96 万行），与 HBase 明细/演示回放对齐。
用法：
  spark-submit --master 'local[4]' --driver-memory 2g spark/batch/ads_flight_list.py
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, functions as F

DWD = "hdfs://localhost:9000/flight/dwd/flight"
MYSQL_DB = os.environ.get("MYSQL_DATABASE", "flight_delay")
MYSQL_URL = (
    f"jdbc:mysql://127.0.0.1:3306/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
    "&rewriteBatchedStatements=true&characterEncoding=utf8"
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2019-01-01")
    ap.add_argument("--end", default="2019-01-10")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("ads_flight_list").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = (
        spark.read.parquet(DWD)
        .filter((F.col("fl_date") >= F.lit(args.start)) & (F.col("fl_date") <= F.lit(args.end)))
        .withColumn(
            "rowkey",
            F.concat_ws("|", F.date_format("fl_date", "yyyy-MM-dd"), "origin",
                        F.col("fl_number").cast("string"), "crs_dep_time"),
        )
        .withColumn(
            "status",
            F.when(F.col("cancelled") == 1, F.lit("cancelled"))
            .when(F.col("arr_delay") >= 15, F.lit("delayed"))
            .otherwise(F.lit("ontime")),
        )
        .select(
            "rowkey", "fl_date", "airline_code", "airline_name", "fl_number",
            "origin", "dest", "crs_dep_time", "dep_delay", "arr_delay",
            "cancelled", "status", "distance",
        )
    )
    count = df.count()
    print(f"ads_flight_list rows={count}")
    (
        df.write.format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", "ads_flight_list")
        .option("user", os.environ.get("MYSQL_FLIGHT_USER", "flight"))
        .option("password", os.environ["MYSQL_FLIGHT_PASSWORD"])
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("batchsize", "1000")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    print("ads_flight_list written")
    spark.stop()


if __name__ == "__main__":
    main()
