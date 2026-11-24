#!/usr/bin/env python3
"""DWD Parquet -> ADS 聚合（MySQL + HDFS DWS）。

用法：
  spark-submit --master 'local[4]' --driver-memory 2g spark/batch/ads_stats.py
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession, functions as F

DWD = "hdfs://localhost:9000/flight/dwd/flight"
DWS = "hdfs://localhost:9000/flight/dws"
MYSQL_DB = os.environ.get("MYSQL_DATABASE", "flight_delay")
MYSQL_URL = (
    f"jdbc:mysql://127.0.0.1:3306/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
    "&rewriteBatchedStatements=true&characterEncoding=utf8"
)


def write_mysql(df: DataFrame, table: str, user: str, password: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("batchsize", "1000")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    print(f"MySQL {table}: {df.count()} 行")


def write_parquet(df: DataFrame, name: str) -> None:
    df.write.mode("overwrite").option("compression", "snappy").parquet(f"{DWS}/{name}")
    print(f"DWS {name}: {df.count()} 行")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=DWD)
    ap.add_argument("--skip-mysql", action="store_true")
    args = ap.parse_args()

    user = os.environ.get("MYSQL_FLIGHT_USER", "flight")
    password = os.environ["MYSQL_FLIGHT_PASSWORD"]

    spark = SparkSession.builder.appName("ads_stats").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet(args.input).persist()

    total = df.count()
    print(f"DWD rows={total}")
    valid = df.filter((F.col("cancelled") == 0) & F.col("arr_delay").isNotNull()).persist()
    print(f"valid (non-cancelled) rows={valid.count()}")

    delayed_flag = F.when(F.col("arr_delay") >= 15, 1).otherwise(0)

    # 1) 航司月度准点率
    airline_names = (
        valid.groupBy("airline_code")
        .agg(F.max("airline_name").alias("airline_name"))
    )
    airline = (
        valid.groupBy("stat_month", "airline_code")
        .agg(
            F.count("*").alias("flights"),
            F.sum(delayed_flag).alias("delayed_flights"),
            F.avg("arr_delay").alias("avg_arr_delay"),
        )
        .withColumn("ontime_rate", 1 - F.col("delayed_flights") / F.col("flights"))
        .join(airline_names, "airline_code", "left")
        .select("stat_month", "airline_code", "airline_name", "flights",
                "delayed_flights", "ontime_rate", "avg_arr_delay")
    )

    # 2) 机场月度准点率（以出发机场计）
    airport = (
        valid.groupBy("stat_month", F.col("origin").alias("airport"))
        .agg(
            F.count("*").alias("flights"),
            F.sum(delayed_flag).alias("delayed_flights"),
            F.avg("arr_delay").alias("avg_arr_delay"),
        )
        .withColumn("ontime_rate", 1 - F.col("delayed_flights") / F.col("flights"))
        .select("stat_month", "airport", "flights", "delayed_flights", "ontime_rate", "avg_arr_delay")
    )

    # 3) 五类延误原因（只统计延误 >=15 的航班）
    cause_stack = (
        "stack(5, "
        "'CARRIER', delay_due_carrier, "
        "'WEATHER', delay_due_weather, "
        "'NAS', delay_due_nas, "
        "'SECURITY', delay_due_security, "
        "'LATE_AIRCRAFT', delay_due_late_aircraft) as (cause, minutes)"
    )
    cause_long = (
        valid.filter(F.col("arr_delay") >= 15)
        .select("stat_month", F.expr(cause_stack))
        .withColumn("minutes", F.coalesce(F.col("minutes"), F.lit(0.0)))
    )
    cause = cause_long.groupBy("stat_month", "cause").agg(
        F.sum("minutes").alias("delay_minutes")
    )
    totals = cause.groupBy("stat_month").agg(F.sum("delay_minutes").alias("total_minutes"))
    cause = (
        cause.join(totals, "stat_month")
        .withColumn("share", F.when(F.col("total_minutes") > 0,
                                    F.col("delay_minutes") / F.col("total_minutes")).otherwise(0.0))
        .select("stat_month", "cause", "delay_minutes", "share")
    )

    # 4) 航线统计
    route = (
        valid.groupBy("origin", "dest")
        .agg(
            F.count("*").alias("flights"),
            F.avg("arr_delay").alias("avg_arr_delay"),
            F.sum(delayed_flag).alias("delayed_flights"),
        )
        .withColumn("ontime_rate", 1 - F.col("delayed_flights") / F.col("flights"))
        .select("origin", "dest", "flights", "avg_arr_delay", "ontime_rate")
    )

    # 5) 星期 x 小时热力图
    heatmap = (
        valid.groupBy("day_of_week", "dep_hour")
        .agg(
            F.count("*").alias("flights"),
            F.sum(delayed_flag).alias("delayed_flights"),
            F.avg("arr_delay").alias("avg_arr_delay"),
        )
        .select("day_of_week", "dep_hour", "flights", "delayed_flights", "avg_arr_delay")
    )

    for name, frame in [
        ("ads_ontime_airline", airline),
        ("ads_ontime_airport", airport),
        ("ads_delay_cause", cause),
        ("ads_route_stats", route),
        ("ads_delay_heatmap", heatmap),
    ]:
        frame.cache()
        write_parquet(frame, name)
        if not args.skip_mysql:
            write_mysql(frame, name, user, password)

    spark.stop()


if __name__ == "__main__":
    main()
