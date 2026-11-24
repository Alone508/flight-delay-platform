#!/usr/bin/env python3
"""ODS CSV -> 清洗后的 DWD Parquet。

用法：
  spark-submit --master 'local[4]' --driver-memory 2g spark/batch/clean_dwd.py

输入：/flight/ods/raw/flights/flights_sample_3m.csv
输出：/flight/dwd/flight/dt=YYYYMMDD/
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F, types as T

RAW = "hdfs://localhost:9000/flight/ods/raw/flights/flights_sample_3m.csv"
DWD = "hdfs://localhost:9000/flight/dwd/flight"

COLUMNS = [
    "FL_DATE", "AIRLINE", "AIRLINE_DOT", "AIRLINE_CODE", "DOT_CODE", "FL_NUMBER",
    "ORIGIN", "ORIGIN_CITY", "DEST", "DEST_CITY", "CRS_DEP_TIME", "DEP_TIME",
    "DEP_DELAY", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME",
    "ARR_TIME", "ARR_DELAY", "CANCELLED", "CANCELLATION_CODE", "DIVERTED",
    "CRS_ELAPSED_TIME", "ELAPSED_TIME", "AIR_TIME", "DISTANCE",
    "DELAY_DUE_CARRIER", "DELAY_DUE_WEATHER", "DELAY_DUE_NAS",
    "DELAY_DUE_SECURITY", "DELAY_DUE_LATE_AIRCRAFT",
]
CSV_SCHEMA = T.StructType([T.StructField(c, T.StringType(), True) for c in COLUMNS])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=RAW)
    ap.add_argument("--output", default=DWD)
    ap.add_argument("--repartition", type=int, default=8)
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("clean_dwd")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = spark.read.option("header", True).option("mode", "PERMISSIVE").schema(CSV_SCHEMA).csv(args.input)

    t = F.trim
    df = raw.select(
        F.to_date(t("FL_DATE")).alias("fl_date"),
        t("AIRLINE").alias("airline_name"),
        t("AIRLINE_CODE").alias("airline_code"),
        t("FL_NUMBER").cast("int").alias("fl_number"),
        t("ORIGIN").alias("origin"),
        t("ORIGIN_CITY").alias("origin_city"),
        t("DEST").alias("dest"),
        t("DEST_CITY").alias("dest_city"),
        F.lpad(F.regexp_replace(t("CRS_DEP_TIME"), r"\.0$", ""), 4, "0").alias("crs_dep_time"),
        t("CRS_ARR_TIME").alias("crs_arr_time"),
        F.col("DEP_DELAY").cast("double").alias("dep_delay"),
        F.col("ARR_DELAY").cast("double").alias("arr_delay"),
        F.col("CANCELLED").cast("int").alias("cancelled"),
        t("CANCELLATION_CODE").alias("cancellation_code"),
        F.col("DIVERTED").cast("int").alias("diverted"),
        F.col("CRS_ELAPSED_TIME").cast("double").alias("crs_elapsed_time"),
        F.col("ELAPSED_TIME").cast("double").alias("elapsed_time"),
        F.col("AIR_TIME").cast("double").alias("air_time"),
        F.col("DISTANCE").cast("double").alias("distance"),
        F.col("TAXI_OUT").cast("double").alias("taxi_out"),
        F.col("TAXI_IN").cast("double").alias("taxi_in"),
        F.col("DELAY_DUE_CARRIER").cast("double").alias("delay_due_carrier"),
        F.col("DELAY_DUE_WEATHER").cast("double").alias("delay_due_weather"),
        F.col("DELAY_DUE_NAS").cast("double").alias("delay_due_nas"),
        F.col("DELAY_DUE_SECURITY").cast("double").alias("delay_due_security"),
        F.col("DELAY_DUE_LATE_AIRCRAFT").cast("double").alias("delay_due_late_aircraft"),
    )

    df = (
        df.filter(F.col("fl_date").isNotNull())
        .filter((F.length("origin") == 3) & (F.length("dest") == 3))
        .filter(F.col("crs_dep_time").rlike(r"^\d{4}$"))
        .filter(F.substring("crs_dep_time", 3, 2).cast("int") < 60)
        .filter(F.substring("crs_dep_time", 1, 2).cast("int") < 24)
        .withColumn("dep_hour", F.substring("crs_dep_time", 1, 2).cast("int"))
        .withColumn("month", F.month("fl_date"))
        # ISO：1=周一 ... 7=周日（与 dim_date 一致）
        .withColumn("day_of_week", F.expr("pmod(dayofweek(fl_date) + 5, 7) + 1"))
        .withColumn("stat_month", F.date_format("fl_date", "yyyy-MM"))
        .withColumn("dt", F.date_format("fl_date", "yyyyMMdd"))
    )

    # 只过滤明显异常值；取消/备降航班的 delay 允许为空
    df = df.filter(
        (F.col("arr_delay").isNull() | (F.abs("arr_delay") <= 1440))
        & (F.col("dep_delay").isNull() | (F.abs("dep_delay") <= 1440))
        & (F.col("distance").isNull() | ((F.col("distance") > 0) & (F.col("distance") < 10000)))
    )

    df = df.dropDuplicates(["fl_date", "airline_code", "fl_number", "origin", "crs_dep_time"])

    df = df.repartition(args.repartition, "dt").sortWithinPartitions("dt", "crs_dep_time")

    df.write.mode("overwrite").option("compression", "snappy").partitionBy("dt").parquet(args.output)

    total = df.count()
    print(f"DWD rows={total}")
    print(f"DWD date range={df.agg(F.min('fl_date'), F.max('fl_date')).first()}")
    print(f"DWD partitions={df.select('dt').distinct().count()}")
    print(f"DWD output={args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
