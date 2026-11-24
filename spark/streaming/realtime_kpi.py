#!/usr/bin/env python3
"""Spark Structured Streaming：Kafka flight-events -> 1 分钟机场 KPI。

输出：
  - MySQL ads_realtime_kpi（按 stat_time+airport 幂等 upsert）
  - MySQL ads_alerts（平均延误 >30 分钟或延误比例 >30%）
  - HDFS /flight/dws/realtime_kpi（Parquet append）

用法：
  spark-submit --master 'local[4]' --driver-memory 2g \
    spark/streaming/realtime_kpi.py --starting-offsets latest
"""
from __future__ import annotations

import argparse
import datetime as dt
import math
import os

import mysql.connector
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

MYSQL_DB = os.environ.get("MYSQL_DATABASE", "flight_delay")
HDFS_OUT = "hdfs://localhost:9000/flight/dws/realtime_kpi"

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("source_time", T.TimestampType()),
    T.StructField("flight", T.StructType([
        T.StructField("fl_date", T.StringType()),
        T.StructField("airline", T.StringType()),
        T.StructField("fl_number", T.IntegerType()),
        T.StructField("origin", T.StringType()),
        T.StructField("dest", T.StringType()),
        T.StructField("crs_dep_time", T.StringType()),
        T.StructField("dep_delay", T.DoubleType()),
        T.StructField("arr_delay", T.DoubleType()),
        T.StructField("cancelled", T.IntegerType()),
        T.StructField("diverted", T.IntegerType()),
        T.StructField("distance", T.IntegerType()),
        T.StructField("crs_elapsed_time", T.DoubleType()),
    ])),
])


def fnum(v, default: float = 0.0) -> float:
    try:
        x = float(v)
        return default if math.isnan(x) or math.isinf(x) else x
    except (TypeError, ValueError):
        return default


def fnum_or_none(v):
    if v is None:
        return None
    x = fnum(v, default=float("nan"))
    return None if math.isnan(x) else x


def mysql_conn():
    return mysql.connector.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_FLIGHT_USER", "flight"),
        password=os.environ["MYSQL_FLIGHT_PASSWORD"],
        database=MYSQL_DB,
    )


def load_weather() -> dict[str, tuple]:
    try:
        conn = mysql_conn()
        cur = conn.cursor()
        cur.execute("SELECT airport, visib, wind, flight_category FROM weather_latest")
        rows = {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}
        cur.close()
        conn.close()
        return rows
    except Exception as exc:
        print(f"[realtime] weather_latest 读取失败: {exc}", flush=True)
        return {}


def handle_batch(batch_df: DataFrame, batch_id: int) -> None:
    batch = batch_df.withColumn("stat_time", F.col("window.start")).drop("window")
    rows = batch.collect()
    if not rows:
        return
    weather = load_weather()
    conn = mysql_conn()
    cur = conn.cursor()
    kpi_rows = []
    alert_rows = []
    for r in rows:
        airport = r["airport"]
        visib, wind, cat = weather.get(airport, (None, None, None))
        visib, wind = fnum_or_none(visib), fnum_or_none(wind)
        departures = int(fnum(r["departures"]))
        delayed = int(fnum(r["delayed_flights"]))
        cancelled = int(fnum(r["cancelled"]))
        avg_delay = fnum(r["avg_dep_delay"])
        ontime = fnum(r["ontime_rate"])
        stat_time = r["stat_time"]
        kpi_rows.append((stat_time, airport, departures, delayed, cancelled,
                         avg_delay, ontime, visib, wind, cat))
        ratio = delayed / departures if departures else 0.0
        if avg_delay > 30 or ratio > 0.3:
            level = "CRITICAL" if (avg_delay > 60 or ratio > 0.5) else "WARN"
            reason = f"avg_dep_delay={avg_delay:.1f}, delayed_ratio={ratio:.2f}"
            alert_rows.append((stat_time, airport, level, reason, avg_delay, ratio))

    cur.executemany(
        "INSERT INTO ads_realtime_kpi "
        "(stat_time, airport, departures, delayed_flights, cancelled, avg_dep_delay, ontime_rate, visib, wind, flight_category) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE departures=VALUES(departures), delayed_flights=VALUES(delayed_flights), "
        "cancelled=VALUES(cancelled), avg_dep_delay=VALUES(avg_dep_delay), ontime_rate=VALUES(ontime_rate), "
        "visib=VALUES(visib), wind=VALUES(wind), flight_category=VALUES(flight_category)",
        kpi_rows,
    )
    if alert_rows:
        alert_rows.sort(key=lambda x: x[4], reverse=True)
        cur.executemany(
            "INSERT INTO ads_alerts (alert_time, airport, level, reason, avg_delay, delayed_ratio) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            alert_rows[:20],
        )
    conn.commit()
    cur.close()
    conn.close()

    try:
        batch.write.mode("append").option("compression", "snappy").parquet(HDFS_OUT)
    except Exception as exc:
        print(f"[realtime] Parquet 写入失败: {exc}", flush=True)

    print(
        f"[realtime] batch={batch_id} rows={len(rows)} alerts={min(len(alert_rows), 20)} "
        f"max_avg_delay={max((x[4] for x in kpi_rows), default=0):.1f}",
        flush=True,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--topic", default="flight-events")
    ap.add_argument("--checkpoint", default="hdfs://localhost:9000/flight/checkpoint/realtime")
    ap.add_argument("--trigger", default="10 seconds")
    ap.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    ap.add_argument("--timeout", type=int, default=0, help="秒；0 表示一直运行")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("realtime_kpi")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.checkpointLocation", args.checkpoint)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")
    ).select("e.*")

    flights = (
        parsed.filter((F.col("event_type") == "flight_status") & F.col("event_time").isNotNull())
        .filter(F.col("flight.origin").isNotNull())
        .withWatermark("event_time", "5 minutes")
    )

    agg = (
        flights.groupBy(F.window("event_time", "1 minute"), F.col("flight.origin").alias("airport"))
        .agg(
            F.count("*").alias("departures"),
            F.sum(F.when(F.col("flight.dep_delay") >= 15, 1).otherwise(0)).alias("delayed_flights"),
            F.sum(F.coalesce(F.col("flight.cancelled"), F.lit(0))).alias("cancelled"),
            F.avg("flight.dep_delay").alias("avg_dep_delay"),
        )
        .withColumn("ontime_rate", 1 - F.col("delayed_flights") / F.col("departures"))
    )

    query = (
        agg.writeStream.outputMode("update")
        .foreachBatch(handle_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )
    print(f"[realtime] started query={query.id} checkpoint={args.checkpoint}", flush=True)
    if args.timeout > 0:
        query.awaitTermination(args.timeout)
        if query.isActive:
            query.stop()
        print("[realtime] stopped by timeout", flush=True)
    else:
        query.awaitTermination()


if __name__ == "__main__":
    main()
