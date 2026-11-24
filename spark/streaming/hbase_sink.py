#!/usr/bin/env python3
"""独立 Kafka -> HBase 写入进程（happybase / HBase Thrift 9090）。

写两张表：
  flight_detail     rowkey = fl_date|origin|fl_number|crs_dep_time, cf=info
  airport_realtime  rowkey = origin|window_start(分钟), cf=m

用法（先启动 hbase thrift）：
  python -I spark/streaming/hbase_sink.py --starting-offsets latest
  python -I spark/streaming/hbase_sink.py --max-messages 100
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from collections import defaultdict

import happybase
from kafka import KafkaConsumer


def log(msg: str) -> None:
    print(f"[hbase-sink {dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def b(v) -> bytes:
    return b"" if v is None else str(v).encode("utf-8")


def status_of(f: dict) -> str:
    if int(f.get("cancelled") or 0) == 1:
        return "cancelled"
    if float(f.get("dep_delay") or 0) >= 15:
        return "delayed"
    return "ontime"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--topic", default="flight-events")
    ap.add_argument("--group-id", default="hbase-sink")
    ap.add_argument("--thrift-host", default="localhost")
    ap.add_argument("--thrift-port", type=int, default=9090)
    ap.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    ap.add_argument("--flush-size", type=int, default=100)
    ap.add_argument("--flush-interval", type=float, default=2.0)
    ap.add_argument("--max-messages", type=int, default=0, help="0 表示一直运行")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset=args.starting_offsets,
        enable_auto_commit=True,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    conn = happybase.Connection(host=args.thrift_host, port=args.thrift_port, timeout=10000)
    detail = conn.table("flight_detail")
    realtime = conn.table("airport_realtime")
    log(f"connected thrift {args.thrift_host}:{args.thrift_port} topic={args.topic}")

    detail_rows: list[tuple[str, dict]] = []
    rt: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"departures": 0, "delayed": 0, "cancelled": 0, "delay_sum": 0.0}
    )
    last_flush = time.monotonic()
    processed = 0

    def reconnect() -> None:
        nonlocal conn, detail, realtime
        try:
            conn.close()
        except Exception:
            pass
        for attempt in range(1, 11):
            try:
                conn = happybase.Connection(host=args.thrift_host, port=args.thrift_port, timeout=10000)
                detail = conn.table("flight_detail")
                realtime = conn.table("airport_realtime")
                log("reconnected thrift")
                return
            except Exception as exc:
                log(f"reconnect failed ({attempt}/10): {exc}")
                time.sleep(2)
        raise RuntimeError("cannot reconnect HBase Thrift")

    def flush() -> None:
        nonlocal last_flush
        if not detail_rows and not rt:
            return
        for attempt in range(1, 4):
            try:
                if detail_rows:
                    with detail.batch(batch_size=max(1, len(detail_rows))) as batch:
                        for rowkey, data in detail_rows:
                            batch.put(rowkey, data)
                if rt:
                    with realtime.batch(batch_size=max(1, len(rt))) as batch:
                        for (origin, minute), v in rt.items():
                            avg = v["delay_sum"] / v["departures"] if v["departures"] else 0.0
                            batch.put(
                                f"{origin}|{minute}",
                                {
                                    b"m:departures": b(v["departures"]),
                                    b"m:delayed": b(v["delayed"]),
                                    b"m:cancelled": b(v["cancelled"]),
                                    b"m:avg_dep_delay": b(round(avg, 2)),
                                    b"m:updated_at": b(dt.datetime.now().isoformat(timespec="seconds")),
                                },
                            )
                log(f"flushed detail={len(detail_rows)} realtime={len(rt)} total={processed}")
                break
            except Exception as exc:
                log(f"flush failed ({attempt}/3): {exc}")
                reconnect()
                time.sleep(2)
        else:
            log("flush dropped after retries")
        detail_rows.clear()
        rt.clear()
        last_flush = time.monotonic()

    try:
        for msg in consumer:
            event = msg.value
            if event.get("event_type") != "flight_status":
                continue
            f = event.get("flight") or {}
            fl_date = f.get("fl_date")
            origin = f.get("origin")
            if not fl_date or not origin:
                continue
            rowkey = f"{fl_date}|{origin}|{f.get('fl_number')}|{f.get('crs_dep_time')}"
            detail_rows.append((
                rowkey,
                {
                    b"info:airline": b(f.get("airline")),
                    b"info:dest": b(f.get("dest")),
                    b"info:dep_delay": b(f.get("dep_delay")),
                    b"info:arr_delay": b(f.get("arr_delay")),
                    b"info:cancelled": b(f.get("cancelled")),
                    b"info:status": b(status_of(f)),
                    b"info:updated_at": b(event.get("event_time")),
                },
            ))
            minute = (event.get("event_time") or "")[:16].replace("T", " ")
            agg = rt[(origin, minute)]
            agg["departures"] += 1
            if int(f.get("cancelled") or 0) == 1:
                agg["cancelled"] += 1
            elif float(f.get("dep_delay") or 0) >= 15:
                agg["delayed"] += 1
            agg["delay_sum"] += float(f.get("dep_delay") or 0)

            processed += 1
            if len(detail_rows) >= args.flush_size or time.monotonic() - last_flush >= args.flush_interval:
                flush()
            if args.max_messages and processed >= args.max_messages:
                break
    except KeyboardInterrupt:
        log("interrupted")
    finally:
        flush()
        consumer.close()
        conn.close()
        log(f"done processed={processed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
