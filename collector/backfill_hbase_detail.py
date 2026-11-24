#!/usr/bin/env python3
"""用回放 JSON 日志幂等回填 HBase flight_detail。

用途：Flume 早期单 channel 分流导致部分事件只进了 HDFS；本脚本读本地完整日志，
把所有 flight_status 事件写入 HBase，保证 Flights 页明细完整。
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import happybase

PROJECT_DIR = Path(__file__).resolve().parent.parent


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
    ap.add_argument("--log", default=str(PROJECT_DIR / "data/logs/flight_events.log"))
    ap.add_argument("--thrift-host", default="localhost")
    ap.add_argument("--thrift-port", type=int, default=9090)
    ap.add_argument("--batch-size", type=int, default=500)
    args = ap.parse_args()

    conn = happybase.Connection(host=args.thrift_host, port=args.thrift_port, timeout=10000)
    table = conn.table("flight_detail")
    n = 0
    skipped = 0
    with open(args.log, encoding="utf-8", errors="replace") as fh, table.batch(batch_size=args.batch_size) as batch:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            if event.get("event_type") != "flight_status":
                skipped += 1
                continue
            f = event.get("flight") or {}
            if not f.get("fl_date") or not f.get("origin"):
                skipped += 1
                continue
            rowkey = f"{f.get('fl_date')}|{f.get('origin')}|{f.get('fl_number')}|{f.get('crs_dep_time')}"
            batch.put(rowkey, {
                b"info:airline": b(f.get("airline")),
                b"info:dest": b(f.get("dest")),
                b"info:dep_delay": b(f.get("dep_delay")),
                b"info:arr_delay": b(f.get("arr_delay")),
                b"info:cancelled": b(f.get("cancelled")),
                b"info:status": b(status_of(f)),
                b"info:updated_at": b(event.get("event_time")),
            })
            n += 1
    conn.close()
    print(f"backfilled {n} rows, skipped {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
