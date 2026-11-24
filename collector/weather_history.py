#!/usr/bin/env python3
"""Open-Meteo Archive：按机场下载 2019-2023 逐小时天气，写 Parquet 并上传 HDFS。

输出列：origin, fl_date, dep_hour, visib(英里), wind(节), precip(mm), temp(C), flight_category
用于 train_model.py 的起飞前天气特征。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import time
from pathlib import Path

import mysql.connector
import pyarrow as pa
import pyarrow.parquet as pq
import requests

PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parent.parent))
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
SCHEMA = pa.schema([
    ("origin", pa.string()),
    ("fl_date", pa.string()),
    ("dep_hour", pa.int32()),
    ("visib", pa.float64()),
    ("wind", pa.float64()),
    ("precip", pa.float64()),
    ("temp", pa.float64()),
    ("flight_category", pa.string()),
])


def log(msg: str) -> None:
    print(f"[weather-history {dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def category(vis_mi: float | None) -> str:
    if vis_mi is None:
        return "UNKNOWN"
    if vis_mi >= 5:
        return "VFR"
    if vis_mi >= 3:
        return "MVFR"
    if vis_mi >= 1:
        return "IFR"
    return "LIFR"


def load_airports(top: int) -> list[dict]:
    conn = mysql.connector.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_FLIGHT_USER", "flight"),
        password=os.environ["MYSQL_FLIGHT_PASSWORD"],
        database=os.environ.get("MYSQL_DATABASE", "flight_delay"),
    )
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT airport_code, latitude, longitude, flights FROM dim_airport "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL ORDER BY flights DESC LIMIT %s",
        (top,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_airport(airport: dict, start: str, end: str, session: requests.Session) -> pa.Table | None:
    code = airport["airport_code"]
    params = {
        "latitude": float(airport["latitude"]),
        "longitude": float(airport["longitude"]),
        "start_date": start,
        "end_date": end,
        "hourly": "visibility,wind_speed_10m,precipitation,temperature_2m",
        "timezone": "auto",
        "wind_speed_unit": "kn",
    }
    for attempt in range(5):
        try:
            resp = session.get(ARCHIVE_URL, params=params, timeout=180)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", "60"))
                log(f"{code}: 429 限流，等待 {wait}s 后重试")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        except requests.RequestException as exc:
            if attempt == 4:
                log(f"{code}: 下载失败 {exc}")
                return None
            time.sleep(10 * (attempt + 1))
    else:
        log(f"{code}: 多次重试仍失败")
        return None
    hourly = resp.json().get("hourly") or {}
    times = hourly.get("time") or []
    vis = hourly.get("visibility") or []
    wind = hourly.get("wind_speed_10m") or []
    precip = hourly.get("precipitation") or []
    temp = hourly.get("temperature_2m") or []
    if not times:
        log(f"{code}: 无 hourly 数据")
        return None
    rows_origin, rows_date, rows_hour = [], [], []
    rows_vis, rows_wind, rows_precip, rows_temp, rows_cat = [], [], [], [], []
    for i, t in enumerate(times):
        try:
            day, hh = t.split("T")
            hour = int(hh[:2])
        except ValueError:
            continue
        v = vis[i] if i < len(vis) else None
        v_mi = round(float(v) / 1609.344, 3) if v is not None else None
        rows_origin.append(code)
        rows_date.append(day)
        rows_hour.append(hour)
        rows_vis.append(v_mi)
        rows_wind.append(float(wind[i]) if i < len(wind) and wind[i] is not None else None)
        rows_precip.append(float(precip[i]) if i < len(precip) and precip[i] is not None else None)
        rows_temp.append(float(temp[i]) if i < len(temp) and temp[i] is not None else None)
        rows_cat.append(category(v_mi))
    return pa.Table.from_arrays(
        [
            pa.array(rows_origin, pa.string()), pa.array(rows_date, pa.string()),
            pa.array(rows_hour, pa.int32()), pa.array(rows_vis, pa.float64()),
            pa.array(rows_wind, pa.float64()), pa.array(rows_precip, pa.float64()),
            pa.array(rows_temp, pa.float64()), pa.array(rows_cat, pa.string()),
        ],
        schema=SCHEMA,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=80)
    ap.add_argument("--start", default="2019-01-01")
    ap.add_argument("--end", default="2023-08-31")
    ap.add_argument("--out-dir", default=str(PROJECT_DIR / "data/weather/parts"))
    ap.add_argument("--hdfs-dir", default="/flight/dwd/weather_hourly")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--sleep", type=float, default=5.0, help="请求间隔秒数")
    ap.add_argument("--force", action="store_true", help="覆盖已有 part")
    args = ap.parse_args()

    airports = load_airports(args.top)
    log(f"机场数 {len(airports)}，区间 {args.start} ~ {args.end}")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = "flight-delay-platform/1.0"
    ok = skipped = failed = 0
    for idx, airport in enumerate(airports, 1):
        code = airport["airport_code"]
        dest = out_dir / f"{code}.parquet"
        if dest.exists() and not args.force:
            skipped += 1
            continue
        table = fetch_airport(airport, args.start, args.end, session)
        if table is None:
            failed += 1
            continue
        pq.write_table(table, dest, compression="snappy")
        ok += 1
        log(f"[{idx}/{len(airports)}] {code} rows={table.num_rows} flights={airport['flights']}")
        time.sleep(args.sleep)
    log(f"完成 新增={ok} 跳过={skipped} 失败={failed} → {out_dir}")

    if not args.skip_upload:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", args.hdfs_dir], check=True)
        files = sorted(out_dir.glob("*.parquet"))
        subprocess.run(["hdfs", "dfs", "-put", "-f"] + [str(f) for f in files] + [args.hdfs_dir], check=True)
        log(f"已上传 {len(files)} 个 Parquet 到 {args.hdfs_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
