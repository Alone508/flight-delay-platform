#!/usr/bin/env python3
"""D3 数据引导：上传原始 CSV 到 HDFS，生成 MySQL 维度表。

用法（先 source scripts/env.sh）：
  python -I collector/bootstrap_ods.py
  python -I collector/bootstrap_ods.py --force-upload --skip-upload

行为：
  1. 将 FLIGHT_CSV 上传到 HDFS /flight/ods/raw/flights/（已存在则跳过）。
  2. 流式扫描 CSV，得到航司、机场、日期范围与航班量。
  3. 下载/读取 OurAirports airports.csv，补全机场经纬度。
  4. 建 MySQL 维度表并幂等 upsert dim_airline / dim_airport / dim_date。
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

import mysql.connector
import requests

PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parent.parent))
DEFAULT_CSV = os.environ.get("FLIGHT_CSV", str(PROJECT_DIR / "data/raw/flights_sample_3m.csv"))
DEFAULT_AIRPORTS = str(PROJECT_DIR / "data/raw/airports.csv")
AIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
HDFS_DIR = "/flight/ods/raw/flights"
HDFS_CSV = f"{HDFS_DIR}/flights_sample_3m.csv"
SCHEMA_SQL = PROJECT_DIR / "sql/mysql_schema.sql"


def log(msg: str) -> None:
    print(f"[bootstrap] {msg}", flush=True)


def run(cmd: list[str]) -> None:
    log(" ".join(cmd))
    subprocess.run(cmd, check=True)


def hdfs_exists(path: str) -> bool:
    return subprocess.run(["hdfs", "dfs", "-test", "-e", path], check=False).returncode == 0


def upload_csv(csv_path: Path, force: bool) -> None:
    if not csv_path.exists():
        raise SystemExit(f"CSV 不存在: {csv_path}")
    if hdfs_exists(HDFS_CSV) and not force:
        log(f"HDFS 已存在，跳过上传: {HDFS_CSV}")
        return
    run(["hdfs", "dfs", "-mkdir", "-p", HDFS_DIR])
    run(["hdfs", "dfs", "-put", "-f", str(csv_path), HDFS_CSV])


def download_airports(dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 1_000_000:
        log(f"机场坐标已存在: {dest}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    log(f"下载机场坐标: {AIRPORTS_URL}")
    with requests.get(AIRPORTS_URL, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        tmp = dest.with_suffix(".part")
        with open(tmp, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)
        tmp.replace(dest)
    log(f"已下载 {dest} ({dest.stat().st_size / 1e6:.1f} MB)")


def scan_flights(csv_path: Path) -> dict:
    airlines: dict[str, dict] = {}
    origin_counts: dict[str, int] = {}
    dest_counts: dict[str, int] = {}
    dates: set[dt.date] = set()
    rows = cancelled = delayed = bad_time = 0

    with open(csv_path, newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        required = {"FL_DATE", "AIRLINE_CODE", "AIRLINE", "ORIGIN", "DEST", "CRS_DEP_TIME"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise SystemExit(f"CSV 缺少字段: {sorted(missing)}")
        for row in reader:
            rows += 1
            code = (row["AIRLINE_CODE"] or "").strip().upper()
            if code:
                entry = airlines.setdefault(code, {"name": (row["AIRLINE"] or "").strip(), "flights": 0})
                entry["flights"] += 1
            origin = (row["ORIGIN"] or "").strip().upper()
            dest = (row["DEST"] or "").strip().upper()
            if origin:
                origin_counts[origin] = origin_counts.get(origin, 0) + 1
            if dest:
                dest_counts[dest] = dest_counts.get(dest, 0) + 1
            try:
                dates.add(dt.date.fromisoformat(row["FL_DATE"]))
            except ValueError:
                pass
            try:
                cancelled += 1 if float(row["CANCELLED"] or 0) == 1 else 0
            except ValueError:
                pass
            try:
                delayed += 1 if float(row["ARR_DELAY"]) >= 15 else 0
            except (TypeError, ValueError):
                pass
            t = (row["CRS_DEP_TIME"] or "").strip()
            if not (t.isdigit() and 1 <= len(t) <= 4 and int(t) <= 2359 and int(t) % 100 < 60):
                bad_time += 1

    if not dates:
        raise SystemExit("未解析到任何日期")
    airports = sorted(set(origin_counts) | set(dest_counts))
    return {
        "rows": rows,
        "airlines": airlines,
        "airports": airports,
        "origin_counts": origin_counts,
        "dest_counts": dest_counts,
        "date_min": min(dates),
        "date_max": max(dates),
        "cancelled": cancelled,
        "delayed": delayed,
        "bad_time": bad_time,
    }


def load_airport_coords(path: Path, wanted: set[str]) -> dict[str, dict]:
    rank = {"large_airport": 0, "medium_airport": 1, "small_airport": 2, "seaplane_base": 3, "heliport": 4}
    best: dict[str, tuple[int, dict]] = {}
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            code = (row.get("iata_code") or "").strip().upper()
            if code not in wanted:
                continue
            if (row.get("scheduled_service") or "").lower() not in ("yes", "y", "1", "true"):
                continue
            r = rank.get((row.get("type") or "").strip(), 9)
            try:
                lat = float(row["latitude_deg"])
                lon = float(row["longitude_deg"])
            except (KeyError, TypeError, ValueError):
                continue
            rec = {
                "name": (row.get("name") or "").strip() or None,
                "city": (row.get("municipality") or "").strip() or None,
                "country": (row.get("iso_country") or "").strip() or None,
                "lat": lat,
                "lon": lon,
            }
            if code not in best or r < best[code][0]:
                best[code] = (r, rec)
    return {code: rec for code, (_, rec) in best.items()}


def split_sql(text: str) -> list[str]:
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]


def mysql_connect(database: str | None = None):
    return mysql.connector.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_FLIGHT_USER", "flight"),
        password=os.environ["MYSQL_FLIGHT_PASSWORD"],
        database=database,
        charset="utf8mb4",
        autocommit=False,
    )


def ensure_schema() -> None:
    statements = split_sql(SCHEMA_SQL.read_text(encoding="utf-8"))
    conn = mysql_connect()
    cur = conn.cursor()
    for stmt in statements:
        cur.execute(stmt)
    conn.commit()
    cur.close()
    conn.close()
    log(f"MySQL 表结构就绪（{len(statements)} 条语句）")


def upsert_dims(info: dict, coords: dict[str, dict]) -> None:
    conn = mysql_connect(os.environ.get("MYSQL_DATABASE", "flight_delay"))
    cur = conn.cursor()

    airline_rows = [
        (code, data["name"] or code, data["flights"])
        for code, data in sorted(info["airlines"].items())
    ]
    cur.executemany(
        "INSERT INTO dim_airline (airline_code, airline_name, flights) VALUES (%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE airline_name=VALUES(airline_name), flights=VALUES(flights)",
        airline_rows,
    )

    airport_rows = []
    missing_coords = []
    for code in info["airports"]:
        rec = coords.get(code)
        if rec is None:
            missing_coords.append(code)
        flights = info["origin_counts"].get(code, 0) + info["dest_counts"].get(code, 0)
        airport_rows.append(
            (
                code,
                rec["name"] if rec else code,
                rec["city"] if rec else None,
                rec["country"] if rec else None,
                rec["lat"] if rec else None,
                rec["lon"] if rec else None,
                flights,
            )
        )
    cur.executemany(
        "INSERT INTO dim_airport (airport_code, name, city, country, latitude, longitude, flights) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE name=VALUES(name), city=VALUES(city), country=VALUES(country), "
        "latitude=VALUES(latitude), longitude=VALUES(longitude), flights=VALUES(flights)",
        airport_rows,
    )

    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    date_rows = []
    d = info["date_min"]
    while d <= info["date_max"]:
        dow = d.isoweekday()
        date_rows.append((d, d.year, (d.month - 1) // 3 + 1, d.month, d.day, dow, day_names[dow - 1], int(dow >= 6)))
        d += dt.timedelta(days=1)
    cur.executemany(
        "INSERT INTO dim_date (date_key, year, quarter, month, day, day_of_week, day_name, is_weekend) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE year=VALUES(year), quarter=VALUES(quarter), month=VALUES(month), "
        "day=VALUES(day), day_of_week=VALUES(day_of_week), day_name=VALUES(day_name), is_weekend=VALUES(is_weekend)",
        date_rows,
    )
    conn.commit()
    cur.close()
    conn.close()

    log(f"dim_airline: {len(airline_rows)} 行")
    log(f"dim_airport: {len(airport_rows)} 行（缺坐标 {len(missing_coords)} 个）")
    if missing_coords:
        log("缺坐标机场: " + ",".join(missing_coords[:30]) + (" ..." if len(missing_coords) > 30 else ""))
    log(f"dim_date: {len(date_rows)} 行（{info['date_min']} ~ {info['date_max']}）")


def main() -> int:
    ap = argparse.ArgumentParser(description="D3 ODS bootstrap")
    ap.add_argument("--csv", default=DEFAULT_CSV)
    ap.add_argument("--airports-csv", default=DEFAULT_AIRPORTS)
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--skip-mysql", action="store_true")
    ap.add_argument("--force-upload", action="store_true")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    airports_path = Path(args.airports_csv)

    if not args.skip_upload:
        upload_csv(csv_path, args.force_upload)

    log("扫描 CSV ...")
    info = scan_flights(csv_path)
    log(
        f"rows={info['rows']:,} airlines={len(info['airlines'])} airports={len(info['airports'])} "
        f"dates={info['date_min']}..{info['date_max']} cancelled={info['cancelled']:,} "
        f"arr_delay>=15={info['delayed']:,} bad_crs_dep_time={info['bad_time']:,}"
    )

    if args.skip_mysql:
        log("跳过 MySQL")
        return 0

    download_airports(airports_path)
    coords = load_airport_coords(airports_path, set(info["airports"]))
    log(f"OurAirports 匹配到 {len(coords)}/{len(info['airports'])} 个机场坐标")

    ensure_schema()
    upsert_dims(info, coords)
    log("完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
