#!/usr/bin/env python3
"""航班事件回放器：把历史 CSV 压缩到墙钟时间轴，追加 JSON Lines。

event_time  = run_start + (source_time - batch_min_source_time) / speedup
source_time = 原始航班计划起飞时间（UTC）
speedup=1440 表示 1 个历史日压缩到 1 个现实分钟。

用法（先 source scripts/env.sh）：
  python -I collector/flight_replay.py --start-date 2019-01-01 --end-date 2019-01-10 --speedup 1440
  python -I collector/flight_replay.py --limit 100 --speedup 200000   # 快速联调
  python -I collector/flight_replay.py --reset                        # 从偏移点重新开始
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parent.parent))
DEFAULT_CSV = os.environ.get("FLIGHT_CSV", str(PROJECT_DIR / "data/raw/flights_sample_3m.csv"))
DEFAULT_OUT = str(PROJECT_DIR / "data/logs/flight_events.log")
DEFAULT_OFFSET = str(PROJECT_DIR / "data/logs/flight_events.offset")
USECOLS = [
    "FL_DATE", "AIRLINE_CODE", "FL_NUMBER", "ORIGIN", "DEST", "CRS_DEP_TIME",
    "DEP_DELAY", "ARR_DELAY", "CANCELLED", "DIVERTED", "DISTANCE", "CRS_ELAPSED_TIME",
]


def log(msg: str) -> None:
    print(f"[replay {dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def iso_z(t: dt.datetime) -> str:
    return t.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def load_rows(csv_path: Path, start: str, end: str) -> pd.DataFrame:
    parts = []
    reader = pd.read_csv(
        csv_path,
        usecols=USECOLS,
        dtype=str,
        chunksize=500_000,
        keep_default_na=False,
        na_values=[""],
    )
    for chunk in reader:
        mask = (chunk["FL_DATE"] >= start) & (chunk["FL_DATE"] <= end)
        if mask.any():
            parts.append(chunk.loc[mask])
    if not parts:
        raise SystemExit(f"日期区间 {start} ~ {end} 内没有数据")
    df = pd.concat(parts, ignore_index=True)
    df["crs_dep_time"] = (
        df["CRS_DEP_TIME"].astype(str).str.strip().str.split(".").str[0].str.zfill(4)
    )
    df = df[df["crs_dep_time"].str.fullmatch(r"\d{4}")]
    df = df[(df["crs_dep_time"].str[2:].astype(int) < 60)]
    df["source_dt"] = pd.to_datetime(
        df["FL_DATE"] + " " + df["crs_dep_time"], format="%Y-%m-%d %H%M", errors="coerce"
    )
    df = df.dropna(subset=["source_dt"])
    df["event_id"] = (
        df["FL_DATE"] + "|" + df["ORIGIN"] + "|" + df["FL_NUMBER"] + "|" + df["crs_dep_time"]
    )
    df = df.sort_values(["source_dt", "event_id"], kind="mergesort").reset_index(drop=True)
    log(f"载入 {len(df):,} 条事件（{df['source_dt'].min()} ~ {df['source_dt'].max()}）")
    return df


def num(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def build_event(row: pd.Series, event_time: dt.datetime) -> dict:
    src = row["source_dt"].to_pydatetime()
    return {
        "event_id": row["event_id"],
        "event_type": "flight_status",
        "event_time": iso_z(event_time),
        "source_time": src.isoformat() + "Z",
        "flight": {
            "fl_date": row["FL_DATE"],
            "airline": row["AIRLINE_CODE"],
            "fl_number": int(num(row["FL_NUMBER"], 0)),
            "origin": row["ORIGIN"],
            "dest": row["DEST"],
            "crs_dep_time": row["crs_dep_time"],
            "dep_delay": num(row["DEP_DELAY"]),
            "arr_delay": num(row["ARR_DELAY"]),
            "cancelled": int(num(row["CANCELLED"], 0)),
            "diverted": int(num(row["DIVERTED"], 0)),
            "distance": int(num(row["DISTANCE"], 0)),
            "crs_elapsed_time": num(row["CRS_ELAPSED_TIME"]),
        },
    }


def read_offset(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def write_offset(path: Path, last_src: dt.datetime | None, last_id: str | None, written: int, finished: bool) -> None:
    payload = {
        "last_source_time": last_src.isoformat() if last_src else None,
        "last_event_id": last_id,
        "written": written,
        "finished": finished,
        "updated_at": iso_z(dt.datetime.now(dt.timezone.utc)),
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def replay(df: pd.DataFrame, out_path: Path, offset_path: Path, speedup: float, limit: int | None, loop: bool) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    offset_path.parent.mkdir(parents=True, exist_ok=True)

    while True:
        offset = read_offset(offset_path)
        last_src = pd.Timestamp(offset["last_source_time"]) if offset.get("last_source_time") else None
        last_id = offset.get("last_event_id")
        if offset.get("finished") and not loop:
            log("偏移文件标记已完成；如需继续请加 --reset 或 --loop")
            return

        batch_min = df["source_dt"].min()
        run_start = dt.datetime.now(dt.timezone.utc)
        written = 0
        started = False
        last_written_src = None
        last_written_id = None

        with open(out_path, "a", encoding="utf-8") as fh:
            for _, row in df.iterrows():
                src = row["source_dt"]
                if not started:
                    if last_src is not None and (src < last_src or (src == last_src and row["event_id"] <= (last_id or ""))):
                        continue
                    started = True
                elapsed = (src - batch_min).total_seconds() / speedup
                target = run_start + dt.timedelta(seconds=elapsed)
                delay = (target - dt.datetime.now(dt.timezone.utc)).total_seconds()
                if delay > 0:
                    time.sleep(delay)
                event = build_event(row, target)
                fh.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
                fh.flush()
                written += 1
                last_written_src = src.to_pydatetime()
                last_written_id = row["event_id"]
                write_offset(offset_path, last_written_src, last_written_id, written, False)
                if written % 1000 == 0:
                    log(f"已回放 {written:,} 条（{row['FL_DATE']} {row['crs_dep_time']}）")
                if limit and written >= limit:
                    break

        write_offset(
            offset_path,
            last_written_src if written else last_src,
            last_written_id if written else last_id,
            written,
            bool(written and not limit and not loop),
        )
        log(f"本轮结束，写入 {written:,} 条 → {out_path}")
        if not loop or limit:
            return
        log("--loop：2 秒后从头开始")
        time.sleep(2)
        if offset_path.exists():
            offset_path.unlink()


def main() -> int:
    ap = argparse.ArgumentParser(description="Flight event replayer")
    ap.add_argument("--csv", default=DEFAULT_CSV)
    ap.add_argument("--start-date", default="2019-01-01")
    ap.add_argument("--end-date", default="2019-01-10")
    ap.add_argument("--speedup", type=float, default=1440.0,
                    help="历史秒 / 现实秒；1440 = 1 历史日/现实分钟")
    ap.add_argument("--limit", type=int, default=None, help="本轮最多写入条数")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--output", default=DEFAULT_OUT)
    ap.add_argument("--offset-file", default=DEFAULT_OFFSET)
    args = ap.parse_args()

    if args.speedup <= 0:
        raise SystemExit("--speedup 必须 > 0")
    offset_path = Path(args.offset_file)
    if args.reset and offset_path.exists():
        offset_path.unlink()
        log("已重置偏移文件")
    if args.reset:
        Path(args.output).write_text("", encoding="utf-8")
        log("已清空输出日志")

    df = load_rows(Path(args.csv), args.start_date, args.end_date)
    replay(df, Path(args.output), offset_path, args.speedup, args.limit, args.loop)
    return 0


if __name__ == "__main__":
    sys.exit(main())
