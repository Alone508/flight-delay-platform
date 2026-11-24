#!/usr/bin/env python3
"""天气采集器：AWC METAR 优先，Open-Meteo 降级，缓存兜底并标记 is_stale。

用法（先 source scripts/env.sh）：
  python -I collector/weather_collector.py --once
  python -I collector/weather_collector.py --interval 300
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path

import mysql.connector
import requests

PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parent.parent))
DEFAULT_OUT = str(PROJECT_DIR / "data/logs/weather_events.log")
DEFAULT_CACHE = str(PROJECT_DIR / "data/logs/weather_cache.json")
AWC_URL = "https://aviationweather.gov/api/data/metar"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

ICAO_OVERRIDES = {
    "ANC": "PANC", "HNL": "PHNL", "OGG": "PHOG", "KOA": "PHKO", "ITO": "PHTO",
    "LIH": "PHLI", "SJU": "TJSJ", "BQN": "TJBQ", "PSE": "TJPS", "STT": "TIST",
    "STX": "TISX", "GUM": "PGUM", "SPN": "PGSN", "PPG": "NSTU", "MAJ": "PKMJ",
    "KWA": "PKWA", "CZM": "MMCZ", "YYZ": "CYYZ", "YVR": "CYVR", "YUL": "CYUL",
}


def log(msg: str) -> None:
    print(f"[weather {dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def icao_of(iata: str) -> str:
    return ICAO_OVERRIDES.get(iata, "K" + iata)


def iata_of(icao: str) -> str:
    for code, ic in ICAO_OVERRIDES.items():
        if ic == icao:
            return code
    return icao[1:] if len(icao) == 4 and icao.startswith("K") else icao


def parse_visib(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace("+", "")
    try:
        return float(s)
    except ValueError:
        return None


def flight_category(vis: float | None, source_cat: str | None = None) -> str:
    if source_cat:
        return source_cat.upper()
    if vis is None:
        return "UNKNOWN"
    if vis >= 5:
        return "VFR"
    if vis >= 3:
        return "MVFR"
    if vis >= 1:
        return "IFR"
    return "LIFR"


def load_airports(limit: int, codes: list[str] | None) -> list[dict]:
    conn = mysql.connector.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_FLIGHT_USER", "flight"),
        password=os.environ["MYSQL_FLIGHT_PASSWORD"],
        database=os.environ.get("MYSQL_DATABASE", "flight_delay"),
    )
    cur = conn.cursor(dictionary=True)
    if codes:
        placeholders = ",".join(["%s"] * len(codes))
        cur.execute(
            f"SELECT airport_code, latitude, longitude, flights FROM dim_airport "
            f"WHERE airport_code IN ({placeholders}) ORDER BY flights DESC",
            codes,
        )
    else:
        cur.execute(
            "SELECT airport_code, latitude, longitude, flights FROM dim_airport "
            "ORDER BY flights DESC LIMIT %s",
            (limit,),
        )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def awc_events(airports: list[dict], now: dt.datetime) -> dict[str, dict]:
    icaos = [icao_of(a["airport_code"]) for a in airports]
    try:
        resp = requests.get(
            AWC_URL,
            params={"ids": ",".join(icaos), "format": "json", "hours": 1},
            headers={"User-Agent": "flight-delay-platform/1.0"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        log(f"AWC 失败: {exc}")
        return {}
    if not isinstance(data, list):
        log(f"AWC 返回非列表: {str(data)[:200]}")
        return {}
    out: dict[str, dict] = {}
    for item in data:
        icao = item.get("icaoId")
        if not icao:
            continue
        iata = iata_of(icao)
        obs_epoch = item.get("obsTime")
        try:
            obs = dt.datetime.fromtimestamp(float(obs_epoch), tz=dt.timezone.utc)
        except (TypeError, ValueError):
            obs = now
        vis = parse_visib(item.get("visib"))
        out[iata] = {
            "event_id": f"weather|{iata}|{obs.isoformat()}",
            "event_type": "weather",
            "event_time": now.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "source_time": obs.isoformat().replace("+00:00", "Z"),
            "weather": {
                "airport": iata,
                "icao": icao,
                "wind": float(item["wspd"]) if item.get("wspd") is not None else None,
                "visib": vis,
                "flight_category": flight_category(vis, item.get("fltCat")),
                "temperature": float(item["temp"]) if item.get("temp") is not None else None,
                "precipitation": None,
                "raw": item.get("rawOb"),
                "source": "awc",
                "is_stale": 0,
            },
        }
    return out


def open_meteo_event(airport: dict, now: dt.datetime) -> dict | None:
    lat, lon = airport.get("latitude"), airport.get("longitude")
    if lat is None or lon is None:
        return None
    try:
        resp = requests.get(
            OPEN_METEO_URL,
            params={
                "latitude": float(lat),
                "longitude": float(lon),
                "current": "temperature_2m,wind_speed_10m,precipitation",
                "hourly": "visibility",
                "forecast_days": 1,
                "wind_speed_unit": "kn",
                "timezone": "UTC",
            },
            headers={"User-Agent": "flight-delay-platform/1.0"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        log(f"Open-Meteo 失败 {airport['airport_code']}: {exc}")
        return None
    current = data.get("current") or {}
    vis = None
    hourly = data.get("hourly") or {}
    values = [v for v in (hourly.get("visibility") or []) if v is not None]
    if values:
        vis = float(values[0])
    code = airport["airport_code"]
    return {
        "event_id": f"weather|{code}|{now.isoformat()}",
        "event_type": "weather",
        "event_time": now.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_time": now.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "weather": {
            "airport": code,
            "icao": icao_of(code),
            "wind": current.get("wind_speed_10m"),
            "visib": vis,
            "flight_category": flight_category(vis),
            "temperature": current.get("temperature_2m"),
            "precipitation": current.get("precipitation"),
            "raw": None,
            "source": "open-meteo",
            "is_stale": 0,
        },
    }


def stale_event(code: str, cache: dict, now: dt.datetime) -> dict | None:
    prev = cache.get(code)
    if not prev:
        return None
    w = dict(prev)
    w["is_stale"] = 1
    w["source"] = "stale"
    return {
        "event_id": f"weather|{code}|{now.isoformat()}|stale",
        "event_type": "weather",
        "event_time": now.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_time": now.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "weather": w,
    }


def upsert_latest(events: list[dict]) -> None:
    """把本轮天气事件写入 MySQL weather_latest（失败不影响文件链路）。"""
    if not events:
        return
    try:
        conn = mysql.connector.connect(
            host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
            port=int(os.environ.get("MYSQL_PORT", "3306")),
            user=os.environ.get("MYSQL_FLIGHT_USER", "flight"),
            password=os.environ["MYSQL_FLIGHT_PASSWORD"],
            database=os.environ.get("MYSQL_DATABASE", "flight_delay"),
        )
        rows = []
        for ev in events:
            w = ev["weather"]
            try:
                obs = dt.datetime.fromisoformat(ev["source_time"].replace("Z", "+00:00"))
                obs_str = obs.strftime("%Y-%m-%d %H:%M:%S")
            except (KeyError, ValueError):
                obs_str = None
            rows.append((w.get("airport"), w.get("icao"), w.get("wind"), w.get("visib"),
                         w.get("flight_category"), w.get("temperature"), w.get("source"),
                         int(w.get("is_stale", 0)), obs_str))
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO weather_latest (airport, icao, wind, visib, flight_category, temperature, source, is_stale, obs_time) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE icao=VALUES(icao), wind=VALUES(wind), visib=VALUES(visib), "
            "flight_category=VALUES(flight_category), temperature=VALUES(temperature), "
            "source=VALUES(source), is_stale=VALUES(is_stale), obs_time=VALUES(obs_time)",
            rows,
        )
        conn.commit()
        cur.close()
        conn.close()
        log(f"weather_latest upsert {len(rows)} 行")
    except Exception as exc:
        log(f"weather_latest 写入失败（忽略）: {exc}")


def cycle(airports: list[dict], out_path: Path, cache_path: Path, cache: dict) -> int:
    now = dt.datetime.now(dt.timezone.utc)
    events = awc_events(airports, now)
    written = 0
    written_events: list[dict] = []
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "a", encoding="utf-8") as fh:
        for airport in airports:
            code = airport["airport_code"]
            event = events.get(code)
            if event is None:
                event = open_meteo_event(airport, now)
            if event is None:
                event = stale_event(code, cache, now)
            if event is None:
                log(f"{code}: AWC/Open-Meteo 均失败且无缓存，跳过")
                continue
            fh.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
            written += 1
            written_events.append(event)
            if event["weather"].get("is_stale") == 0:
                cache[code] = event["weather"]
    upsert_latest(written_events)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=1), encoding="utf-8")
    sources: dict[str, int] = {}
    for airport in airports:
        w = cache.get(airport["airport_code"])
        if w:
            sources[w.get("source", "?")] = sources.get(w.get("source", "?"), 0) + 1
    log(f"写入 {written} 条天气事件；缓存来源 {sources}")
    return written


def main() -> int:
    ap = argparse.ArgumentParser(description="Weather collector")
    ap.add_argument("--airports", default=None, help="逗号分隔的 IATA 代码")
    ap.add_argument("--limit-airports", type=int, default=20)
    ap.add_argument("--interval", type=int, default=300)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--output", default=DEFAULT_OUT)
    ap.add_argument("--cache", default=DEFAULT_CACHE)
    args = ap.parse_args()

    codes = [c.strip().upper() for c in args.airports.split(",")] if args.airports else None
    airports = load_airports(args.limit_airports, codes)
    log(f"采集机场 {len(airports)} 个: " + ",".join(a["airport_code"] for a in airports))

    cache_path = Path(args.cache)
    cache = json.loads(cache_path.read_text(encoding="utf-8")) if cache_path.exists() else {}
    out_path = Path(args.output)

    while True:
        try:
            cycle(airports, out_path, cache_path, cache)
        except Exception as exc:  # 单轮失败不退出循环
            log(f"本轮异常: {exc}")
        if args.once:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("停止")
        sys.exit(0)
