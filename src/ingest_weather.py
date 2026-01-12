# src/ingest_weather.py
"""
Ingest station TXT files from ./wx_data into Postgres table: weather

File format per line (tab-separated):
YYYYMMDD <TAB> TMAX <TAB> TMIN <TAB> PRCP

- TMAX/TMIN are in tenths of degrees C (e.g., 123 -> 12.3 C)
- PRCP is in tenths of millimeters (e.g., 10 -> 1.0 mm)
- Missing values are typically -9999

We store:
- max_temp_c, min_temp_c in degrees C
- precip_cm in centimeters

Behavior:
- Skips ingestion if table already has rows unless FORCE_INGEST=1
- Uses bulk UPSERT on (station_id, date)
- Prints progress every COMMIT_EVERY rows
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple

import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text


WX_DIR = Path(os.getenv("WX_DIR", "wx_data"))
COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "20000"))
FORCE_INGEST = os.getenv("FORCE_INGEST", "0").strip().lower() in {"1", "true", "yes", "y"}


def _parse_int(x: str) -> Optional[int]:
    x = x.strip()
    if x == "" or x == "-9999":
        return None
    try:
        return int(x)
    except ValueError:
        return None


def _weather_count(database_url: str) -> int:
    eng = create_engine(database_url, pool_pre_ping=True)
    with eng.connect() as c:
        return int(c.execute(text("select count(*) from weather")).scalar() or 0)


def main() -> None:
    start = datetime.utcnow()
    print("[ingest] starting...", flush=True)

    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. Example: postgresql+psycopg2://postgres:password@db:5432/weather"
        )

    # Pre-check: skip if table already populated (unless forced)
    existing = _weather_count(database_url)
    if existing > 0 and not FORCE_INGEST:
        print(
            f"[ingest] skipping (weather already has {existing} rows). "
            f"Set FORCE_INGEST=1 to re-ingest.",
            flush=True,
        )
        return

    if not WX_DIR.exists() or not WX_DIR.is_dir():
        raise RuntimeError(f"[ingest] wx_data directory not found: {WX_DIR.resolve()}")

    files = sorted(WX_DIR.glob("*.txt"))
    print(f"[ingest] wx_dir={WX_DIR.resolve()} files={len(files)} commit_every={COMMIT_EVERY} force={FORCE_INGEST}", flush=True)

    # psycopg2 expects "postgresql://" not "postgresql+psycopg2://"
    pg_dsn = database_url.replace("postgresql+psycopg2://", "postgresql://", 1)
    conn = psycopg2.connect(pg_dsn)
    conn.autocommit = False

    upsert_sql = """
        INSERT INTO weather (station_id, date, max_temp_c, min_temp_c, precip_cm)
        VALUES %s
        ON CONFLICT (station_id, date) DO UPDATE SET
            max_temp_c = EXCLUDED.max_temp_c,
            min_temp_c = EXCLUDED.min_temp_c,
            precip_cm = EXCLUDED.precip_cm
    """

    total_lines = 0
    upserts = 0
    skipped_bad_lines = 0
    batch: List[Tuple[str, str, Optional[float], Optional[float], Optional[float]]] = []

    try:
        with conn.cursor() as cur:
            for fp in files:
                station_id = fp.stem
                with fp.open("r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        total_lines += 1
                        parts = line.rstrip("\n").split("\t")
                        if len(parts) < 4:
                            skipped_bad_lines += 1
                            continue

                        datestr = parts[0].strip()
                        if len(datestr) != 8 or not datestr.isdigit():
                            skipped_bad_lines += 1
                            continue

                        # parse ints
                        tmax_i = _parse_int(parts[1])
                        tmin_i = _parse_int(parts[2])
                        prcp_i = _parse_int(parts[3])

                        # convert units
                        max_c = None if tmax_i is None else tmax_i / 10.0
                        min_c = None if tmin_i is None else tmin_i / 10.0
                        precip_cm = None if prcp_i is None else (prcp_i / 10.0) / 10.0  # tenths mm -> mm -> cm

                        # store date as YYYY-MM-DD string (psycopg2 will cast)
                        obs_date = f"{datestr[0:4]}-{datestr[4:6]}-{datestr[6:8]}"

                        batch.append((station_id, obs_date, max_c, min_c, precip_cm))

                        if len(batch) >= COMMIT_EVERY:
                            execute_values(cur, upsert_sql, batch, page_size=5000)
                            conn.commit()
                            upserts += len(batch)
                            print(f"[ingest] committed {upserts} upserts...", flush=True)
                            batch.clear()

            # final flush
            if batch:
                execute_values(cur, upsert_sql, batch, page_size=5000)
                conn.commit()
                upserts += len(batch)
                print(f"[ingest] committed {upserts} upserts...", flush=True)
                batch.clear()

    finally:
        conn.close()

    end = datetime.utcnow()
    print(f"[ingest] start={start.isoformat()} end={end.isoformat()}", flush=True)
    print(
        f"[ingest] files={len(files)} lines={total_lines} upserts={upserts} skipped_bad_lines={skipped_bad_lines}",
        flush=True,
    )


if __name__ == "__main__":
    main()
