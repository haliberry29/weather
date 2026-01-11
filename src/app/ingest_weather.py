"""
src/app/ingestion.py

A fully self-contained ingestion module that:
1) Ensures tables exist (create_all is safe to call repeatedly)
2) Skips ingestion if it has already been completed (marker table)
3) Uses Postgres UPSERT semantics to avoid crashing on duplicates
4) Is written to be dropped into your repo and imported from main.py

How to use (in your FastAPI startup/lifespan):
    from src.app.ingestion import run_startup_ingestion
    run_startup_ingestion()

If your project uses different import paths for engine/session/models, update the imports
at the top (they are isolated in one block).
"""

from __future__ import annotations  # allows forward type refs for Python <3.11

from typing import Any, Dict, Iterable, List, Optional, Tuple  # typing helpers

from sqlalchemy import Boolean, Column, String, select, func  # SQLAlchemy core
from sqlalchemy.exc import SQLAlchemyError  # base SQLAlchemy exception type
from sqlalchemy.orm import Session  # session type for type hints

# Postgres-specific INSERT that supports ON CONFLICT (UPSERT).
from sqlalchemy.dialects.postgresql import insert  # enables on_conflict_do_nothing / do_update

# ---------------------------------------------------------------------
# ✅ PROJECT IMPORTS (edit only here if your repo uses different paths)
# ---------------------------------------------------------------------
try:
    # Your project likely has these (common in templates).
    from src.app.db import Base, SessionLocal, engine  # Base metadata + session + engine
    from src.app.models import Weather, WeatherStats  # ORM models for the tables
except Exception as e:
    # If imports fail, raise a clear error telling you what to edit.
    raise ImportError(
        "Could not import Base/SessionLocal/engine or Weather/WeatherStats.\n"
        "Edit the import block in src/app/ingestion.py to match your repo.\n"
        f"Original error: {e}"
    )


# ---------------------------------------------------------------------
# ✅ INGESTION MARKER TABLE
#    This prevents re-ingestion every time the API starts.
# ---------------------------------------------------------------------
class IngestionState(Base):  # declare an ORM model on the shared Base
    __tablename__ = "ingestion_state"  # table name in Postgres

    key = Column(String, primary_key=True)  # unique key for each ingestion job
    value = Column(Boolean, nullable=False, default=False)  # did it complete?


# A single key for this project (you can add more keys for other pipelines).
INGESTION_KEY = "weather_ingested_v1"  # version it so you can force re-ingest later


# ---------------------------------------------------------------------
# ✅ TABLE CREATION
# ---------------------------------------------------------------------
def ensure_tables_exist() -> None:
    """Create all tables if they don't exist (safe to call repeatedly)."""
    Base.metadata.create_all(bind=engine)  # creates missing tables only


# ---------------------------------------------------------------------
# ✅ INGESTION GUARDS
# ---------------------------------------------------------------------
def already_ingested(session: Session) -> bool:
    """
    Return True if ingestion has already run and completed.

    Primary method: checks ingestion_state marker table.
    Fallback method: checks if Weather AND WeatherStats have rows.
    """
    # Try the marker row first (fastest + most reliable).
    marker = session.get(IngestionState, INGESTION_KEY)  # fetch marker row by PK
    if marker is not None:  # marker row exists
        return bool(marker.value)  # True means ingestion already done

    # Fallback: if no marker row exists yet, check table row counts.
    # This helps in case you ingested before adding the marker table.
    weather_count = session.execute(  # execute a scalar query
        select(func.count()).select_from(Weather)  # count rows in Weather table
    ).scalar_one()  # extract the scalar result

    stats_count = session.execute(  # execute a scalar query
        select(func.count()).select_from(WeatherStats)  # count rows in WeatherStats table
    ).scalar_one()  # extract the scalar result

    # If both tables have rows, we treat ingestion as already done.
    return (weather_count > 0) and (stats_count > 0)


def mark_ingested(session: Session) -> None:
    """Upsert ingestion marker so future startups skip ingestion."""
    # Use merge so it inserts or updates the row seamlessly.
    session.merge(IngestionState(key=INGESTION_KEY, value=True))  # set completed marker
    session.commit()  # persist the marker in DB


# ---------------------------------------------------------------------
# ✅ INGESTION HELPERS
# ---------------------------------------------------------------------
def ingest_weather(session: Session, rows: List[Dict[str, Any]]) -> int:
    """
    Insert weather rows into Weather table in an idempotent way.

    - Uses ON CONFLICT DO NOTHING to avoid crashing if duplicates exist.
    - Assumes your Weather model has a uniqueness constraint on the natural key.
      If your unique key differs, adjust `index_elements=[...]`.
    """
    if not rows:  # no data to insert
        return 0  # inserted 0 rows

    # Build a Postgres insert statement for the Weather ORM model.
    stmt = insert(Weather).values(rows)  # bulk insert values

    # IMPORTANT:
    # Update this list to match the unique constraint / natural key in your Weather table.
    # Example: if unique is (station_id, date), set index_elements=["station_id", "date"].
    stmt = stmt.on_conflict_do_nothing(  # skip rows that already exist
        index_elements=["station_id", "date"]  # <-- change if your Weather unique key differs
    )

    result = session.execute(stmt)  # execute the statement
    session.commit()  # commit transaction

    # result.rowcount is often -1 for some DBAPIs/bulk ops; still return best-effort.
    return max(result.rowcount or 0, 0)  # normalize None/-1 to 0


def ingest_stats(session: Session, rows: List[Dict[str, Any]]) -> int:
    """
    Insert stats rows into WeatherStats table in an idempotent way.

    Your error showed a unique constraint: uq_stats_station_year
    so we use ON CONFLICT DO NOTHING on (station_id, year).
    """
    if not rows:  # no data to insert
        return 0  # inserted 0 rows

    # Build a Postgres insert statement for WeatherStats.
    stmt = insert(WeatherStats).values(rows)  # bulk insert values

    # This matches your constraint (station_id, year) uniqueness.
    stmt = stmt.on_conflict_do_nothing(  # skip duplicates instead of throwing
        index_elements=["station_id", "year"]  # natural key for stats
    )

    result = session.execute(stmt)  # execute insert
    session.commit()  # persist data
    return max(result.rowcount or 0, 0)  # normalize to a safe integer


# ---------------------------------------------------------------------
# ✅ DATA LOADING (YOU PLUG YOUR EXISTING LOADER HERE)
# ---------------------------------------------------------------------
def load_weather_rows() -> List[Dict[str, Any]]:
    """
    Return a list of dict rows for Weather inserts.

    Replace this stub with your existing loader (CSV/JSON/API).
    Keep the output shape like:
        [{"station_id": "...", "date": "YYYY-MM-DD", "tmax_c": ..., ...}, ...]
    """
    # NOTE: This stub intentionally returns [] so the module is safe by default.
    return []


def load_stats_rows() -> List[Dict[str, Any]]:
    """
    Return a list of dict rows for WeatherStats inserts.

    Replace this stub with your existing stats aggregation/loader.
    Keep the output shape like:
        [{"station_id": "...", "year": 1985, "avg_max_temp_c": ..., ...}, ...]
    """
    # NOTE: This stub intentionally returns [] so the module is safe by default.
    return []


# ---------------------------------------------------------------------
# ✅ MAIN ENTRYPOINT FOR STARTUP
# ---------------------------------------------------------------------
def run_startup_ingestion() -> None:
    """
    Call this once at app startup.

    Behavior:
    - Create tables if missing
    - If marker says already ingested: skip
    - Else load rows and insert with ON CONFLICT DO NOTHING
    - Mark ingested only if both steps run without exception
    """
    ensure_tables_exist()  # always ensure schema exists first

    session: Optional[Session] = None  # init session handle
    try:
        session = SessionLocal()  # open a DB session

        if already_ingested(session):  # check marker/rowcounts
            print("[ingest] Data already present — skipping ingestion.")  # log skip
            return  # do nothing else

        # --- Load data using your pipeline functions (replace stubs above) ---
        weather_rows = load_weather_rows()  # fetch weather rows
        stats_rows = load_stats_rows()  # fetch stats rows

        # --- Insert in an idempotent way ---
        w_ins = ingest_weather(session, weather_rows)  # insert weather rows safely
        s_ins = ingest_stats(session, stats_rows)  # insert stats rows safely

        # Mark ingestion done (even if inserts were 0 because data already existed).
        mark_ingested(session)  # set the marker so future startups skip

        print(f"[ingest] Done. Inserted weather={w_ins}, stats={s_ins}.")  # summary log

    except SQLAlchemyError as e:
        # Roll back if anything went wrong during DB work.
        if session is not None:  # guard if session wasn't created
            session.rollback()  # rollback transaction safely
        # Re-raise so you can see the real error in logs.
        raise

    finally:
        # Always close the session.
        if session is not None:  # guard for safety
            session.close()  # release DB connection back to pool
