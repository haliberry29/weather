"""
src/app/main.py

FastAPI entry point for the Weather API.

Features:
- Startup ingestion with idempotent skip logic
- Clean, human-readable API responses
- Values rounded to 2 decimal places
- No internal DB fields exposed (e.g., id)
- Stable single-process startup (Windows-safe)
"""

from __future__ import annotations

import os
import logging
from contextlib import asynccontextmanager
from datetime import date as DateType
from decimal import Decimal
from typing import Any, Dict, Generator, Optional

from fastapi import Depends, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from src.app.db import SessionLocal
from src.app import crud
from src.app.ingest_weather import run_startup_ingestion


# -------------------------------------------------
# Logging configuration
# -------------------------------------------------
logger = logging.getLogger("app")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


# -------------------------------------------------
# Database session dependency
# -------------------------------------------------
def get_db() -> Generator[Session, None, None]:
    """Provide a database session per request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# -------------------------------------------------
# Helper: round numeric values safely
# -------------------------------------------------
def round_2(value: Any) -> Any:
    """Round floats / Decimals to 2 decimal places."""
    if isinstance(value, Decimal):
        return round(float(value), 2)
    if isinstance(value, float):
        return round(value, 2)
    return value


# -------------------------------------------------
# Application lifespan (startup / shutdown)
# -------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run ingestion once at startup (skip if already ingested)."""
    try:
        logger.info("[startup] Running ingestion check...")
        run_startup_ingestion()
        logger.info("[startup] Ingestion step finished.")
    except Exception:
        logger.exception("[startup] Ingestion failed.")
        raise

    yield

    logger.info("[shutdown] Application shutting down.")


# -------------------------------------------------
# FastAPI app instance
# -------------------------------------------------
app = FastAPI(
    title=os.getenv("APP_TITLE", "Weather API"),
    version=os.getenv("APP_VERSION", "0.1.0"),
    lifespan=lifespan,
)


# -------------------------------------------------
# CORS (development-friendly defaults)
# -------------------------------------------------
origins = os.getenv("CORS_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in origins],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------------------------------
# Meta endpoints
# -------------------------------------------------
@app.get("/", tags=["meta"])
def root():
    return {"status": "ok", "docs": "/docs"}


@app.get("/health", tags=["meta"])
def health():
    return {"status": "healthy"}


@app.get("/ping", tags=["meta"])
def ping():
    return {"ping": "pong"}


# -------------------------------------------------
# Daily weather endpoint
# -------------------------------------------------
@app.get("/api/weather", tags=["weather"])
def api_weather(
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=500),
    station_id: Optional[str] = Query(None),
    date: Optional[DateType] = Query(None),
    db: Session = Depends(get_db),
):
    total, rows, total_pages, offset = crud.get_weather(
        db=db,
        page=page,
        page_size=page_size,
        station_id=station_id,
        date=date,
    )

    data = []
    for r in rows:
        data.append({
            "station_id": r.station_id,
            "date": r.date.isoformat(),
            "temperature_celsius": {
                "max": round_2(r.max_temp_c),
                "min": round_2(r.min_temp_c),
            },
            "precipitation_cm": round_2(r.precip_cm),
        })

    return {
        "metadata": {
            "total_records": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        },
        "data": data,
    }


# -------------------------------------------------
# Yearly weather statistics endpoint (CLEAN FORMAT)
# -------------------------------------------------
@app.get("/api/weather/stats", tags=["weather"])
def api_weather_stats(
    page: int = Query(1, ge=1),
    page_size: int = Query(3, ge=1, le=500),
    station_id: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    total, rows, total_pages, offset = crud.get_weather_stats(
        db=db,
        page=page,
        page_size=page_size,
        station_id=station_id,
        year=year,
    )

    data = []
    for r in rows:
        data.append({
            "station_id": r.station_id,
            "year": r.year,
            "temperature_celsius": {
                "average_max": round_2(r.avg_max_temp_c),
                "average_min": round_2(r.avg_min_temp_c),
            },
            "precipitation_cm": {
                "total": round_2(r.total_precip_cm),
            },
        })

    return {
        "metadata": {
            "total_records": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        },
        "data": data,
    }


# -------------------------------------------------
# Local development entrypoint
# -------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.app.main:app",
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
        reload=False,  # keep OFF on Windows
    )
