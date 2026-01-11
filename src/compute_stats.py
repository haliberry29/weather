# src/compute_stats.py

# ------------------------------------------------------------
# Enable postponed evaluation of type hints (Python 3.10+)
# ------------------------------------------------------------
from __future__ import annotations

# ------------------------------------------------------------
# Used for timing logs and date extraction
# ------------------------------------------------------------
from datetime import datetime

# ------------------------------------------------------------
# SQLAlchemy tools:
# - func: avg/sum
# - extract: extract year from date
# - Integer: correct SQLAlchemy type (NOT Python int)
# ------------------------------------------------------------
from sqlalchemy import func, extract, Integer

# ------------------------------------------------------------
# SQLAlchemy session manager
# ------------------------------------------------------------
from sqlalchemy.orm import Session

# ------------------------------------------------------------
# DB engine (configured via DATABASE_URL)
# ------------------------------------------------------------
from src.app.db import engine

# ------------------------------------------------------------
# ORM models
# ------------------------------------------------------------
from src.app.models import Weather, WeatherStats


def main() -> None:
    """
    Compute yearly statistics per station and store in weather_stats.

    For each (station_id, year):
      - avg_max_temp_c = AVG(max_temp_c)
      - avg_min_temp_c = AVG(min_temp_c)
      - total_precip_cm = SUM(precip_cm)

    NOTE: SQL aggregates ignore NULL values by default.
    """

    # Record start time for log output
    start = datetime.now()

    # Track how many stat rows we wrote
    rows_written = 0

    # Open DB session
    with Session(engine) as db:
        # Build a reusable "year expression" for SELECT and GROUP BY
        year_expr = extract("year", Weather.date).cast(Integer)

        # Build an aggregation query grouped by station and year
        query = (
            db.query(
                # Station identifier
                Weather.station_id.label("station_id"),
                # Year derived from Weather.date
                year_expr.label("year"),
                # Average max temp across that year
                func.avg(Weather.max_temp_c).label("avg_max_temp_c"),
                # Average min temp across that year
                func.avg(Weather.min_temp_c).label("avg_min_temp_c"),
                # Total precipitation across that year
                func.sum(Weather.precip_cm).label("total_precip_cm"),
            )
            # Group results by station_id and year
            .group_by(Weather.station_id, year_expr)
        )

        # Execute query (returns one row per station-year)
        results = query.all()

        # Upsert each aggregated row into WeatherStats
        for r in results:
            # db.merge() will INSERT if not present, UPDATE if present
            db.merge(
                WeatherStats(
                    station_id=r.station_id,
                    year=int(r.year),
                    avg_max_temp_c=r.avg_max_temp_c,
                    avg_min_temp_c=r.avg_min_temp_c,
                    total_precip_cm=r.total_precip_cm,
                )
            )
            rows_written += 1

        # Commit all changes
        db.commit()

    # Record end time
    end = datetime.now()

    # Print final summary
    print(
        f"[stats] start={start.isoformat()} end={end.isoformat()} rows={rows_written}",
        flush=True,
    )


# Standard entry point guard
if __name__ == "__main__":
    main()
