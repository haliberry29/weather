# src/app/models.py
from __future__ import annotations

from sqlalchemy import Column, Integer, String, Float, Date, UniqueConstraint, Index
from .db import Base


class Weather(Base):
    __tablename__ = "weather"

    id = Column(Integer, primary_key=True, index=True)
    station_id = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)

    # store in "nice" units to match API expectations
    max_temp_c = Column(Float, nullable=True)
    min_temp_c = Column(Float, nullable=True)
    precip_cm = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("station_id", "date", name="uq_weather_station_date"),
        Index("ix_weather_station_date", "station_id", "date"),
    )


class WeatherStats(Base):
    __tablename__ = "weather_stats"

    id = Column(Integer, primary_key=True, index=True)
    station_id = Column(String, nullable=False, index=True)
    year = Column(Integer, nullable=False, index=True)

    avg_max_temp_c = Column(Float, nullable=True)
    avg_min_temp_c = Column(Float, nullable=True)
    total_precip_cm = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("station_id", "year", name="uq_stats_station_year"),
        Index("ix_stats_station_year", "station_id", "year"),
    )
