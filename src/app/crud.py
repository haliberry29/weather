"""
src/app/crud.py

Query helpers for Weather and WeatherStats.

Functions return:
    (total, rows, total_pages, offset)

- total: total rows matching filters
- rows: list of ORM objects for the current page
- total_pages: number of pages given page_size
- offset: offset used for pagination
"""

from __future__ import annotations  # forward refs

from datetime import date as DateType  # date type for filters
from math import ceil  # compute total pages
from typing import List, Optional, Tuple  # typing

from sqlalchemy.orm import Session  # DB session

from .models import Weather, WeatherStats  # ORM models


def get_weather(
    db: Session,  # database session
    page: int,  # 1-indexed page number
    page_size: int,  # rows per page
    station_id: Optional[str] = None,  # optional station filter
    date: Optional[DateType] = None,  # optional date filter
) -> Tuple[int, List[Weather], int, int]:
    """
    Fetch paginated daily weather rows.
    """
    q = db.query(Weather)  # start query on Weather table

    if station_id:  # apply station filter if provided
        q = q.filter(Weather.station_id == station_id)

    if date:  # apply date filter if provided
        q = q.filter(Weather.date == date)

    q = q.order_by(Weather.station_id.asc(), Weather.date.asc())  # stable ordering

    total = q.count()  # count total rows matching filters
    total_pages = ceil(total / page_size) if page_size else 0  # compute total pages
    offset = (page - 1) * page_size  # compute offset for pagination

    rows = q.offset(offset).limit(page_size).all()  # fetch paginated rows
    return total, rows, total_pages, offset  # return pagination tuple


def get_weather_stats(
    db: Session,  # database session
    page: int,  # 1-indexed page number
    page_size: int,  # rows per page
    station_id: Optional[str] = None,  # optional station filter
    year: Optional[int] = None,  # optional year filter
) -> Tuple[int, List[WeatherStats], int, int]:
    """
    Fetch paginated yearly weather stats rows.
    """
    q = db.query(WeatherStats)  # start query on WeatherStats table

    if station_id:  # apply station filter if provided
        q = q.filter(WeatherStats.station_id == station_id)

    if year is not None:  # apply year filter if provided (0 is valid, so check None)
        q = q.filter(WeatherStats.year == year)

    q = q.order_by(WeatherStats.station_id.asc(), WeatherStats.year.asc())  # stable ordering

    total = q.count()  # count total rows matching filters
    total_pages = ceil(total / page_size) if page_size else 0  # compute total pages
    offset = (page - 1) * page_size  # compute offset for pagination

    rows = q.offset(offset).limit(page_size).all()  # fetch paginated rows
    return total, rows, total_pages, offset  # return pagination tuple
