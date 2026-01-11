
# Weather API Code Challenge — Step-by-Step README (Windows / PowerShell)

This repository exposes a FastAPI service that serves:
- **Daily weather records** (paginated)
- **Yearly weather statistics** (paginated)

It also includes an **idempotent ingestion step** that:
- creates tables if missing
- **skips ingestion** if data is already present (so re-running the app won’t crash with duplicate inserts)

This README is written for both:
- **another programmer** (who wants to run, debug, or extend)
- **a non-technical reviewer** (who wants a plain-English explanation)

---

## What we built (plain English)

When the API starts:
1. It checks the database.
2. If tables don’t exist, it creates them.
3. If the database already contains data, it **does not ingest again**.
4. The API serves JSON endpoints for weather data.

We also improved the API output so it is:
- easier to read (grouped fields and metadata)
- rounded to **2 decimal places** where appropriate (for stats and weather metrics)

---

## Key Files We Changed / Added

### `src/app/main.py`
- Main FastAPI entry point
- Runs ingestion at startup (but safely skips if already done)
- Provides endpoints:
  - `GET /api/weather`
  - `GET /api/weather/stats`
- Returns clean JSON with:
  - `metadata` (paging info)
  - `data` (human-readable objects)
- Rounds numeric values to **2 decimals**
- Removes internal DB `id` field from output

### `src/app/ingest_weather.py`
- Ingestion module (creates tables + inserts data)
- Adds a “do not ingest twice” guard
- Uses safe insert behavior to avoid duplicate key crashes

### `src/app/crud.py`
- Database query functions:
  - `get_weather(...)`
  - `get_weather_stats(...)`
- Implements filters + pagination

### `src/app/db.py`
- Database connection setup (Postgres URL, engine, sessions)

---

## Prerequisites (what you need installed)

- **Python 3.10+** (recommended)
- **PostgreSQL** running locally
- Windows PowerShell

---

## Step 1 — Open the project folder

In PowerShell:

```powershell
cd F:\CLOUD\SQL\code-challenge-template
