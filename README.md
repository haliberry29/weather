Weather Data API (link to aws deployed challenge: http://18.208.245.176:8000/docs)

A production-quality FastAPI + PostgreSQL service for ingesting, aggregating, and serving historical weather data.
This project implements a safe, idempotent ingestion pipeline, yearly weather statistics, and paginated REST APIs, with a one-command local runner and a clear AWS deployment path.

📌 Features

Daily Weather API

Paginated access to daily observations

Filters by station_id, start_date, and end_date

Yearly Statistics API

Aggregated yearly statistics per station

Average max/min temperature and total precipitation

Safe & Idempotent Ingestion

PostgreSQL UPSERT with uniqueness guarantees

Automatic skip if data already exists (override supported)

Performance-Oriented

Batched commits

Composite database indexes

Developer-Friendly

One-command local run

## AWS Deployment

This project can be deployed to AWS using **EC2 + Docker Compose**.

Step-by-step instructions are available here:

 [AWS Deployment Guide](aws/README.md)


Swagger/OpenAPI documentation

Clean project structure

 Architecture Overview
wx_data/                # Raw NOAA weather station files
│
├── ingest_weather.py   # Ingests raw data → weather table
├── compute_stats.py    # Computes yearly stats → weather_stats table
│
└── FastAPI
    ├── /api/weather        # Daily observations
    └── /api/weather/stats  # Yearly aggregates


Storage

PostgreSQL

weather (daily observations)

weather_stats (yearly aggregates)

 Database Schema
weather

station_id (string)

date (date)

max_temp_c (float, nullable)

min_temp_c (float, nullable)

precip_cm (float, nullable)

Constraints & Indexes

Unique: (station_id, date)

Index: (station_id, date)

weather_stats

station_id (string)

year (integer)

avg_max_temp_c (float)

avg_min_temp_c (float)

total_precip_cm (float)

Constraints & Indexes

Unique: (station_id, year)

Index: (station_id, year)

 Setup & Run Locally
Prerequisites

Python 3.10+

PostgreSQL

Git

Windows PowerShell (or equivalent shell)

1️. Clone the repository
git clone https://github.com/haliberry29/weather.git
cd weather

2️. Create and activate virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

3️. Create PostgreSQL database
CREATE DATABASE weather;

4️. Set database connection
$env:DATABASE_URL="postgresql+psycopg2://postgres:<password>@localhost:5432/weather"

5️. Run everything (recommended)
.\run.ps1


This will:

Ingest weather data (skips if already loaded)

Compute yearly statistics

Start the FastAPI server

 API Documentation

Once running, open:

http://127.0.0.1:8000/docs

Example Endpoints
Daily Weather
GET /api/weather?page=1&page_size=5
GET /api/weather?station_id=USC00110072
GET /api/weather?start_date=1987-01-01&end_date=1987-12-31

Yearly Statistics
GET /api/weather/stats?page=1&page_size=5
GET /api/weather/stats?station_id=USC00110072
GET /api/weather/stats?year=1987

Sample Response
{
  "total": 4820,
  "page": 1,
  "page_size": 3,
  "items": [
    {
      "station_id": "USC00110072",
      "year": 1987,
      "avg_max_temp_c": 17.76,
      "avg_min_temp_c": 6.33,
      "total_precip_cm": 79.36
    }
  ]
}

 Ingestion Behavior (Important)

Ingestion is idempotent

If data already exists, ingestion automatically skips

To force re-ingestion:

$env:FORCE_INGEST="1"
.\run.ps1


This ensures fast re-runs and safe production behavior.


## AWS Deployment

This project can be deployed to AWS using **EC2 + Docker Compose**.

Step-by-step instructions are available here:

[AWS Deployment Guide](aws/README.md)
