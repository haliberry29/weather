# run.ps1
# ------------------------------------------------------------
# End-to-end runner:
# 1) Set DATABASE_URL
# 2) Activate venv
# 3) (Optional) FORCE_INGEST for re-ingest
# 4) Ingest (auto-skip unless FORCE_INGEST=1)
# 5) Compute stats
# 6) Start API
# ------------------------------------------------------------

$ErrorActionPreference = "Stop"

# -----------------------------
# CONFIG: Database connection
# -----------------------------
$env:DATABASE_URL = "postgresql+psycopg2://postgres:1966@localhost:5432/weather"

# -----------------------------
# CONFIG: Re-ingest toggle
#   - Default is "0" (do NOT re-ingest if data exists)
#   - Set to "1" to force re-ingestion
# -----------------------------
# To force re-ingestion for a single run, either:
#   A) set this line to "1", OR
#   B) run: $env:FORCE_INGEST="1"; .\run.ps1
if (-not $env:FORCE_INGEST) {
    $env:FORCE_INGEST = "0"
}

# -----------------------------
# VENV: Ensure .venv exists
# -----------------------------
if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
    throw "Missing .venv. Create it first: python -m venv .venv"
}

# Use venv python explicitly (prevents using Anaconda Python by accident)
$PY = ".\.venv\Scripts\python.exe"

# Activate venv for consistency (optional, but nice)
if (Test-Path ".\.venv\Scripts\Activate.ps1") {
    . .\.venv\Scripts\Activate.ps1
}

# -----------------------------
# Print run configuration
# -----------------------------
Write-Host ""
Write-Host "================ RUN CONFIG ================"
Write-Host "DATABASE_URL  = $env:DATABASE_URL"
Write-Host "FORCE_INGEST  = $env:FORCE_INGEST (1=force re-ingest, 0=skip if loaded)"
Write-Host "PYTHON        = $PY"
Write-Host "==========================================="
Write-Host ""

# -----------------------------
# Step 1: Ingest
# -----------------------------
if ($env:FORCE_INGEST -eq "1") {
    Write-Host "[1/3] Ingesting weather data (FORCED re-ingest enabled)..."
} else {
    Write-Host "[1/3] Ingesting weather data (auto-skip if already loaded)..."
}

# Run ingestion in unbuffered mode so progress logs show immediately
& $PY -u -m src.ingest_weather

# -----------------------------
# Step 2: Compute stats
# -----------------------------
Write-Host ""
Write-Host "[2/3] Computing yearly weather statistics..."
& $PY -m src.compute_stats

# -----------------------------
# Step 3: Start API
# -----------------------------
Write-Host ""
Write-Host "[3/3] Starting API..."
Write-Host "Swagger UI: http://127.0.0.1:8000/docs"
Write-Host "Weather:    http://127.0.0.1:8000/api/weather?page=1&page_size=5"
Write-Host "Stats:      http://127.0.0.1:8000/api/weather/stats?page=1&page_size=5"
& $PY -m uvicorn src.app.main:app --reload
