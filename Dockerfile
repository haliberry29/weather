FROM python:3.11-slim

WORKDIR /app

# deps for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev curl && rm -rf /var/lib/apt/lists/*

# install python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# copy app
COPY . /app

# expose port
EXPOSE 8000

# run API
CMD ["python","-m","uvicorn","src.app.main:app","--host","0.0.0.0","--port","8000"]
