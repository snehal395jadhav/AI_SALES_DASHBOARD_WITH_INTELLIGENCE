FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application
COPY app.py /app/app.py
COPY intelligence.py /app/intelligence.py
COPY market_info.py /app/market_info.py
COPY frontend /app/frontend

RUN mkdir -p /app/data

EXPOSE 8000

# Gunicorn for production; single worker is plenty for this in-house dashboard.
# Use --timeout 600 because Market Info / Forecast AI calls (NVIDIA nemotron-3-ultra
# with a large reasoning budget, two sequential turns) can take several minutes,
# and parsing a 60k-row Sales workbook can take a minute too.
CMD ["gunicorn", "-w", "1", "-k", "sync", "--timeout", "600", \
     "-b", "0.0.0.0:8000", "app:app"]
