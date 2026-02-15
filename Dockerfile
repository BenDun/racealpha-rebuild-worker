FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for DuckDB + Postgres
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data lake directory
RUN mkdir -p /app/data_lake

# Environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PORT=8000

# Expose port for web UI
EXPOSE 8000

# Default: Run web UI (can be overridden to run rebuild_worker.py for cron)
CMD ["python", "web_ui.py"]
