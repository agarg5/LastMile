# Backend Dockerfile for Railway deployment
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend application code (includes CSV seed data)
COPY backend/ ./backend/

# Create directory for SQLite database (fallback for local dev)
RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1
ENV FLASK_DEBUG=false
ENV ALLOW_UNSAFE_WERKZEUG=false

EXPOSE 8000

# Initialize DB schema, seed data, then start gunicorn with eventlet for WebSocket
CMD ["sh", "-c", "cd /app/backend && python -c 'from app import init_db; init_db(); print(\"DB initialized\")' && python load_data.py 2>/dev/null || true && gunicorn --worker-class eventlet -w 1 --bind 0.0.0.0:${PORT:-8000} app:app"]
