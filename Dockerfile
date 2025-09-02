# Dev-friendly container for CrossWatch (modular platform)
FROM python:3.11-slim

# System deps (bash, tzdata, git, build tools minimal), uvicorn for dev server
RUN apt-get update && apt-get install -y --no-install-recommends \    bash ca-certificates tzdata curl git \    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Copy everything (dev intent). We'll clean common junk right after.
COPY . /app

# Remove junk we don't want inside the image
RUN rm -rf /app/.venv /app/.vscode || true \    && find /app -type d -name "__pycache__" -prune -exec rm -rf {} + || true

# Install dependencies if present
# - prefer locked deps when available
RUN if [ -f requirements.txt ]; then pip install -r requirements.txt; fi \    && pip install --no-cache-dir uvicorn

# Non-root dev user (configurable via env at runtime)
ENV PUID=1000 PGID=1000 TZ=Europe/Amsterdam

# Expose default web port
EXPOSE 8787

# Scripts
COPY /docker/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY /docker/run-sync.sh /usr/local/bin/run-sync.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/run-sync.sh

# Default runtime env
ENV RUNTIME_DIR=/config \
    WEB_HOST=0.0.0.0 \
    WEB_PORT=8787 \
    WEBINTERFACE=yes \
    DEV_SHELL_ON_FAIL=yes

VOLUME ["/config"]

# Start via entrypoint (starts web UI; falls back to shell on failure when DEV_SHELL_ON_FAIL=yes)
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
