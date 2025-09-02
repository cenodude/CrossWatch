# ./Dockerfile
FROM python:3.12-slim

# Install OS deps
RUN apt-get update \
 && apt-get install -y --no-install-recommends cron tzdata \
 && rm -rf /var/lib/apt/lists/*

# Python runtime flags
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy application files
COPY plex_simkl_watchlist_sync.py /app/
COPY config.example.json /app/
COPY plex_token_helper.py /app/
COPY webapp.py /app/
COPY _auth_helper.py /app/
COPY _FastAPI.py /app/
COPY _scheduling.py /app/
COPY _TMDB.py /app/
COPY _watchlist.py /app/
COPY _statistics.py /app/

# Copy assets folder
COPY assets/ /app/assets/

# Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir \
    plexapi \
    requests \
    fastapi \
    uvicorn \
    pydantic \
    pillow \
    packaging

# Copy helper scripts
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY docker/run-sync.sh   /usr/local/bin/run-sync.sh

# Permissions + cron log
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/run-sync.sh \
 && touch /var/log/cron.log

# Default runtime configuration
ENV TZ="Europe/Amsterdam" \
    RUNTIME_DIR="/config" \
    CRON_SCHEDULE="0 0 * * *" \
    PUID="1000" \
    PGID="1000" \
    SYNC_CMD="python /app/plex_simkl_watchlist_sync.py --sync" \
    INIT_CMD="python /app/plex_simkl_watchlist_sync.py --init-simkl redirect --bind 0.0.0.0:8787" \
    WEBINTERFACE="yes"

# First-run OAuth environment variables (optional)
ENV PLEX_ACCOUNT_TOKEN="" \
    SIMKL_CLIENT_ID="" \
    SIMKL_CLIENT_SECRET=""

# Web UI port
EXPOSE 8787

# Persist configuration/state
VOLUME ["/config"]

# Entrypoint decides what to launch; by default we want the web interface.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["python", "/app/webapp.py", "--host", "0.0.0.0", "--port", "8787"]
