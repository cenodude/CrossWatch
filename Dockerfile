# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

LABEL org.opencontainers.image.description="One brain for all your media syncs A single place to configure everything."

# --- env ---
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app \
    TZ=Europe/Amsterdam

# --- minimal OS deps ---
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tzdata bash curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- runtime user ---
ARG APP_USER=appuser
ARG APP_UID=1000
ARG APP_GID=1000
RUN groupadd -g "${APP_GID}" "${APP_USER}" \
 && useradd -m -u "${APP_UID}" -g "${APP_GID}" -s /bin/bash "${APP_USER}"

# --- deps
COPY requirements.txt /app/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# --- app code ---
COPY . /app

# --- cleanup & avoid import shadows ---
RUN rm -rf /app/.venv /app/.vscode /app/.idea || true \
 && find /app -type d -name "__pycache__" -prune -exec rm -rf {} + || true \
 && find /app -maxdepth 2 -type f -name "packaging.py" -delete || true \
 && find /app -maxdepth 2 -type d -name "packaging" -exec rm -rf {} + || true

# --- scripts ---
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY docker/run-sync.sh   /usr/local/bin/run-sync.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/run-sync.sh

# --- runtime env ---
ENV RUNTIME_DIR=/config \
    WEB_HOST=0.0.0.0 \
    WEB_PORT=8787 \
    WEBINTERFACE=yes \
    DEV_SHELL_ON_FAIL=yes

# --- healthcheck ---
HEALTHCHECK --interval=30s --timeout=5s --retries=5 \
  CMD ["python","-c","import os,socket,sys; s=socket.socket(); s.settimeout(2); p=int(os.environ.get('WEB_PORT','8787')); sys.exit(0 if s.connect_ex(('127.0.0.1',p))==0 else 1)"]

EXPOSE 8787
VOLUME ["/config"]

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
