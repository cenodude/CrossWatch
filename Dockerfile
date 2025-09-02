# syntax=docker/dockerfile:1.7
# ---------- base ----------
FROM python:3.11-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app \
    TZ=Europe/Amsterdam

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---------- deps (cacheable) ----------
FROM base AS deps
# If you use constraints/hashes, copy them too
COPY requirements.txt /app/requirements.txt
# Optional: BuildKit cache for wheels (speeds rebuilds)
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt && \
    pip install uvicorn[standard]

# ---------- runtime (final image) ----------
FROM base AS runtime
# Create non-root user
ARG APP_USER=cwatch
ARG APP_UID=1000
ARG APP_GID=1000
RUN groupadd -g "${APP_GID}" "${APP_USER}" \
 && useradd -m -u "${APP_UID}" -g "${APP_GID}" -s /bin/bash "${APP_USER}"

# Copy site-packages from deps
COPY --from=deps /usr/local/lib/python3.11 /usr/local/lib/python3.11
COPY --from=deps /usr/local/bin/uvicorn /usr/local/bin/uvicorn

# Copy app (no leading slash in Dockerfile COPY)
COPY . /app

# Remove junk and common shadows (prevents import collisions like "packaging")
RUN rm -rf /app/.venv /app/.vscode /app/.idea || true \
 && find /app -type d -name "__pycache__" -prune -exec rm -rf {} + || true \
 && find /app -maxdepth 2 -type f -name "packaging.py" -delete || true \
 && find /app -maxdepth 2 -type d -name "packaging" -exec rm -rf {} + || true

# Scripts
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY docker/run-sync.sh   /usr/local/bin/run-sync.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/run-sync.sh

# Runtime env
ENV RUNTIME_DIR=/config \
    WEB_HOST=0.0.0.0 \
    WEB_PORT=8787 \
    WEBINTERFACE=yes \
    DEV_SHELL_ON_FAIL=yes

# Own files and drop privileges
RUN chown -R ${APP_USER}:${APP_USER} /app
VOLUME ["/config"]
EXPOSE 8787
USER ${APP_USER}

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
