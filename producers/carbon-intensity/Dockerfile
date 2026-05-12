# ============================================================================
# Carbon Intensity Producer - multi-stage Dockerfile
# ----------------------------------------------------------------------------
# Stage 1 (builder): use uv to install deps into the system python.
# Stage 2 (runtime): copy only the installed deps + source. Smaller final
# image, no build toolchain shipped to production.
# ============================================================================

# ---------- Stage 1: builder ----------
FROM python:3.11-slim AS builder

# uv installs faster than pip and produces deterministic resolutions
RUN pip install --no-cache-dir uv==0.4.18

WORKDIR /app

# Copy only the dependency manifest first, so layer caching reuses the
# pip-install layer when source changes but deps don't.
COPY pyproject.toml ./

# Install runtime deps (NOT dev deps) into the system Python.
# `--no-cache` keeps the layer small.
RUN uv pip install --system --no-cache \
    "httpx>=0.27" \
    "aiokafka>=0.11" \
    "pydantic>=2.7" \
    "structlog>=24.1" \
    "azure-identity>=1.17" \
    "aiohttp>=3.9" \
    "tenacity>=8.2"


# ---------- Stage 2: runtime ----------
FROM python:3.11-slim

# Run as a non-root user. Container Apps doesn't require this but it's
# a free security win.
RUN useradd --create-home --uid 10001 app

# Copy the installed Python packages from the builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy our source code
COPY src/ /app/src/

WORKDIR /app
USER app

# PYTHONUNBUFFERED=1 makes stdout/stderr line-buffered, so logs appear
# in `az containerapp logs show` in real time instead of being held in
# Python's internal buffer.
ENV PYTHONUNBUFFERED=1

# No HEALTHCHECK: Container Apps uses its own probes. We expose no port
# because this is a long-running poller, not an HTTP service.

CMD ["python", "-u", "src/main.py"]
