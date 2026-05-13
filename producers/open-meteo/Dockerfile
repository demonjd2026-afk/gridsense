# ============================================================================
# Open-Meteo Producer - multi-stage Dockerfile
# ----------------------------------------------------------------------------
# IMPORTANT: this Dockerfile expects the build context to be the REPO ROOT,
# not the producer directory, because we need to COPY in producers/_common/
# alongside producers/open-meteo/. Build with:
#   az acr build --registry $ACR -t open-meteo:vX -f producers/open-meteo/Dockerfile .
# ============================================================================

# ---------- Stage 1: builder ----------
FROM python:3.11-slim AS builder

RUN pip install --no-cache-dir uv==0.4.18

WORKDIR /build

# Copy both the shared package and the producer manifest first for layer caching
COPY producers/_common/ /build/_common/
COPY producers/open-meteo/pyproject.toml /build/open-meteo/

# Install third-party runtime deps
RUN uv pip install --system --no-cache \
    "httpx>=0.27" \
    "aiokafka>=0.11" \
    "pydantic>=2.7" \
    "structlog>=24.1" \
    "azure-identity>=1.17" \
    "aiohttp>=3.9" \
    "tenacity>=8.2"

# Install the shared _common package into system Python.
# Non-editable so the final image doesn't need the source at /build.
RUN uv pip install --system --no-cache /build/_common


# ---------- Stage 2: runtime ----------
FROM python:3.11-slim

RUN useradd --create-home --uid 10001 app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY producers/open-meteo/src/ /app/src/

WORKDIR /app
USER app

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "src/main.py"]
