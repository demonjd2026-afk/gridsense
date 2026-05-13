# ============================================================================
# ENTSO-E Producer - multi-stage Dockerfile
# Built from REPO ROOT to include producers/_common/. Build with:
#   az acr build --registry $ACR -t entsoe:vX -f producers/entsoe/Dockerfile .
# ============================================================================

# ---------- Stage 1: builder ----------
FROM python:3.11-slim AS builder

RUN pip install --no-cache-dir uv==0.4.18

WORKDIR /build

COPY producers/_common/ /build/_common/
COPY producers/entsoe/pyproject.toml /build/entsoe/

RUN uv pip install --system --no-cache \
    "httpx>=0.27" \
    "aiokafka>=0.11" \
    "pydantic>=2.7" \
    "structlog>=24.1" \
    "azure-identity>=1.17" \
    "aiohttp>=3.9" \
    "tenacity>=8.2" \
    "xmltodict>=0.13"

RUN uv pip install --system --no-cache /build/_common


# ---------- Stage 2: runtime ----------
FROM python:3.11-slim

RUN useradd --create-home --uid 10001 app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY producers/entsoe/src/ /app/src/

WORKDIR /app
USER app

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "src/main.py"]
