# syntax=docker/dockerfile:1

# ═══════════════════════════════════════════════════════════════════════════════
# Multi-stage build: builder → distroless runtime
# ═══════════════════════════════════════════════════════════════════════════════

# ── Stage 1: Builder ─────────────────────────────────────────────────────────
# Use Bookworm (Debian 12) to match the distroless runtime glibc version.
FROM python:3.13-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Install build dependencies (gcc for packages with native extensions)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN uv pip install --system \
    feedparser>=6.0.11 \
    kafka-python>=2.0.2 \
    psycopg2-binary>=2.9.12 \
    pylatexenc>=2.10 \
    requests>=2.32.3 \
    sentence-transformers>=5.5.1 \
    tqdm>=4.67.3 \
    watchdog>=6.0.0 \
    --extra-index-url https://download.pytorch.org/whl/cpu

# Collect architecture-agnostic shared libraries into a flat staging directory.
# The "*.so*" wildcard ensures we grab the real file and any symlinks.
RUN mkdir -p /app/libs && \
    for lib in libz.so.1 libffi.so.8 libssl.so.3 libcrypto.so.3; do \
        cp /lib/$(uname -m)-linux-gnu/${lib}* /app/libs/ 2>/dev/null || \
        cp /lib/x86_64-linux-gnu/${lib}* /app/libs/ 2>/dev/null || \
        cp /lib/aarch64-linux-gnu/${lib}* /app/libs/ 2>/dev/null; \
    done

# Copy source code
COPY core/ ./core/
COPY pipeline/ ./pipeline/
COPY scripts/ ./scripts/
COPY sql/ ./sql/

# ── Stage 2: Distroless runtime ───────────────────────────────────────────────
FROM gcr.io/distroless/cc-debian12

WORKDIR /app

# Copy Python 3.13 runtime from builder
COPY --from=builder /usr/local/bin/python3.13 /usr/local/bin/python3.13
COPY --from=builder /usr/local/lib/libpython3.13.so.1.0 /usr/local/lib/libpython3.13.so.1.0
COPY --from=builder /usr/local/lib/libpython3.13.so /usr/local/lib/libpython3.13.so
COPY --from=builder /usr/local/lib/python3.13 /usr/local/lib/python3.13

# Copy shared libraries required by the Python standard library and dependencies.
COPY --from=builder /app/libs/* /lib/

# Copy application code
COPY --from=builder /app/core /app/core
COPY --from=builder /app/pipeline /app/pipeline
COPY --from=builder /app/scripts /app/scripts
COPY --from=builder /app/sql /app/sql

ENV PATH="/usr/local/bin:$PATH"
ENV PYTHONPATH="/app"

# Default database and Kafka endpoints inside Docker network
ENV DATABASE_URL="postgresql://bgd:bgd@postgres:5432/bgd"
ENV KAFKA_BOOTSTRAP="kafka:29092"

# Data directories are created via volume mounts in docker-compose.yml.
# Default command: run the continuous streaming pipeline.
CMD ["python3.13", "-m", "core.streamer"]
