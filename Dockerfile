FROM python:3.11-slim

# Install uv — Astral's fast Python package manager.
# We copy the binary directly from their official image rather than
# pip-installing it, which keeps the image smaller and the install instant.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency manifest first, before source code.
# Docker builds layers in order — if pyproject.toml hasn't changed,
# the dependency install layer is reused from cache on subsequent builds.
# This means code-only changes don't re-download packages.
COPY pyproject.toml .

# Install dependencies into the system Python.
# --system: skip virtualenv creation — the container itself is the isolation boundary.
# --no-cache: keep the image lean by not storing the pip download cache.
RUN uv pip install --system --no-cache aiohttp asyncpg "python-dotenv>=1.0" "pendulum>=3.0"

# Copy source after deps so code changes don't invalidate the dependency layer.
COPY . .

# The pipeline runs once and exits — not a long-running server.
# docker compose handles restart policy; we just define the entry point.
CMD ["python", "main.py"]
