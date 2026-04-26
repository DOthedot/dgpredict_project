"""
Pipeline configuration — loaded once at startup from environment variables.

Pipeline position: read by ALL modules.

All settings come from environment variables so the same Docker image can be
pointed at different databases or tuned without rebuilding. Defaults are set
for local development; Docker Compose overrides POSTGRES_HOST to 'postgres'.

To customise: copy .env.example → .env and edit. The dotenv load happens here
so every module that imports config automatically picks up .env values.
"""

import os
from dotenv import load_dotenv

# Load .env file if present. In Docker, variables come from env_file in
# docker-compose.yml instead, so load_dotenv() is a no-op there.
load_dotenv()

# ── Database ──────────────────────────────────────────────────────────────────

POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB", "polymarket")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "polymarket")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "polymarket")

# asyncpg DSN format — used to create the connection pool.
DB_DSN = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# asyncpg pool sizing — min keeps connections warm, max prevents overwhelming
# Postgres (default max_connections = 100). We use 10 max for safety.
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "2"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "50"))

# Seconds to wait for a free connection from the pool before raising TimeoutError.
# Without this, pool.acquire() hangs indefinitely when all connections are busy
# (e.g. during a spike or misconfigured pool size), stalling the pipeline forever.
DB_ACQUIRE_TIMEOUT = int(os.getenv("DB_ACQUIRE_TIMEOUT", "30"))

# ── Polymarket Gamma API ──────────────────────────────────────────────────────

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

# Path to the CSV of 500 football event IDs inside the container.
# Mounted as a read-only volume at /data/ via docker-compose.yml.
EVENT_IDS_PATH = os.getenv("EVENT_IDS_PATH", "/data/football_event_ids_500.csv")

# ── Async fetch tuning ────────────────────────────────────────────────────────

# Max concurrent HTTP requests. Above ~30 the Gamma API starts returning 429s.
# Below 10 it's too slow to process 500 events in a reasonable time.
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "30"))

# Per-request timeout in seconds. Gamma API is generally fast (<2s), but
# we give 30s headroom for occasional slow responses before retrying.
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

# Max retry attempts per failed request (429, 5xx, timeout).
# 3 retries with exponential backoff = up to 14 seconds of wait per entity.
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# Number of event/market IDs per API call.
# 50 IDs per chunk keeps query strings short while minimising total API calls.
# Must also pass limit=50 explicitly — the API defaults to limit=20 and silently
# truncates any chunk larger than 20 without this override.
# 500 events → 10 chunks of 50, all fired concurrently via the semaphore.
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "50"))
