"""
Async fetcher for the Polymarket Gamma API.

Pipeline position: CSV + Gamma API → [fetcher.py] → callbacks → bronze/loader.py

Reads from:
    - data/football_event_ids_500.csv            — 500 football event IDs
    - https://gamma-api.polymarket.com/events    — events with nested markets + series
    - https://gamma-api.polymarket.com/markets   — volume24hr per market

Returns:
    FetchResult with event/market/series counts and FetchError objects for
    dead-lettering in bronze.fetch_errors. Full records are never accumulated
    in memory — each chunk is written to Postgres immediately via callbacks.

Fetch strategy (pipelined per event chunk):
    For each chunk of 50 events, the coroutine does both passes in sequence:

    Step 1 — GET /events?id=<50 IDs>&limit=50
        Returns events with nested markets[] and series[].
        Markets from this response have all fields EXCEPT volume24hr.
        Events + series are written to bronze immediately via on_events_chunk.

    Step 2 — GET /markets?id=<market IDs>&limit=N  (for this chunk's markets)
        Fetches full market records to get volume24hr.
        volume24hr is only available from the /markets endpoint directly.
        Returned markets are merged with Step 1 data and written via on_markets_chunk.
        Markets not returned by /markets are written using Step 1 data with volume24hr=0
        (typically negRisk sub-markets that the /markets endpoint doesn't serve individually).

    All 10 chunk coroutines run concurrently via asyncio.gather(), so Step 1 and
    Step 2 for different chunks overlap — no global accumulation between phases.

    limit= must be set explicitly on all requests — the API defaults to limit=20
    and silently truncates longer chunks without this parameter.

Concurrency design:
    - asyncio.Semaphore(MAX_CONCURRENT_REQUESTS): caps in-flight HTTP requests.
      Above ~30 concurrent requests the Gamma API starts returning 429s.
    - Shared aiohttp.ClientSession with TCP keep-alive reuses connections,
      avoiding a new TLS handshake (~100ms) per request.
    - asyncio.gather() fires all chunk coroutines concurrently within the semaphore.
    - Shared sets (returned_event_ids, seen_series_ids) are safe without locks because
      asyncio is single-threaded cooperative multitasking — only one coroutine runs
      at a time between await points, so no concurrent mutation occurs.

Error handling (per request):
    - 429: respect Retry-After header (default 5s), retry up to MAX_RETRIES.
    - 5xx: exponential backoff (2^attempt seconds), retry up to MAX_RETRIES.
    - 404: log and skip — entity doesn't exist on Polymarket, no point retrying.
    - timeout/network: exponential backoff, retry up to MAX_RETRIES.
    - All failures return FetchError objects; the pipeline continues without them.
"""

import asyncio
import csv
import json
import random
from dataclasses import dataclass, field

import aiohttp

from src.config import (
    CHUNK_SIZE,
    EVENT_IDS_PATH,
    GAMMA_BASE_URL,
    MAX_CONCURRENT_REQUESTS,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from src.logger import get_logger

logger = get_logger("fetcher")


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class FetchError:
    """Represents a single failed API fetch, stored in bronze.fetch_errors."""
    entity_type:   str   # 'event' | 'market' | 'series'
    entity_id:     str   # the ID that failed
    endpoint:      str   # full URL attempted
    error_type:    str   # '429' | '5xx' | 'timeout' | '404' | 'network' | 'unknown'
    error_message: str
    retry_count:   int


@dataclass
class FetchResult:
    """
    Aggregated result of a full pipeline fetch pass.

    With streaming writes, full records are written to Postgres per chunk and
    not accumulated in memory. Only counts and errors are tracked here.
    """
    events_count:  int = 0
    markets_count: int = 0
    series_count:  int = 0
    errors: list[FetchError] = field(default_factory=list)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_event_ids(path: str) -> list[str]:
    """
    Load football event IDs from the provided CSV file.

    The CSV has an 'event_id' column (confirmed from the actual file).
    Returns a plain list of string IDs for chunking.
    """
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        ids = [row["event_id"].strip() for row in reader]
    logger.info(f"Loaded {len(ids)} event IDs from {path}")
    return ids


def _chunk(lst: list, size: int) -> list[list]:
    """
    Split a list into sub-lists of at most `size` items.

    We chunk event/market IDs before sending to the API because the Gamma API
    silently truncates or errors on very long query strings. Chunks of 50
    stay safely under the limit while keeping the number of calls small.
    """
    return [lst[i : i + size] for i in range(0, len(lst), size)]


# ── Async fetcher ─────────────────────────────────────────────────────────────

class AsyncFetcher:
    """
    Context-manager wrapper around aiohttp for concurrent Gamma API fetching.

    Usage:
        async with AsyncFetcher() as fetcher:
            result = await fetcher.fetch_all()

    The shared ClientSession is created on __aenter__ and closed on __aexit__,
    ensuring TCP connections are reused across all requests in a pipeline run
    and are properly released at the end.
    """

    def __init__(self) -> None:
        # Semaphore caps concurrent in-flight requests at MAX_CONCURRENT_REQUESTS.
        # Without this, asyncio.gather() on 500+ coroutines would open hundreds of
        # simultaneous connections, triggering rate limits and exhausting sockets.
        self._sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "AsyncFetcher":
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        # A single shared session reuses the TCP connection pool (keep-alive).
        # Creating a new session per request would add a TLS handshake (~100ms)
        # to every call — a significant penalty for 500+ sequential chunks.
        self._session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, *_) -> None:
        if self._session:
            await self._session.close()

    # ── Core HTTP primitive ───────────────────────────────────────────────────

    async def _get(
        self,
        url: str,
        params: list[tuple[str, str]],
        entity_type: str,
        entity_id: str,
    ) -> list | dict | FetchError:
        """
        Make one GET request with retry logic. Returns parsed JSON or FetchError.

        Args:
            url:         Full endpoint URL.
            params:      List of (key, value) tuples — supports repeated keys
                         for multi-ID filtering (?id=1&id=2&id=3).
            entity_type: 'event' | 'market' for error reporting.
            entity_id:   Human-readable ID for log messages.

        Returns:
            Parsed JSON (list or dict) on success, FetchError on final failure.
        """
        async with self._sem:
            for attempt in range(MAX_RETRIES + 1):
                try:
                    async with self._session.get(url, params=params) as resp:

                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            logger.debug(
                                f"GET {url} | entity={entity_id} | status=200"
                            )
                            return data

                        elif resp.status == 429:
                            # Exponential backoff + full jitter for 429s.
                            # Flat retries cause a thundering herd — all concurrent
                            # requests wake up at the same instant and re-trigger the
                            # rate limit. Jitter spreads them out randomly so the API
                            # sees a trickle instead of a burst.
                            # Formula: min(cap, base * 2^attempt) * random(0, 1)
                            # Respects Retry-After as the floor if the API provides one.
                            retry_after = int(resp.headers.get("Retry-After", "0"))
                            backoff = min(30, 1 * (2 ** attempt))
                            wait = max(retry_after, backoff) * random.random() + 1
                            logger.warning(
                                f"429 rate-limited | {entity_type}={entity_id} | "
                                f"attempt={attempt + 1}/{MAX_RETRIES} | wait={wait:.1f}s"
                            )
                            if attempt < MAX_RETRIES:
                                await asyncio.sleep(wait)
                                continue
                            return FetchError(
                                entity_type, entity_id, url, "429",
                                "Rate limited — max retries exceeded", attempt,
                            )

                        elif resp.status == 404:
                            # 404 means this entity simply doesn't exist on Polymarket.
                            # Retrying would just return 404 again — skip immediately.
                            logger.warning(f"404 not found | {entity_type}={entity_id}")
                            return FetchError(
                                entity_type, entity_id, url, "404", "Not found", 0
                            )

                        elif resp.status >= 500:
                            wait = 2 ** attempt
                            logger.warning(
                                f"{resp.status} server error | {entity_type}={entity_id} | "
                                f"attempt={attempt + 1}/{MAX_RETRIES} | wait={wait}s"
                            )
                            if attempt < MAX_RETRIES:
                                await asyncio.sleep(wait)
                                continue
                            return FetchError(
                                entity_type, entity_id, url,
                                "5xx", f"HTTP {resp.status}", attempt,
                            )

                        else:
                            return FetchError(
                                entity_type, entity_id, url,
                                str(resp.status), f"Unexpected HTTP {resp.status}", attempt,
                            )

                except asyncio.TimeoutError:
                    wait = 2 ** attempt
                    logger.warning(
                        f"Timeout | {entity_type}={entity_id} | "
                        f"attempt={attempt + 1}/{MAX_RETRIES} | wait={wait}s"
                    )
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(wait)
                        continue
                    return FetchError(
                        entity_type, entity_id, url, "timeout", "Request timed out", attempt
                    )

                except aiohttp.ClientError as exc:
                    wait = 2 ** attempt
                    logger.warning(
                        f"Network error | {entity_type}={entity_id} | "
                        f"{exc} | attempt={attempt + 1}/{MAX_RETRIES} | wait={wait}s"
                    )
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(wait)
                        continue
                    return FetchError(
                        entity_type, entity_id, url, "network", str(exc), attempt
                    )

                except Exception as exc:
                    logger.error(
                        f"Unexpected error | {entity_type}={entity_id} | {exc}",
                        exc_info=True,
                    )
                    return FetchError(
                        entity_type, entity_id, url, "unknown", str(exc), attempt
                    )

        return FetchError(
            entity_type, entity_id, url, "exhausted", "Max retries exceeded", MAX_RETRIES
        )

    # ── Pass 1: events ────────────────────────────────────────────────────────

    async def _fetch_event_chunk(
        self, event_ids: list[str]
    ) -> tuple[list[dict], list[FetchError]]:
        """
        Fetch a chunk of up to CHUNK_SIZE events in one API call.

        GET /events?id=id1&id=id2&... returns a list of event objects,
        each with nested markets[] and series[] arrays already included.
        This means one call gives us events + base market data + series.

        limit must be explicitly set to match the chunk size — the API
        defaults to limit=20 per request, so without it we'd only get
        20 events back from each chunk of 50, silently dropping the rest.
        """
        # Pass limit= equal to chunk size so the API returns all requested IDs.
        # Without this, the API defaults to limit=20 and silently truncates any
        # chunk larger than 20, returning 20 results regardless of how many IDs were sent.
        params = [("id", eid) for eid in event_ids]
        params.append(("limit", str(len(event_ids))))
        label = f"{event_ids[0]}..{event_ids[-1]}"
        result = await self._get(
            f"{GAMMA_BASE_URL}/events", params, "event", label
        )

        if isinstance(result, FetchError):
            # If the whole chunk fails, record one error per event_id.
            return [], [
                FetchError(
                    "event", eid, f"{GAMMA_BASE_URL}/events",
                    result.error_type, result.error_message, result.retry_count,
                )
                for eid in event_ids
            ]

        events = result if isinstance(result, list) else []
        return events, []

    # ── Pass 2: market volume24hr ─────────────────────────────────────────────

    async def _fetch_market_chunk(
        self, market_ids: list[str]
    ) -> tuple[list[dict], list[FetchError]]:
        """
        Fetch full market records for a chunk of market IDs.

        The markets nested inside event responses are missing volume24hr —
        that field is only available by querying /markets directly.
        We batch market IDs (50 at a time) to keep API calls manageable.

        Same limit fix as events — API defaults to 20, we need the full chunk.
        """
        # Same limit override — API defaults to 20, must match chunk size.
        params = [("id", mid) for mid in market_ids]
        params.append(("limit", str(len(market_ids))))
        label = f"{market_ids[0]}..{market_ids[-1]}"
        result = await self._get(
            f"{GAMMA_BASE_URL}/markets", params, "market", label
        )

        if isinstance(result, FetchError):
            return [], [
                FetchError(
                    "market", mid, f"{GAMMA_BASE_URL}/markets",
                    result.error_type, result.error_message, result.retry_count,
                )
                for mid in market_ids
            ]

        markets = result if isinstance(result, list) else []
        return markets, []

    # ── Main entry point ──────────────────────────────────────────────────────

    async def fetch_all(
        self,
        on_events_chunk=None,
        on_markets_chunk=None,
    ) -> FetchResult:
        """
        Fetch all events, markets, and series — writing to Postgres per chunk.

        Streaming design (pipelined Pass 1 + Pass 2 per chunk):
            Each event chunk coroutine immediately fires market API calls for
            the markets it found — no global accumulation in memory. Pass 1
            and Pass 2 are pipelined per event chunk rather than run as two
            sequential phases. This keeps memory flat (one chunk at a time)
            and starts writing markets earlier.

            If Pass 2 returns a market → write with real volume24hr.
            If Pass 2 misses a market → write with Pass 1 data, volume24hr=0.
            bronze.raw_markets uses ON CONFLICT DO UPDATE so a later chunk with
            better data (e.g. real volume24hr) overwrites an earlier partial row.

        Shared state safety:
            asyncio is single-threaded cooperative multitasking. Shared sets
            modified synchronously (no await between check and update) are
            race-free — only one coroutine runs at a time between await points.

        Args:
            on_events_chunk:  async callback(events, series, errors) — fired when
                              each event chunk arrives from the API.
            on_markets_chunk: async callback(markets, errors) — fired after markets
                              for that event chunk are fetched and merged.

        Returns:
            FetchResult with counts and errors only (no full record lists).
        """
        event_ids = _load_event_ids(EVENT_IDS_PATH)
        result = FetchResult()

        chunks = _chunk(event_ids, CHUNK_SIZE)
        logger.info(
            f"Fetching {len(event_ids)} events in {len(chunks)} chunks of {CHUNK_SIZE} "
            f"| concurrency={MAX_CONCURRENT_REQUESTS} | Pass 1+2 pipelined per chunk"
        )

        # Shared state — safe in asyncio cooperative multitasking.
        returned_event_ids: set[str] = set()
        seen_series_ids: set[str]    = set()  # cross-chunk series deduplication
        fallback_count: list[int]    = [0]    # markets written with Pass 1 fallback data

        async def _process_chunk(chunk: list[str]):
            """
            Fetch one event chunk then immediately fetch its markets.

            Events and series are written first (on_events_chunk), then markets
            are fetched from /markets for this chunk's market IDs and written
            (on_markets_chunk). No data held in memory beyond this chunk.
            """
            # ── Pass 1: fetch events ──────────────────────────────────────────
            events, event_errors = await self._fetch_event_chunk(chunk)

            chunk_market_base: dict[str, dict] = {}  # local to this chunk
            chunk_series: list[dict] = []

            for event in events:
                returned_event_ids.add(str(event["id"]))
                for market in event.get("markets", []):
                    market["_event_id"] = str(event["id"])
                    chunk_market_base[str(market["id"])] = market
                for s in event.get("series", []):
                    sid = str(s["id"])
                    if sid not in seen_series_ids:
                        seen_series_ids.add(sid)
                        chunk_series.append(s)

            if on_events_chunk:
                await on_events_chunk(events, chunk_series, event_errors)

            # ── Pass 2: fetch volume24hr for this chunk's markets ─────────────
            # Fire /markets calls immediately for the markets we just found,
            # rather than waiting for all 500 events to load first.
            chunk_market_ids = list(chunk_market_base.keys())
            market_sub_chunks = _chunk(chunk_market_ids, CHUNK_SIZE)

            all_market_errors: list[FetchError] = []
            merged_markets: list[dict] = []

            for sub_chunk in market_sub_chunks:
                pass2_markets, pass2_errors = await self._fetch_market_chunk(sub_chunk)
                all_market_errors.extend(pass2_errors)

                returned_in_sub: set[str] = set()
                for m in pass2_markets:
                    mid = str(m["id"])
                    returned_in_sub.add(mid)
                    if mid in chunk_market_base:
                        # Enrich the Pass 1 record with Pass 2 fields
                        chunk_market_base[mid]["volume24hr"]   = m.get("volume24hr", 0) or 0
                        chunk_market_base[mid]["volumeNum"]    = m.get("volumeNum")    or chunk_market_base[mid].get("volumeNum", 0)
                        chunk_market_base[mid]["liquidityNum"] = m.get("liquidityNum") or chunk_market_base[mid].get("liquidityNum", 0)
                        merged_markets.append(chunk_market_base[mid])

                # Markets not returned by Pass 2 — use Pass 1 data with volume24hr=0.
                # ON CONFLICT DO UPDATE in bronze means a future chunk with better data
                # will overwrite this row rather than silently skip it.
                for mid in sub_chunk:
                    if mid not in returned_in_sub:
                        chunk_market_base[mid].setdefault("volume24hr", 0)
                        merged_markets.append(chunk_market_base[mid])
                        fallback_count[0] += 1

            if on_markets_chunk:
                await on_markets_chunk(merged_markets, all_market_errors)

            return len(events), len(chunk_series), len(merged_markets), event_errors + all_market_errors

        tasks = [_process_chunk(c) for c in chunks]
        chunk_results = await asyncio.gather(*tasks)

        for event_count, series_count, market_count, errors in chunk_results:
            result.events_count  += event_count
            result.series_count  += series_count
            result.markets_count += market_count
            result.errors.extend(errors)

        # Cross-check: events requested but not in any API response.
        for eid in event_ids:
            if eid not in returned_event_ids:
                logger.warning(f"Event silently missing from API response | event_id={eid}")
                result.errors.append(FetchError(
                    entity_type="event",
                    entity_id=eid,
                    endpoint=f"{GAMMA_BASE_URL}/events",
                    error_type="not_returned",
                    error_message="Event ID was requested but not present in any API response chunk",
                    retry_count=0,
                ))

        if fallback_count[0] > 0:
            logger.warning(
                f"{fallback_count[0]} markets not returned by /markets — "
                f"written with Pass 1 data (volume24hr=0). "
                f"Likely negRisk sub-markets or archived markets."
            )

        failed_events  = len([e for e in result.errors if e.entity_type == "event"])
        failed_markets = len([e for e in result.errors if e.entity_type == "market"])
        logger.info(
            f"Fetch complete | events={result.events_count} ok/{failed_events} failed | "
            f"markets={result.markets_count} ok/{failed_markets} failed | "
            f"series={result.series_count} unique"
        )

        return result
