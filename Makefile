.PHONY: build run rebuild reset logs psql q1 q2 q3 help

# ── Setup ─────────────────────────────────────────────────────────────────

## Build the ETL image and run the full pipeline (first-time setup)
build:
	cp -n .env.example .env 2>/dev/null || true
	docker compose up --build

# ── Pipeline runs ─────────────────────────────────────────────────────────

## Manually trigger one pipeline run (Postgres must already be running)
run:
	docker compose run --rm etl

## Rebuild ETL image (after code changes) then run pipeline
rebuild:
	docker compose build etl
	docker compose run --rm etl

## Start Postgres in the background (detached), keep it running
postgres:
	docker compose up -d postgres

# ── Debugging ─────────────────────────────────────────────────────────────

## Tail logs from both services
logs:
	docker compose logs -f

## Tail ETL logs only
logs-etl:
	docker compose logs -f etl

## Open a psql shell against the running Postgres container
psql:
	docker compose exec postgres psql -U $${POSTGRES_USER:-polymarket} -d $${POSTGRES_DB:-polymarket}

# ── Business queries ──────────────────────────────────────────────────────

## Q1: Top 25 markets by 24-hour volume
q1:
	docker compose exec -T postgres psql -U $${POSTGRES_USER:-polymarket} -d $${POSTGRES_DB:-polymarket} -f /dev/stdin < sql/queries/q1_top_markets_by_volume24hr.sql

## Q2: Top 20 events by volume + top 20 by market count
q2:
	docker compose exec -T postgres psql -U $${POSTGRES_USER:-polymarket} -d $${POSTGRES_DB:-polymarket} -f /dev/stdin < sql/queries/q2_top_events.sql

## Q3: Top 20 most concentrated events
q3:
	docker compose exec -T postgres psql -U $${POSTGRES_USER:-polymarket} -d $${POSTGRES_DB:-polymarket} -f /dev/stdin < sql/queries/q3_most_concentrated_events.sql

# ── Reset ─────────────────────────────────────────────────────────────────

## Stop all containers and wipe the Postgres volume (full clean slate)
reset:
	docker compose down -v
	@echo "All containers stopped and data volume wiped."

## Stop containers but keep the data volume
stop:
	docker compose down

# ── Help ──────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  make build      — first-time setup: build image + run pipeline"
	@echo "  make run        — manually trigger one pipeline run"
	@echo "  make rebuild    — rebuild ETL image then run pipeline"
	@echo "  make postgres   — start Postgres in background"
	@echo "  make logs       — tail logs from all services"
	@echo "  make logs-etl   — tail ETL logs only"
	@echo "  make psql       — open psql shell"
	@echo "  make reset      — wipe everything (containers + data volume)"
	@echo "  make stop       — stop containers, keep data"
	@echo ""
	@echo "  make q1         — top 25 markets by volume24hr"
	@echo "  make q2         — top 20 events by volume + by market count"
	@echo "  make q3         — top 20 most concentrated events"
	@echo ""
