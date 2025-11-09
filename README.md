# Eternal Return Easy Store Stats (`er_stats`)

Utilities to ingest Eternal Return Open API data into SQLite and run lightweight analytics. Ships as a small Python package with a simple CLI.

## Package Overview

- `er_stats/api_client.py` — minimal HTTP client for the Eternal Return API.
- `er_stats/db.py` — SQLite schema and upsert helpers for matches, users, and stats.
- `er_stats/ingest.py` — orchestrates ingestion from seed users and game participants.
- `er_stats/aggregations.py` — query helpers for rankings and summaries.
- `er_stats/cli.py` — command line interface wrapping ingestion and queries.
- `er_stats/__init__.py` — convenient exports for library usage.

## Installation

Requires Python 3.9+.

```bash
python -m venv .venv
. .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

## Testing

Use the helper scripts to ensure the package is installed in editable mode
with its test dependencies before running the test suite:

```bash
./scripts/run_tests.sh
```

On Windows (PowerShell):

```powershell
./scripts/run_tests.ps1
```

From a non-PowerShell shell you can invoke PowerShell manually:

```bash
pwsh -File scripts/run_tests.ps1
```

> [!TIP]
> For projects that need to exercise multiple environments or test matrices,
> adopting a dedicated runner such as `tox`, `nox`, or GitHub Actions can offer
> a more scalable alternative to these helper scripts while still ensuring
> dependencies are installed before the tests execute.

The scripts accept additional arguments that are forwarded to `pytest`, so you
can run a subset of tests when needed, for example:

```bash
./scripts/run_tests.sh tests/test_cli.py -k ingest
```

```powershell
pwsh ./scripts/run_tests.ps1 tests/test_cli.py -k ingest
```

## Library Usage

Basic ingestion from user seeds, storing into SQLite:

```python
from er_stats import EternalReturnAPIClient, SQLiteStore, IngestionManager

BASE_URL = "https://open-api.bser.io"
API_KEY = "<your-api-key>"

store = SQLiteStore("er_stats.sqlite")
store.setup_schema()

client = EternalReturnAPIClient(BASE_URL, api_key=API_KEY)
manager = IngestionManager(client, store, max_games_per_user=50)

try:
    # Crawl matches for these users and discover opponents (depth=1)
    manager.ingest_from_seeds([1733900], depth=1)
finally:
    client.close()
    store.close()
```

Query analytics after ingestion:

```python
from er_stats.aggregations import (
    character_rankings,
    equipment_rankings,
    bot_usage_statistics,
    mmr_change_statistics,
)
from er_stats import SQLiteStore

store = SQLiteStore("er_stats.sqlite")

context = {
    "season_id": 25,
    "server_name": "NA",
    "matching_mode": 3,
    "matching_team_mode": 1,
}

try:
    chars = character_rankings(store, **context)
    equips = equipment_rankings(store, min_samples=5, **context)
    bots = bot_usage_statistics(store, min_matches=3, **context)
    mmr = mmr_change_statistics(store, **context)
    print(chars[:3])
finally:
    store.close()
```

## CLI Usage

Ingest data into a SQLite DB:

```bash
python -m er_stats.cli --db er.sqlite ingest \
  --base-url https://open-api.bser.io \
  --api-key $ER_DEV_APIKEY \
  --user 1733900 --depth 1 --max-games 50 \
  --min-interval 1.0 --max-retries 3
```

Write Parquet datasets during ingest (for DuckDB/analytics):

```bash
python -m er_stats.cli --db er.sqlite ingest \
  --base-url https://open-api.bser.io \
  --api-key $ER_DEV_APIKEY \
  --user 1733900 --depth 1 \
  --parquet-dir data/parquet
```

This creates partitioned datasets under `data/parquet/`:
- `matches/season_id=..../server_name=.../matching_mode=.../date=YYYY-MM-DD/*.parquet`
- `participants/season_id=..../server_name=.../matching_mode=.../date=YYYY-MM-DD/*.parquet`

You can query them directly with DuckDB, for example:

```sql
SELECT character_num, AVG(game_rank) AS avg_rank
FROM 'data/parquet/participants/**/*.parquet'
WHERE season_id=35 AND server_name='NA' AND matching_mode=3 AND matching_team_mode=1
GROUP BY character_num
ORDER BY avg_rank;
```

Run aggregations (outputs JSON to stdout):

```bash
# Character rankings
python -m er_stats.cli --db er.sqlite character \
  --season 25 --server NA --mode 3 --team-mode 1

# Equipment performance
python -m er_stats.cli --db er.sqlite equipment \
  --season 25 --server NA --mode 3 --team-mode 1 --min-samples 5

# Bot usage stats
python -m er_stats.cli --db er.sqlite bot \
  --season 25 --server NA --mode 3 --team-mode 1 --min-matches 3

# MMR change stats
python -m er_stats.cli --db er.sqlite mmr \
  --season 25 --server NA --mode 3 --team-mode 1
```

## Data Model (SQLite)

- `users` — known players with last-seen info and flags.
- `matches` — one row per game with context and raw payload.
- `user_match_stats` — per-user per-game stats, plus raw payload.
- `equipment` — gear used, one row per slot.
- `mastery_levels` — mastery level per type.
- `skill_levels` — final level per skill code.
- `skill_orders` — skill acquisition order sequence.

## Notes

- API base URL is typically `https://open-api.bser.io` and requires an `x-api-key`.
- Timestamps are normalized to ISO-8601 where possible.
- The client enforces a default 1 request/second rate limit. You can override
  via `--min-interval` and control 429 retry attempts with `--max-retries`.
