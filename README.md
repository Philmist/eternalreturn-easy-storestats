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

For development and running tests (installs pytest via extras):

```bash
pip install -e .[test]
# Then run tests
python -m pytest -q
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
    # Crawl matches for these nicknames and discover opponents (depth=1)
    manager.ingest_from_seeds(["Philmist", "AnotherPlayer"], depth=1)
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
  --nickname Philmist --depth 1 --max-games 50 \
  --min-interval 1.0 --max-retries 3
```

For recurring jobs with mostly fixed settings, you can use a TOML
configuration file instead of repeating all options on the command line.

1. Copy `ingest.sample.toml` to a new file (for example `ingest.main.toml`) and edit values as needed.
2. Keep API keys out of the file and provide them via environment variables as indicated in the `[auth]` section.
3. Run ingest pointing to that config:

```bash
python -m er_stats.cli ingest --config ingest.main.toml
```

Write Parquet datasets during ingest (for DuckDB/analytics):

```bash
python -m er_stats.cli --db er.sqlite ingest \
  --base-url https://open-api.bser.io \
  --api-key $ER_DEV_APIKEY \
  --nickname Philmist --depth 1 \
  --parquet-dir data/parquet
```

> [!NOTE]
> The API now returns a new `uid` (`userId`) on every nickname lookup. Stored nickname→uid mappings are reused unless `/v1/user/games/uid/{uid}` returns 404, in which case the nickname is resolved again. Databases created with older `userNum`-based versions must be recreated for this release.
> Stale UIDs may be lazily rechecked with `/v1/user/games/uid/{uid}`; a 404 triggers nickname re-resolution, while other responses keep the cached uid.
> The developer API may return 403 or 429 when rate limited; the client applies backoff/retries for these, but persistent 403 will abort the current ingest attempt.

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

Parquet file counts and compaction
- Ingest batches rows per partition to reduce small files. You can tune batching by adjusting `flush_rows` in `ParquetExporter` (code) if needed.
- To compact and compress an existing dataset (e.g., many small files) into ZSTD-compressed Parquet with larger row groups:

```bash
# Using the separate tools CLI entry
er-stats-tools parquet-compact \
  --src data/parquet/participants \
  --dst data/parquet_compacted/participants \
  --compression zstd \
  --max-rows-per-file 250000

# Or via module path
python -m er_stats.tools_cli parquet-compact \
  --src data/parquet/participants \
  --dst data/parquet_compacted/participants \
  --compression zstd \
  --max-rows-per-file 250000
```

Repeat for `matches` if desired, or point `--src` to the root (e.g., `data/parquet`) to rewrite all partitions.

### Ingest by nickname

Ingestion seeds now require public nicknames:

```bash
python -m er_stats.cli --db er.sqlite ingest \
  --base-url https://open-api.bser.io \
  --api-key $ER_DEV_APIKEY \
  --nickname Philmist --nickname AnotherPlayer \
  --depth 1 --max-games 10
```
The CLI resolves each `--nickname` via `GET /v1/user/nickname?query=...` when needed. UID seeds are no longer supported.

Run aggregations (outputs JSON to stdout):

```bash
# Character rankings
python -m er_stats.cli --db er.sqlite stats character \
  --season 25 --server NA --mode 3 --team-mode 1

# Limit to a time window (ISO-8601 with timezone or relative presets like last:7d)
python -m er_stats.cli --db er.sqlite stats character \
  --season 25 --server NA --mode 3 --team-mode 1 \
  --range last:7d

# Equipment performance
python -m er_stats.cli --db er.sqlite stats equipment \
  --season 25 --server NA --mode 3 --team-mode 1 --min-samples 5

# Bot usage stats
python -m er_stats.cli --db er.sqlite stats bot \
  --season 25 --server NA --mode 3 --team-mode 1 --min-matches 3

# MMR change stats
python -m er_stats.cli --db er.sqlite stats mmr \
  --season 25 --server NA --mode 3 --team-mode 1

# Filter by a specific patch (season + version_major) or the latest patch in the DB
python -m er_stats.cli --db er.sqlite stats character \
  --server NA --mode 3 --team-mode 1 --patch latest

# Team compositions (win/top rates; defaults to all servers when --server omitted)
python -m er_stats.cli --db er.sqlite stats team \
  --mode 3 --team-mode 3 --season 25 \
  --top-n 3 --min-matches 5
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

## Ingestion Performance and Worst Case

> WARNING: In the worst case, recursive user discovery can explode and take a very long time.

- Breadth-first user discovery expands from each processed user to all participants in their games. If those participants are all new and non-overlapping, the number of users explored grows exponentially with `--depth`.
- Per user, the workflow performs roughly:
  - 1 request to list recent games (assuming up to 10 games fit in one page)
  - Up to 10 requests to fetch per-game participants (one per game)
  - ≈ 11 requests/user at `--max-games 10` (more if paging or if `--max-games` is larger)
- With branching factor `b ≈ (games_per_user) × (participants_per_game − 1)`, total processed users can approach `Σ b^i` up to the specified depth. For example, with 10 games/user, 18 participants/game, and `--depth 3`, the theoretical upper bound exceeds 4.9M users and ~54M requests — not realistic in practice due to deduplication, but it illustrates the potential growth.

Recommendations
- Start conservatively: `--depth 0..1`, `--max-games 3..5`, then scale up gradually.
- Respect rate limits: tune `--min-interval` and `--max-retries` for your environment.
- Run in batches and checkpoint: it’s safer to ingest incrementally and monitor growth.
- Prefer Parquet for analysis: write Parquet during ingest (`--parquet-dir`) to avoid repeated DB reads later; adjust batching (`flush_rows`) to reduce small files.
