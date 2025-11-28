"""Command line interface for the Eternal Return statistics toolkit."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

from .aggregations import (
    bot_usage_statistics,
    character_rankings,
    equipment_rankings,
    mmr_change_statistics,
    team_composition_statistics,
)
from .api_client import EternalReturnAPIClient
from .config import ConfigError, load_ingest_config
from .db import SQLiteStore
from .ingest import IngestionManager


LOGGER_NAME = "er_stats"
LOG_FORMAT_DEFAULT = "%(message)s"
LOG_FORMAT_INGEST = "%(asctime)s: %(message)s"

logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(logging.INFO)
ingest_logger = logging.getLogger(f"{LOGGER_NAME}.ingest")
ingest_logger.setLevel(logging.DEBUG)

default_log_formatter = logging.Formatter(LOG_FORMAT_DEFAULT)
ingest_log_formatter = logging.Formatter(LOG_FORMAT_INGEST)

default_log_handler = logging.StreamHandler()
default_log_handler.setLevel(logging.WARNING)
ingest_log_handler = logging.StreamHandler()
ingest_log_handler.setLevel(logging.INFO)

default_log_handler.setFormatter(default_log_formatter)
ingest_log_handler.setFormatter(ingest_log_formatter)

logger.addHandler(default_log_handler)
ingest_logger.addHandler(ingest_log_handler)

MATCHING_MODE_TEAM_MODE_DEFAULTS = {
    2: 3,
    3: 3,
    6: 4,
    8: 3,
}

MATCHING_MODE_ALIASES = {
    "normal": 2,
    "ranked": 3,
    "cobalt": 6,
    "union": 8,
}


def parse_matching_mode(value: str) -> int:
    """Parse matching mode from an integer or named alias."""

    try:
        return int(value)
    except ValueError:
        mode = MATCHING_MODE_ALIASES.get(value.lower())
        if mode is None:
            valid = ", ".join(sorted(MATCHING_MODE_ALIASES))
            raise argparse.ArgumentTypeError(
                f"Invalid matching mode '{value}'. Use an integer code or one of: {valid}."
            )
        return mode


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = _build_parser()
    return parser.parse_args(argv)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest Eternal Return API data into SQLite and query aggregates.",
    )
    parser.add_argument("--db", type=Path, required=False, help="SQLite database path")

    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser(
        "ingest", help="Ingest matches starting from user seeds"
    )
    ingest_parser.add_argument(
        "--config",
        type=Path,
        help="TOML configuration file providing ingest defaults",
    )
    ingest_parser.add_argument(
        "--base-url",
        default="https://open-api.bser.io/",
        help="Eternal Return API base URL",
    )
    ingest_parser.add_argument("--api-key", help="API key for authentication")
    ingest_parser.add_argument(
        "--uid",
        dest="uids",
        type=str,
        action="append",
        required=False,
        help="Seed user UID (userId). Specify multiple times for several seeds.",
    )
    ingest_parser.add_argument(
        "--nickname",
        dest="nicknames",
        type=str,
        action="append",
        required=False,
        help="Public nickname to resolve to uid. Repeatable.",
    )
    ingest_parser.add_argument(
        "--depth", type=int, default=1, help="Recursive depth for user discovery"
    )
    ingest_parser.add_argument(
        "--max-games",
        type=int,
        default=None,
        help="Maximum number of games to fetch per user (omit for all)",
    )
    ingest_parser.add_argument(
        "--min-interval",
        type=float,
        default=1.0,
        help="Minimum seconds between API requests (rate limit)",
    )
    ingest_parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Max retries on HTTP 429 Too Many Requests",
    )
    ingest_parser.add_argument(
        "--only-newer-games",
        dest="only_newer_games",
        action="store_true",
        default=True,
        help=(
            "Stop paging once a previously ingested match is encountered "
            "for a user (default behaviour)."
        ),
    )
    ingest_parser.add_argument(
        "--include-older-games",
        dest="only_newer_games",
        action="store_false",
        help="Continue ingesting all pages even if older matches were already stored.",
    )
    ingest_parser.add_argument(
        "--parquet-dir",
        type=Path,
        default=None,
        help="Optional directory to write Parquet datasets (matches, participants)",
    )
    ingest_parser.add_argument(
        "--require-metadata-refresh",
        action="store_true",
        help=(
            "Fail ingest if character or item catalog refresh fails "
            "(default: continue with a warning)."
        ),
    )

    def add_context_args(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--season",
            type=int,
            required=False,
            help=(
                "Season ID filter. Defaults to the latest "
                "known season for ranked mode; 0 for other modes."
            ),
        )
        subparser.add_argument(
            "--server",
            required=False,
            help="Server name filter. Omit to include all servers.",
        )
        subparser.add_argument(
            "--mode",
            type=parse_matching_mode,
            required=True,
            help=(
                "Matching mode filter. Accepts an integer code "
                "or one of: normal, ranked, cobalt, union."
            ),
        )
        subparser.add_argument(
            "--team-mode",
            type=int,
            default=None,
            help=(
                "Matching team mode filter. When omitted, a default "
                "is inferred from the matching mode when possible."
            ),
        )
        subparser.add_argument(
            "--start-dtm",
            type=str,
            help="Start datetime (inclusive) in ISO-8601 with timezone, or date (UTC midnight)",
        )
        subparser.add_argument(
            "--end-dtm",
            type=str,
            help="End datetime (exclusive) in ISO-8601 with timezone, or date (UTC midnight)",
        )
        subparser.add_argument(
            "--range",
            dest="time_range",
            type=str,
            help=(
                "Relative time range such as last:3d, last:12h, today, yesterday, "
                "this-week, prev-week. Cannot be combined with --start-dtm/--end-dtm."
            ),
        )
        subparser.add_argument(
            "--patch",
            type=str,
            help=(
                "Patch filter: 'latest', '35.1', or 'season=35,major=1'. "
                "Overrides --season when provided."
            ),
        )

    stats_parser = subparsers.add_parser(
        "stats", help="Query aggregated player and equipment statistics"
    )
    stats_subparsers = stats_parser.add_subparsers(dest="stats_command", required=True)

    char_parser = stats_subparsers.add_parser(
        "character", help="Character average rank and distribution"
    )
    add_context_args(char_parser)

    equip_parser = stats_subparsers.add_parser(
        "equipment", help="Equipment performance statistics"
    )
    add_context_args(equip_parser)
    equip_parser.add_argument(
        "--min-samples",
        type=int,
        default=5,
        help="Minimum number of matches required to show equipment stats",
    )

    bot_parser = stats_subparsers.add_parser(
        "bot", help="Bot usage and performance statistics"
    )
    add_context_args(bot_parser)
    bot_parser.add_argument(
        "--min-matches",
        type=int,
        default=3,
        help="Minimum number of bot matches per character to include",
    )

    mmr_parser = stats_subparsers.add_parser(
        "mmr", help="Character MMR gain statistics"
    )
    add_context_args(mmr_parser)

    team_parser = stats_subparsers.add_parser(
        "team", help="Team composition performance statistics"
    )
    add_context_args(team_parser)
    team_parser.add_argument(
        "--top-n",
        type=int,
        default=3,
        help="Rank threshold for counting a top finish (default: 3)",
    )
    team_parser.add_argument(
        "--min-matches",
        type=int,
        default=5,
        help="Minimum matches required to include a composition",
    )
    team_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of compositions returned",
    )
    team_parser.add_argument(
        "--sort-by",
        choices=["win-rate", "top-rate", "avg-rank"],
        default="win-rate",
        help="Sort compositions by win-rate, top-rate, or avg-rank",
    )
    team_parser.add_argument(
        "--no-include-names",
        dest="include_names",
        action="store_false",
        default=True,
        help="Do not include character names in the output",
    )

    return parser


def _parse_datetime_or_date(value: str) -> dt.datetime:
    """Parse ISO-8601 datetime (requires tz) or date string as UTC midnight."""

    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        try:
            date_value = dt.date.fromisoformat(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"Invalid datetime/date format: {value}"
            ) from exc
        return dt.datetime.combine(date_value, dt.time(0, 0, tzinfo=dt.timezone.utc))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(
            f"Timezone offset is required for datetime '{value}'."
        )
    return parsed


def parse_time_window(
    start_dtm_str: Optional[str],
    end_dtm_str: Optional[str],
    range_spec: Optional[str],
    *,
    now: Optional[dt.datetime] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """Return (start_iso, end_iso) where values are ISO-8601 strings or None."""

    if (start_dtm_str or end_dtm_str) and range_spec:
        raise argparse.ArgumentTypeError(
            "Specify either --range or --start-dtm/--end-dtm, not both."
        )
    now_value = now or dt.datetime.now(dt.timezone.utc)
    start_dt: Optional[dt.datetime] = None
    end_dt: Optional[dt.datetime] = None

    if range_spec:
        spec = range_spec.strip().lower()
        if spec.startswith("last:"):
            body = spec.split("last:", 1)[1]
            if not body:
                raise argparse.ArgumentTypeError("last:N[d|h] requires a number.")
            unit = body[-1]
            try:
                magnitude = int(body[:-1])
            except ValueError as exc:
                raise argparse.ArgumentTypeError(
                    f"Invalid last:* magnitude: {body}"
                ) from exc
            if magnitude <= 0 or unit not in {"d", "h"}:
                raise argparse.ArgumentTypeError(
                    "Use last:N d/h with N > 0, e.g., last:3d or last:12h."
                )
            delta = (
                dt.timedelta(days=magnitude)
                if unit == "d"
                else dt.timedelta(hours=magnitude)
            )
            end_dt = now_value
            start_dt = end_dt - delta
        elif spec == "today":
            start_dt = dt.datetime.combine(
                now_value.date(), dt.time(0, 0), tzinfo=dt.timezone.utc
            )
            end_dt = start_dt + dt.timedelta(days=1)
        elif spec == "yesterday":
            end_dt = dt.datetime.combine(
                now_value.date(), dt.time(0, 0), tzinfo=dt.timezone.utc
            )
            start_dt = end_dt - dt.timedelta(days=1)
        elif spec == "this-week":
            weekday = now_value.weekday()  # Monday=0
            start_dt = dt.datetime.combine(
                now_value.date() - dt.timedelta(days=weekday),
                dt.time(0, 0),
                tzinfo=dt.timezone.utc,
            )
            end_dt = start_dt + dt.timedelta(days=7)
        elif spec == "prev-week":
            weekday = now_value.weekday()
            this_week_start = dt.datetime.combine(
                now_value.date() - dt.timedelta(days=weekday),
                dt.time(0, 0),
                tzinfo=dt.timezone.utc,
            )
            end_dt = this_week_start
            start_dt = end_dt - dt.timedelta(days=7)
        else:
            raise argparse.ArgumentTypeError(f"Unsupported --range value: {range_spec}")
    else:
        start_dt = _parse_datetime_or_date(start_dtm_str) if start_dtm_str else None
        end_dt = _parse_datetime_or_date(end_dtm_str) if end_dtm_str else None

    if start_dt and end_dt and start_dt >= end_dt:
        raise argparse.ArgumentTypeError(
            "--start-dtm must be earlier than --end-dtm (half-open interval)."
        )

    start_iso = start_dt.isoformat() if start_dt else None
    end_iso = end_dt.isoformat() if end_dt else None
    return start_iso, end_iso


@dataclass(frozen=True)
class PatchSpec:
    season_id: Optional[int]
    version_major: Optional[int]
    latest: bool = False


def parse_patch_spec(patch_value: Optional[str]) -> Optional[PatchSpec]:
    if patch_value is None:
        return None
    raw = patch_value.strip()
    if not raw:
        return None
    lowered = raw.lower()
    if lowered == "latest":
        return PatchSpec(season_id=None, version_major=None, latest=True)
    if "." in raw:
        parts = raw.split(".")
        if len(parts) != 2:
            raise argparse.ArgumentTypeError(f"Invalid patch format: {patch_value}")
        try:
            season_id = int(parts[0])
            version_major = int(parts[1])
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"Patch values must be integers: {patch_value}"
            ) from exc
        return PatchSpec(season_id=season_id, version_major=version_major)
    if "=" in raw:
        items = [item.strip() for item in raw.split(",") if item.strip()]
        values = {}
        for item in items:
            if "=" not in item:
                raise argparse.ArgumentTypeError(f"Invalid patch token: {item}")
            key, val = item.split("=", 1)
            try:
                values[key.strip()] = int(val.strip())
            except ValueError as exc:
                raise argparse.ArgumentTypeError(
                    f"Patch values must be integers: {item}"
                ) from exc
        if "season" not in values or "major" not in values:
            raise argparse.ArgumentTypeError(
                f"Patch requires season and major: {patch_value}"
            )
        return PatchSpec(season_id=values["season"], version_major=values["major"])
    raise argparse.ArgumentTypeError(f"Unsupported patch format: {patch_value}")


def resolve_patch_spec(
    spec: PatchSpec,
    store: SQLiteStore,
    *,
    server_name: Optional[str],
    matching_mode: int,
    matching_team_mode: int,
) -> Tuple[int, int]:
    params = {
        "matching_mode": matching_mode,
        "matching_team_mode": matching_team_mode,
    }
    if spec.latest:
        where_clauses = [
            "matching_mode = :matching_mode",
            "matching_team_mode = :matching_team_mode",
            "season_id IS NOT NULL",
            "version_major IS NOT NULL",
        ]
        if server_name is not None:
            where_clauses.append("server_name = :server_name")
            params["server_name"] = server_name
        where = " AND ".join(where_clauses)
        query = f"""
            SELECT season_id, version_major
            FROM matches
            WHERE {where}
            ORDER BY season_id DESC, version_major DESC
            LIMIT 1
        """
        cur = store.connection.execute(query, params)
        row = cur.fetchone()
        if row is None:
            raise argparse.ArgumentTypeError(
                "Cannot resolve latest patch because no matching games exist in the database."
            )
        return int(row["season_id"]), int(row["version_major"])
    if spec.season_id is None or spec.version_major is None:
        raise argparse.ArgumentTypeError("Patch specification is incomplete.")
    return spec.season_id, spec.version_major


def _load_ingest_config(
    args: argparse.Namespace,
) -> Tuple[Optional[dict], Optional[int]]:
    if args.command != "ingest" or args.config is None:
        return None, None
    try:
        ingest_config = load_ingest_config(args.config)
        logger.info("Load config from '%s'", args.config)
        return ingest_config, None
    except ConfigError as exc:
        logger.error("%s", exc)
        return None, 2


def _resolve_db_path(
    args: argparse.Namespace, ingest_config: Optional[dict]
) -> Optional[Path]:
    if args.command == "ingest":
        if args.db is not None:
            return args.db
        if ingest_config is not None:
            ingest_table = ingest_config.get("ingest", {})
            db_value = ingest_table.get("db_path")
            if isinstance(db_value, str):
                return Path(db_value)
        return None
    return args.db


def _run_ingest(
    args: argparse.Namespace, store: SQLiteStore, ingest_config: Optional[dict]
) -> int:
    ingest_table = ingest_config.get("ingest", {}) if ingest_config is not None else {}
    seeds_cfg = ingest_config.get("seeds", {}) if ingest_config is not None else {}
    auth_cfg = ingest_config.get("auth", {}) if ingest_config is not None else {}
    base_url = ingest_table.get("base_url", args.base_url)
    min_interval = ingest_table.get("min_interval", args.min_interval)
    max_retries = ingest_table.get("max_retries", args.max_retries)
    api_key = args.api_key
    if api_key is None:
        api_key_env_name = auth_cfg.get("api_key_env")
        if isinstance(api_key_env_name, str) and api_key_env_name:
            api_key = os.environ.get(api_key_env_name) or None
    client = EternalReturnAPIClient(
        base_url,
        api_key=api_key,
        min_interval=min_interval,
        max_retries=max_retries,
    )

    characters_ok = refresh_character_catalog(store, client)
    items_ok = refresh_item_catalog(store, client)
    if args.require_metadata_refresh and (not characters_ok or not items_ok):
        ingest_logger.error(
            "Metadata refresh failed (characters or items); "
            "aborting ingest due to --require-metadata-refresh."
        )
        return 2

    def report(message: str) -> None:
        ingest_logger.info(message)

    parquet_exporter = None
    parquet_dir_value = args.parquet_dir
    if parquet_dir_value is None and isinstance(ingest_table.get("parquet_dir"), str):
        parquet_dir_value = Path(ingest_table["parquet_dir"])
    if parquet_dir_value is not None:
        try:
            from .parquet_export import ParquetExporter

            parquet_exporter = ParquetExporter(parquet_dir_value)
        except Exception as e:
            ingest_logger.warning("Parquet export disabled: %s", e)
    seed_uids: list[str] = []
    seed_uids_raw = seeds_cfg.get("uids", [])
    if isinstance(seed_uids_raw, (list, tuple, set)):
        seed_uids.extend(str(v) for v in seed_uids_raw)
    elif seed_uids_raw:
        seed_uids.append(str(seed_uids_raw))
    # Backward compatibility: allow legacy "users" key but treat as strings
    seed_uids.extend(str(v) for v in seeds_cfg.get("users", []))
    if args.uids:
        seed_uids.extend(args.uids)
    nickname_sources = list(seeds_cfg.get("nicknames", []))
    if args.nicknames:
        nickname_sources.extend(args.nicknames)
    if nickname_sources:
        ingest_logger.info("Try to resolve nickname(s).")
        for nick in nickname_sources:
            try:
                payload = client.fetch_user_by_nickname(nick)
                user = payload.get("user") or {}
                uid_value = user.get("userId") or user.get("uid")
                if not isinstance(uid_value, str) or not uid_value:
                    raise ValueError(
                        f"Nickname '{nick}' did not resolve to a valid uid"
                    )
                seed_uids.append(uid_value)
            except Exception as e:
                ingest_logger.error("Failed to resolve nickname '%s': %s", nick, e)
                return 2
    if not seed_uids:
        ingest_logger.error("No seeds provided. Specify at least --uid or --nickname.")
        return 2
    depth = ingest_table.get("depth", args.depth)
    if args.max_games is not None:
        max_games_per_user = args.max_games
    else:
        max_games_per_user = ingest_table.get("max_games_per_user")
    config_only_newer = ingest_table.get("only_newer_games")
    if args.only_newer_games is not True:
        only_newer_games = args.only_newer_games
    elif isinstance(config_only_newer, bool):
        only_newer_games = config_only_newer
    else:
        only_newer_games = args.only_newer_games
    manager = IngestionManager(
        client,
        store,
        max_games_per_user=max_games_per_user,
        only_newer_games=only_newer_games,
        parquet_exporter=parquet_exporter,
        progress_callback=report,
    )
    try:
        manager.ingest_from_seeds(seed_uids, depth=depth)
    finally:
        if parquet_exporter is not None:
            try:
                parquet_exporter.close()
            except Exception:
                pass
        client.close()
    return 0


def _run_stats(args: argparse.Namespace, store: SQLiteStore) -> int:
    matching_mode = args.mode
    if args.team_mode is not None:
        matching_team_mode = args.team_mode
    else:
        matching_team_mode = MATCHING_MODE_TEAM_MODE_DEFAULTS.get(matching_mode)
        if matching_team_mode is None:
            logger.error(
                "Matching team mode could not be inferred for matching mode %s. "
                "Please specify --team-mode.",
                matching_mode,
            )
            return 2

    try:
        patch_spec = parse_patch_spec(getattr(args, "patch", None))
    except argparse.ArgumentTypeError as exc:
        logger.error("%s", exc)
        return 2

    try:
        start_dtm_from, start_dtm_to = parse_time_window(
            getattr(args, "start_dtm", None),
            getattr(args, "end_dtm", None),
            getattr(args, "time_range", None),
        )
    except argparse.ArgumentTypeError as exc:
        logger.error("%s", exc)
        return 2

    season_override: Optional[int] = None
    version_major: Optional[int] = None
    if patch_spec:
        if patch_spec.latest:
            try:
                season_override, version_major = resolve_patch_spec(
                    patch_spec,
                    store,
                    server_name=args.server,
                    matching_mode=matching_mode,
                    matching_team_mode=matching_team_mode,
                )
            except argparse.ArgumentTypeError as exc:
                logger.error("%s", exc)
                return 2
        else:
            season_override, version_major = (
                patch_spec.season_id,
                patch_spec.version_major,
            )
            if (
                args.season is not None
                and season_override is not None
                and args.season != season_override
            ):
                logger.error(
                    "Patch season %s conflicts with --season %s",
                    season_override,
                    args.season,
                )
                return 2

    if season_override is not None:
        season_id = season_override
    elif args.season is not None:
        season_id = args.season
    else:
        ranked_mode = MATCHING_MODE_ALIASES["ranked"]
        if matching_mode == ranked_mode:
            with store.cursor() as cur:
                cur.execute("SELECT MAX(season_id) AS max_season FROM matches")
                row = cur.fetchone()
            max_season = row["max_season"] if row is not None else None
            if max_season is None:
                logger.error(
                    "No matches found in the database; cannot infer default "
                    "season for ranked mode. Please specify --season."
                )
                return 2
            season_id = max_season
        else:
            season_id = 0

    context = {
        "season_id": season_id,
        "server_name": args.server,
        "matching_mode": matching_mode,
        "matching_team_mode": matching_team_mode,
        "start_dtm_from": start_dtm_from,
        "start_dtm_to": start_dtm_to,
        "version_major": version_major,
    }
    if args.stats_command == "character":
        rows = character_rankings(store, **context)
    elif args.stats_command == "equipment":
        rows = equipment_rankings(store, min_samples=args.min_samples, **context)
    elif args.stats_command == "bot":
        rows = bot_usage_statistics(store, min_matches=args.min_matches, **context)
    elif args.stats_command == "mmr":
        rows = mmr_change_statistics(store, **context)
    elif args.stats_command == "team":
        rows = team_composition_statistics(
            store,
            top_n=args.top_n,
            min_matches=args.min_matches,
            include_names=args.include_names,
            sort_by=args.sort_by,
            limit=args.limit,
            **context,
        )
    else:  # pragma: no cover - argparse enforces available commands
        raise ValueError(f"Unsupported command: {args.stats_command}")
    json.dump(rows, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


def refresh_character_catalog(
    store: SQLiteStore, client: EternalReturnAPIClient
) -> bool:
    """Fetch the official character list and store it in SQLite.

    Returns True on success, False when refresh failed.
    """

    try:
        payload = client.fetch_character_attributes()
    except Exception as exc:  # pragma: no cover - logging path
        ingest_logger.warning("Failed to refresh character catalog: %s", exc)
        return False

    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        ingest_logger.warning(
            "Character API response did not include a 'data' list; skipping refresh"
        )
        return False

    count = store.refresh_characters(data)
    ingest_logger.info("Stored %d character definitions", count)
    return True


def refresh_item_catalog(store: SQLiteStore, client: EternalReturnAPIClient) -> bool:
    """Fetch the official item catalogs and store them in SQLite.

    Returns True on success, False when refresh failed.
    """

    try:
        armor_payload = client.fetch_item_armor()
        weapon_payload = client.fetch_item_weapon()
    except Exception as exc:  # pragma: no cover - logging path
        ingest_logger.warning("Failed to refresh item catalog: %s", exc)
        return False

    armor_data = armor_payload.get("data") if isinstance(armor_payload, dict) else None
    weapon_data = (
        weapon_payload.get("data") if isinstance(weapon_payload, dict) else None
    )
    if not isinstance(armor_data, list) or not isinstance(weapon_data, list):
        ingest_logger.warning(
            "Item API response did not include 'data' lists; skipping refresh"
        )
        return False

    combined = list(armor_data) + list(weapon_data)
    count = store.refresh_items(combined)
    ingest_logger.info("Stored %d item definitions", count)
    return True


def run(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    ingest_config, config_error = _load_ingest_config(args)
    if config_error is not None:
        return config_error

    db_path = _resolve_db_path(args, ingest_config)
    if db_path is None:
        if args.command == "ingest":
            logger.error(
                "Database path must be provided via --db or ingest.db_path in the config file."
            )
        else:
            logger.error("--db is required for this command.")
        return 2

    store = SQLiteStore(str(db_path))
    try:
        store.setup_schema()
        if args.command == "ingest":
            return _run_ingest(args, store, ingest_config)
        if args.command == "stats":
            return _run_stats(args, store)
        raise ValueError(f"Unsupported command: {args.command}")
    finally:
        store.close()


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
