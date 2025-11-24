"""Command line interface for the Eternal Return statistics toolkit."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Iterable, Optional

from .aggregations import (
    bot_usage_statistics,
    character_rankings,
    equipment_rankings,
    mmr_change_statistics,
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
        "--user",
        dest="users",
        type=int,
        action="append",
        required=False,
        help="Seed user number. Specify multiple times for several seeds.",
    )
    ingest_parser.add_argument(
        "--nickname",
        dest="nicknames",
        type=str,
        action="append",
        required=False,
        help="Public nickname to resolve to userNum. Repeatable.",
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
        subparser.add_argument("--server", required=True, help="Server name filter")
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

    char_parser = subparsers.add_parser(
        "character", help="Character average rank and distribution"
    )
    add_context_args(char_parser)

    equip_parser = subparsers.add_parser(
        "equipment", help="Equipment performance statistics"
    )
    add_context_args(equip_parser)
    equip_parser.add_argument(
        "--min-samples",
        type=int,
        default=5,
        help="Minimum number of matches required to show equipment stats",
    )

    bot_parser = subparsers.add_parser(
        "bot", help="Bot usage and performance statistics"
    )
    add_context_args(bot_parser)
    bot_parser.add_argument(
        "--min-matches",
        type=int,
        default=3,
        help="Minimum number of bot matches per character to include",
    )

    mmr_parser = subparsers.add_parser("mmr", help="Character MMR gain statistics")
    add_context_args(mmr_parser)

    return parser.parse_args(argv)


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

    db_path: Optional[Path] = None
    ingest_config = None
    if args.command == "ingest":
        if args.config is not None:
            try:
                ingest_config = load_ingest_config(args.config)
                logger.info("Load config from '%s'", args.config)
            except ConfigError as exc:
                logger.error("%s", exc)
                return 2
        if args.db is not None:
            db_path = args.db
        elif ingest_config is not None:
            ingest_table = ingest_config.get("ingest", {})
            db_value = ingest_table.get("db_path")
            if isinstance(db_value, str):
                db_path = Path(db_value)
        if db_path is None:
            logger.error(
                "Database path must be provided via --db or ingest.db_path in the config file."
            )
            return 2
    else:
        if args.db is None:
            logger.error("--db is required for this command.")
            return 2
        db_path = args.db

    store = SQLiteStore(str(db_path))
    try:
        store.setup_schema()
        if args.command == "ingest":
            ingest_table = (
                ingest_config.get("ingest", {}) if ingest_config is not None else {}
            )
            seeds_cfg = (
                ingest_config.get("seeds", {}) if ingest_config is not None else {}
            )
            auth_cfg = (
                ingest_config.get("auth", {}) if ingest_config is not None else {}
            )
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
            if parquet_dir_value is None and isinstance(
                ingest_table.get("parquet_dir"), str
            ):
                parquet_dir_value = Path(ingest_table["parquet_dir"])
            if parquet_dir_value is not None:
                try:
                    from .parquet_export import ParquetExporter

                    parquet_exporter = ParquetExporter(parquet_dir_value)
                except Exception as e:
                    ingest_logger.warning("Parquet export disabled: %s", e)
            # Build seed user list from --user and --nickname
            seed_users = list(seeds_cfg.get("users", []))
            if args.users:
                seed_users.extend(args.users)
            nickname_sources = list(seeds_cfg.get("nicknames", []))
            if args.nicknames:
                nickname_sources.extend(args.nicknames)
            if nickname_sources:
                ingest_logger.info("Try to resolve nickname(s).")
                for nick in nickname_sources:
                    try:
                        payload = client.fetch_user_by_nickname(nick)
                        user = payload.get("user") or {}
                        user_num = user.get("userNum")
                        if not isinstance(user_num, int):
                            raise ValueError(
                                f"Nickname '{nick}' did not resolve to a valid userNum"
                            )
                        seed_users.append(user_num)
                    except Exception as e:
                        ingest_logger.error(
                            "Failed to resolve nickname '%s': %s", nick, e
                        )
                        return 2
            if not seed_users:
                ingest_logger.error(
                    "No seeds provided. Specify at least --user or --nickname."
                )
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
                manager.ingest_from_seeds(seed_users, depth=depth)
            finally:
                if parquet_exporter is not None:
                    try:
                        parquet_exporter.close()
                    except Exception:
                        pass
                client.close()
            return 0

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

        if args.season is not None:
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
        }
        if args.command == "character":
            rows = character_rankings(store, **context)
        elif args.command == "equipment":
            rows = equipment_rankings(store, min_samples=args.min_samples, **context)
        elif args.command == "bot":
            rows = bot_usage_statistics(store, min_matches=args.min_matches, **context)
        elif args.command == "mmr":
            rows = mmr_change_statistics(store, **context)
        else:
            raise ValueError(f"Unsupported command: {args.command}")
        json.dump(rows, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    finally:
        store.close()


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
