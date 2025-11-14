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

    def add_context_args(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--season", type=int, required=True, help="Season ID filter"
        )
        subparser.add_argument("--server", required=True, help="Server name filter")
        subparser.add_argument(
            "--mode", type=int, required=True, help="Matching mode filter"
        )
        subparser.add_argument(
            "--team-mode",
            type=int,
            default=3,
            help="Matching team mode filter",
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
        help="Minimum number of matches per user to include",
    )

    mmr_parser = subparsers.add_parser("mmr", help="Character MMR gain statistics")
    add_context_args(mmr_parser)

    return parser.parse_args(argv)


def run(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    db_path: Optional[Path] = None
    ingest_config = None
    if args.command == "ingest":
        if args.config is not None:
            try:
                ingest_config = load_ingest_config(args.config)
            except ConfigError as exc:
                logging.basicConfig(level=logging.ERROR, format="%(message)s")
                logger = logging.getLogger("er_stats.ingest")
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
            logging.basicConfig(level=logging.ERROR, format="%(message)s")
            logger = logging.getLogger("er_stats.ingest")
            logger.error(
                "Database path must be provided via --db or ingest.db_path in the config file."
            )
            return 2
    else:
        if args.db is None:
            logging.basicConfig(level=logging.ERROR, format="%(message)s")
            logger = logging.getLogger("er_stats")
            logger.error("--db is required for this command.")
            return 2
        db_path = args.db

    store = SQLiteStore(str(db_path))
    try:
        store.setup_schema()
        if args.command == "ingest":
            logging.basicConfig(level=logging.INFO, format="%(message)s")
            logger = logging.getLogger("er_stats.ingest")
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

            def report(message: str) -> None:
                logger.info(message)

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
                    logger.warning("Parquet export disabled: %s", e)
            # Build seed user list from --user and --nickname
            seed_users = list(seeds_cfg.get("users", []))
            if args.users:
                seed_users.extend(args.users)
            nickname_sources = list(seeds_cfg.get("nicknames", []))
            if args.nicknames:
                nickname_sources.extend(args.nicknames)
            if nickname_sources:
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
                        logger.error("Failed to resolve nickname '%s': %s", nick, e)
                        return 2
            if not seed_users:
                logger.error(
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

        context = {
            "season_id": args.season,
            "server_name": args.server,
            "matching_mode": args.mode,
            "matching_team_mode": args.team_mode,
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
