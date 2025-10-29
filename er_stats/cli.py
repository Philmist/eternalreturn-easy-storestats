"""Command line interface for the Eternal Return statistics toolkit."""

from __future__ import annotations

import argparse
import json
import logging
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
from .db import SQLiteStore
from .ingest import IngestionManager


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Eternal Return API data into SQLite and query aggregates.",
    )
    parser.add_argument("--db", type=Path, required=True, help="SQLite database path")

    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Ingest matches starting from user seeds")
    ingest_parser.add_argument("--base-url", required=True, help="Eternal Return API base URL")
    ingest_parser.add_argument("--api-key", help="API key for authentication")
    ingest_parser.add_argument(
        "--user",
        dest="users",
        type=int,
        action="append",
        required=True,
        help="Seed user number. Specify multiple times for several seeds.",
    )
    ingest_parser.add_argument("--depth", type=int, default=1, help="Recursive depth for user discovery")
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

    def add_context_args(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--season", type=int, required=True, help="Season ID filter")
        subparser.add_argument("--server", required=True, help="Server name filter")
        subparser.add_argument("--mode", type=int, required=True, help="Matching mode filter")
        subparser.add_argument(
            "--team-mode",
            type=int,
            required=True,
            help="Matching team mode filter",
        )

    char_parser = subparsers.add_parser("character", help="Character average rank and distribution")
    add_context_args(char_parser)

    equip_parser = subparsers.add_parser("equipment", help="Equipment performance statistics")
    add_context_args(equip_parser)
    equip_parser.add_argument(
        "--min-samples",
        type=int,
        default=5,
        help="Minimum number of matches required to show equipment stats",
    )

    bot_parser = subparsers.add_parser("bot", help="Bot usage and performance statistics")
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
    store = SQLiteStore(str(args.db))
    try:
        store.setup_schema()
        if args.command == "ingest":
            logging.basicConfig(level=logging.INFO, format="%(message)s")
            logger = logging.getLogger("er_stats.ingest")
            client = EternalReturnAPIClient(
                args.base_url,
                api_key=args.api_key,
                min_interval=args.min_interval,
                max_retries=args.max_retries,
            )
            def report(message: str) -> None:
                logger.info(message)

            manager = IngestionManager(
                client,
                store,
                max_games_per_user=args.max_games,
                progress_callback=report,
            )
            try:
                manager.ingest_from_seeds(args.users, depth=args.depth)
            finally:
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
