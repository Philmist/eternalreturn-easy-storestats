"""Data ingestion workflow for Eternal Return API payloads."""

from __future__ import annotations

import datetime as dt
import logging
from collections import deque
from typing import Callable, Iterable, Optional, Set

from .api_client import EternalReturnAPIClient
from .db import SQLiteStore, parse_start_time

try:
    # Optional Parquet export; available when pyarrow is installed
    from .parquet_export import ParquetExporter
except Exception:  # pragma: no cover - optional dependency
    ParquetExporter = None  # type: ignore


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class IngestionManager:
    """Coordinate recursive ingestion of match data."""

    def __init__(
        self,
        client: EternalReturnAPIClient,
        store: SQLiteStore,
        *,
        max_games_per_user: Optional[int] = None,
        fetch_game_details: bool = True,
        progress_callback: Optional[Callable[[str], None]] = None,
        parquet_exporter: Optional["ParquetExporter"] = None,
        only_newer_games: bool = False,
        prefer_nickname_fetch: bool = False,
    ) -> None:
        self.client = client
        self.store = store
        self.max_games_per_user = max_games_per_user
        self.fetch_game_details = fetch_game_details
        self._seen_games: Set[int] = set()
        self._progress_callback = progress_callback
        self._parquet = parquet_exporter
        self.only_newer_games = only_newer_games
        self.prefer_nickname_fetch = prefer_nickname_fetch

    def _report(self, message: str) -> None:
        if self._progress_callback:
            self._progress_callback(message)
        else:
            logger.info(message)

    def _extract_uid(self, nickname: str) -> Optional[str]:
        game_uid: Optional[str] = (
            None
            if self.prefer_nickname_fetch
            else self.store.get_uid_from_nickname(nickname)
        )
        if not isinstance(game_uid, str):
            uid_response = self.client.fetch_user_by_nickname(nickname)
            game_uid = uid_response.get("user", {}).get("userId", None)
        return game_uid

    def ingest_user(self, uid: str) -> Set[str]:
        """Ingest matches for a single user.

        Returns a set of newly discovered UID from the processed games.
        """

        uid = str(uid)
        discovered: Set[str] = set()
        next_token: Optional[str] = None
        processed = 0

        self._report(f"Fetching games for uid {uid}")

        cutoff: Optional[dt.datetime] = None
        if self.only_newer_games:
            last_seen = self.store.get_user_last_seen(uid)
            if last_seen:
                try:
                    cutoff = dt.datetime.fromisoformat(last_seen)
                except ValueError:
                    cutoff = None

        stop_due_to_cutoff = False
        while True:
            payload = self.client.fetch_user_games(uid, next_token)
            games = payload.get("userGames", [])
            for game in games:
                if cutoff:
                    start_iso = parse_start_time(game.get("startDtm"))
                    if start_iso:
                        try:
                            start_dt = dt.datetime.fromisoformat(start_iso)
                        except ValueError:
                            start_dt = None
                        else:
                            if start_dt <= cutoff:
                                stop_due_to_cutoff = True
                                self._report(
                                    "Encountered previously ingested game "
                                    f"{game.get('gameId')} for uid {uid}; stopping early"
                                )
                                break
                game_id = game.get("gameId")
                game_already_known = bool(game_id and self.store.has_game(game_id))
                game_uid = self._extract_uid(game.get("nickname", ""))
                if game_uid is None:
                    continue
                game["uid"] = game_uid
                self.store.upsert_from_game_payload(game)
                if self._parquet is not None:
                    self._parquet.write_from_game_payload(game)
                processed += 1
                self._report(f"Processed game {processed} for uid {uid}")
                if self.fetch_game_details:
                    discovered.update(
                        self._ingest_game_participants(
                            game_id, already_known=game_already_known
                        )
                    )
                if self.max_games_per_user and processed >= self.max_games_per_user:
                    break
            if stop_due_to_cutoff:
                break
            if self.max_games_per_user and processed >= self.max_games_per_user:
                break
            next_token = payload.get("next")
            if not next_token:
                break
        return discovered

    def ingest_from_seeds(self, seeds: Iterable[str], *, depth: int = 1) -> None:
        """Recursively ingest matches starting from the provided seed users."""

        queue = deque((seed, 0) for seed in seeds)
        seen_users: Set[str] = set()
        while queue:
            self._report(f"Ingest queue left: {len(queue)} users")
            uid, current_depth = queue.popleft()
            if uid in seen_users:
                continue
            seen_users.add(uid)
            self._report(f"Ingesting user {uid} at depth {current_depth}")
            new_users = self.ingest_user(uid)
            self._report(f"Discovered {len(new_users)} new users from user {uid}")
            if current_depth + 1 > depth:
                continue
            for next_user in new_users:
                if next_user not in seen_users:
                    queue.append((next_user, current_depth + 1))

    def _ingest_game_participants(
        self, game_id: Optional[int], *, already_known: bool = False
    ) -> Set[str]:
        if not game_id or game_id in self._seen_games:
            return set()
        self._seen_games.add(game_id)
        if already_known:
            cached_participants = self.store.get_participants_for_game(game_id)
            if cached_participants and len(cached_participants) > 1:
                self._report(
                    f"Skipping API fetch for known game {game_id}; "
                    f"loaded {len(cached_participants)} participants from cache"
                )
                return cached_participants
        payload = self.client.fetch_game_result(game_id)
        participants = payload.get("userGames", [])
        discovered: Set[str] = set()
        for participant in participants:
            uid = self._extract_uid(participant.get("nickname", ""))
            participant["uid"] = uid
            self.store.upsert_from_game_payload(participant)
            if self._parquet is not None:
                self._parquet.write_from_game_payload(participant)
            if uid is not None:
                discovered.add(uid)
        self._report(f"Fetched {len(participants)} participants for game {game_id}")
        return discovered


__all__ = ["IngestionManager"]
