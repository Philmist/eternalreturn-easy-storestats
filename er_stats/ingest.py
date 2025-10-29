"""Data ingestion workflow for Eternal Return API payloads."""

from __future__ import annotations

import logging
from collections import deque
from typing import Callable, Iterable, Optional, Set

from .api_client import EternalReturnAPIClient
from .db import SQLiteStore


logger = logging.getLogger(__name__)


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
    ) -> None:
        self.client = client
        self.store = store
        self.max_games_per_user = max_games_per_user
        self.fetch_game_details = fetch_game_details
        self._seen_games: Set[int] = set()
        self._progress_callback = progress_callback

    def _report(self, message: str) -> None:
        if self._progress_callback:
            self._progress_callback(message)
        else:
            logger.info(message)

    def ingest_user(self, user_num: int) -> Set[int]:
        """Ingest matches for a single user.

        Returns a set of newly discovered user numbers from the processed games.
        """

        discovered: Set[int] = set()
        next_token: Optional[str] = None
        processed = 0
        self._report(f"Fetching games for user {user_num}")
        while True:
            payload = self.client.fetch_user_games(user_num, next_token)
            games = payload.get("userGames", [])
            for game in games:
                self.store.upsert_from_game_payload(game)
                processed += 1
                self._report(
                    f"Processed game {processed} for user {user_num}"
                )
                if self.fetch_game_details:
                    discovered.update(self._ingest_game_participants(game.get("gameId")))
                if self.max_games_per_user and processed >= self.max_games_per_user:
                    break
            if self.max_games_per_user and processed >= self.max_games_per_user:
                break
            next_token = payload.get("next")
            if not next_token:
                break
        return discovered

    def ingest_from_seeds(self, seeds: Iterable[int], *, depth: int = 1) -> None:
        """Recursively ingest matches starting from the provided seed users."""

        queue = deque((seed, 0) for seed in seeds)
        seen_users: Set[int] = set()
        while queue:
            user_num, current_depth = queue.popleft()
            if user_num in seen_users:
                continue
            seen_users.add(user_num)
            self._report(f"Ingesting user {user_num} at depth {current_depth}")
            new_users = self.ingest_user(user_num)
            self._report(
                f"Discovered {len(new_users)} new users from user {user_num}"
            )
            if current_depth + 1 > depth:
                continue
            for next_user in new_users:
                if next_user not in seen_users:
                    queue.append((next_user, current_depth + 1))

    def _ingest_game_participants(self, game_id: Optional[int]) -> Set[int]:
        if not game_id or game_id in self._seen_games:
            return set()
        self._seen_games.add(game_id)
        payload = self.client.fetch_game_result(game_id)
        participants = payload.get("userGames", [])
        discovered: Set[int] = set()
        for participant in participants:
            self.store.upsert_from_game_payload(participant)
            discovered.add(participant.get("userNum"))
        self._report(
            f"Fetched {len(participants)} participants for game {game_id}"
        )
        return discovered


__all__ = ["IngestionManager"]
