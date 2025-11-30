"""Data ingestion workflow for Eternal Return API payloads."""

from __future__ import annotations

import datetime as dt
import logging
import time
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
        nickname_recheck_interval: dt.timedelta = dt.timedelta(hours=24),
        max_nickname_attempts: int = 3,
        participant_retry_attempts: int = 2,
        participant_retry_delay: float = 1.0,
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
        self.nickname_recheck_interval = nickname_recheck_interval
        self.max_nickname_attempts = int(max_nickname_attempts)
        self.participant_retry_attempts = int(participant_retry_attempts)
        self.participant_retry_delay = float(participant_retry_delay)
        self.ingest_started_at = dt.datetime.now(dt.timezone.utc)

    def _report(self, message: str) -> None:
        if self._progress_callback:
            self._progress_callback(message)
        else:
            logger.info(message)

    def _fetch_uid_with_retries(self, nickname: str) -> Optional[str]:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_nickname_attempts + 1):
            try:
                payload = self.client.fetch_user_by_nickname(nickname)
                user = payload.get("user") or {}
                uid_value = user.get("userId") or user.get("uid")
                if isinstance(uid_value, str) and uid_value:
                    return uid_value
                raise ValueError(f"Nickname '{nickname}' did not resolve to a uid.")
            except Exception as exc:
                last_exc = exc
                if attempt >= self.max_nickname_attempts:
                    break
        if last_exc is not None:
            self._report(
                f"Failed to resolve nickname '{nickname}' after {self.max_nickname_attempts} attempts: {last_exc}"
            )
        return None

    def _resolve_uid(self, nickname: str, start_dtm: Optional[str]) -> Optional[str]:
        """Resolve a nickname to UID, preferring cached mappings and re-checking when stale."""

        if not isinstance(nickname, str) or not nickname:
            return None
        start_iso = parse_start_time(start_dtm)
        start_dt: Optional[dt.datetime] = None
        if start_iso:
            try:
                start_dt = dt.datetime.fromisoformat(start_iso)
            except ValueError:
                start_dt = None

        cached_uid: Optional[str] = None
        cached_last_seen_dt: Optional[dt.datetime] = None
        if not self.prefer_nickname_fetch:
            cached = self.store.get_uid_info_for_nickname(nickname)
            if cached:
                cached_uid, cached_last_seen = cached
                if cached_last_seen:
                    try:
                        cached_last_seen_dt = dt.datetime.fromisoformat(
                            cached_last_seen
                        )
                    except ValueError:
                        cached_last_seen_dt = None

        reference_dt = start_dt or self.ingest_started_at

        # Trust cache when it is recent enough relative to the match timestamp
        if cached_uid is not None:
            if cached_last_seen_dt is None:
                return cached_uid
            if reference_dt <= cached_last_seen_dt:
                return cached_uid
            if reference_dt - cached_last_seen_dt <= self.nickname_recheck_interval:
                return cached_uid

        resolved = self._fetch_uid_with_retries(nickname)
        return resolved

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
                game["uid"] = uid
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
        incomplete = False
        for participant in participants:
            success = False
            for attempt in range(1, self.participant_retry_attempts + 1):
                if not participant.get("startDtm"):
                    participant["startDtm"] = self.ingest_started_at.isoformat()
                uid = self._resolve_uid(
                    participant.get("nickname", ""), participant.get("startDtm")
                )
                if uid is None:
                    if attempt < self.participant_retry_attempts:
                        time.sleep(self.participant_retry_delay)
                    continue
                participant["uid"] = uid
                try:
                    self.store.upsert_from_game_payload(participant)
                    success = True
                    if self._parquet is not None:
                        self._parquet.write_from_game_payload(participant)
                    discovered.add(uid)
                    break
                except ValueError as exc:
                    self._report(
                        f"Skipping participant attempt {attempt} for game {game_id} due to error: {exc}"
                    )
                    if attempt < self.participant_retry_attempts:
                        time.sleep(self.participant_retry_delay)
            if not success:
                incomplete = True
        if incomplete and game_id is not None:
            self.store.mark_game_incomplete(int(game_id))
        self._report(f"Fetched {len(participants)} participants for game {game_id}")
        return discovered


__all__ = ["IngestionManager"]
