"""Data ingestion workflow for Eternal Return API payloads."""

from __future__ import annotations

import datetime as dt
import logging
import time
from collections import deque
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

import requests

from .api_client import (
    ApiResponseError,
    EternalReturnAPIClient,
    is_nickname_not_found_error,
    is_transport_not_found_error,
    is_user_games_no_games_error,
    is_user_games_uid_missing_error,
)
from .db import SQLiteStore, parse_start_time

try:
    # Optional Parquet export; available when pyarrow is installed
    from .parquet_export import ParquetExporter
except ImportError:  # pragma: no cover - optional dependency
    ParquetExporter = None  # type: ignore


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _is_game_result_payload_not_found_error(exc: Exception) -> bool:
    """Return True when game-result endpoint reports a missing game payload."""

    return (
        isinstance(exc, ApiResponseError)
        and exc.code == 404
        and "/v1/games/" in exc.url
    )


class IngestionManager:
    """Coordinate recursive ingestion of match data."""

    def __init__(
        self,
        client: EternalReturnAPIClient,
        store: SQLiteStore,
        *,
        max_games_per_user: int | None = None,
        fetch_game_details: bool = True,
        progress_callback: Callable[[str], None] | None = None,
        parquet_exporter: "ParquetExporter" | None = None,
        only_newer_games: bool = False,
        prefer_nickname_fetch: bool = False,
        nickname_recheck_interval: dt.timedelta = dt.timedelta(hours=24),
        uid_recheck_interval: dt.timedelta = dt.timedelta(hours=24),
        max_nickname_attempts: int = 3,
        max_payload404_uids_per_seed: int = 3,
        max_seed_uid_resolve_attempts: int = 5,
        max_failed_uids_per_seed: int | None = None,
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
        self.uid_recheck_interval = uid_recheck_interval
        self.max_nickname_attempts = int(max_nickname_attempts)
        self.max_payload404_uids_per_seed = int(max_payload404_uids_per_seed)
        if max_failed_uids_per_seed is None:
            self.max_failed_uids_per_seed = self.max_payload404_uids_per_seed
        else:
            self.max_failed_uids_per_seed = int(max_failed_uids_per_seed)
        self.max_seed_uid_resolve_attempts = int(max_seed_uid_resolve_attempts)
        self.participant_retry_attempts = int(participant_retry_attempts)
        self.participant_retry_delay = float(participant_retry_delay)
        self.ingest_started_at = dt.datetime.now(dt.timezone.utc)
        self._not_found_nicknames: Set[str] = set()
        self._uid_missing_uids_by_seed: Dict[str, Set[str]] = {}
        self._seed_uid_resolve_attempts: Dict[str, int] = {}

    def _report(self, message: str) -> None:
        if self._progress_callback:
            self._progress_callback(message)
        else:
            logger.info(message)

    def _queue_parquet_payload(
        self,
        payload: Dict[str, Any],
        parquet_buffer: Optional[List[Dict[str, Any]]],
    ) -> None:
        if self._parquet is None:
            return
        if parquet_buffer is not None:
            parquet_buffer.append(dict(payload))
        else:
            self._parquet.write_from_game_payload(payload)

    def _fetch_uid_with_retries(self, nickname: str) -> Optional[str]:
        if nickname in self._not_found_nicknames:
            return None
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_nickname_attempts + 1):
            try:
                payload = self.client.fetch_user_by_nickname(nickname)
                user = payload.get("user") or {}
                uid_value = user.get("userId") or user.get("uid")
                if isinstance(uid_value, str) and uid_value:
                    return uid_value
                raise ValueError(f"Nickname '{nickname}' did not resolve to a uid.")
            except ApiResponseError as exc:
                last_exc = exc
                if is_nickname_not_found_error(exc):
                    self._not_found_nicknames.add(nickname)
                    break
                if attempt >= self.max_nickname_attempts:
                    break
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
        """Resolve a nickname to UID using cached mapping first, then API."""

        if not isinstance(nickname, str) or not nickname:
            return None
        cached_uid = None
        if not self.prefer_nickname_fetch:
            cached = self.store.get_uid_info_for_nickname(nickname)
            if cached:
                cached_uid = cached[0]
        if cached_uid:
            return cached_uid
        return self._fetch_uid_with_retries(nickname)

    def _record_seed_uid_missing_uid(self, seed_nickname: str, uid: str) -> None:
        if not seed_nickname:
            return
        if not isinstance(uid, str) or not uid:
            return
        uid_set = self._uid_missing_uids_by_seed.setdefault(seed_nickname, set())
        uid_set.add(uid)

    def _is_seed_uid_missing_uid(self, seed_nickname: str, uid: str) -> bool:
        if not seed_nickname:
            return False
        if not isinstance(uid, str) or not uid:
            return False
        uid_set = self._uid_missing_uids_by_seed.get(seed_nickname)
        return uid_set is not None and uid in uid_set

    def _next_seed_uid_resolve_attempt(self, seed_nickname: str) -> int:
        if not seed_nickname:
            return 0
        previous = self._seed_uid_resolve_attempts.get(seed_nickname, 0)
        current = previous + 1
        self._seed_uid_resolve_attempts[seed_nickname] = current
        return current

    def _prepare_seed_recovery_after_uid_missing(
        self, seed_nickname: str, uid: str
    ) -> tuple[Optional[int], Optional[str]]:
        self._record_seed_uid_missing_uid(seed_nickname, uid)
        uid_missing_uids = self._uid_missing_uids_by_seed.get(seed_nickname, set())
        uid_missing_count = len(uid_missing_uids)
        if uid_missing_count >= self.max_failed_uids_per_seed:
            sorted_uids = ", ".join(sorted(uid_missing_uids))
            reason = (
                f"Stopping ingest for seed '{seed_nickname}' because failed uid variants reached "
                f"{uid_missing_count} (limit {self.max_failed_uids_per_seed}; uids={sorted_uids})."
            )
            return None, reason
        resolve_attempt = self._next_seed_uid_resolve_attempt(seed_nickname)
        if resolve_attempt >= self.max_seed_uid_resolve_attempts:
            reason = (
                f"Stopping ingest for seed '{seed_nickname}' because resolve attempts reached "
                f"{resolve_attempt} (limit {self.max_seed_uid_resolve_attempts})."
            )
            return None, reason
        return resolve_attempt, None

    def _try_recover_seed_uid(
        self,
        *,
        uid: str,
        seed_nickname: Optional[str],
        error_label: str,
    ) -> Optional[str]:
        """Attempt to recover a seed UID by resolving nickname again."""

        if not seed_nickname:
            return None
        resolve_attempt, stop_reason = self._prepare_seed_recovery_after_uid_missing(
            seed_nickname, uid
        )
        if stop_reason:
            self._report(stop_reason)
            return None
        assert resolve_attempt is not None
        self._report(
            f"UID {uid} returned {error_label}; retrying nickname lookup for '{seed_nickname}' "
            f"(resolve attempt {resolve_attempt})"
        )
        resolved_uid = self._fetch_uid_with_retries(seed_nickname)
        if not resolved_uid:
            self._report(
                f"Stopping ingest for seed '{seed_nickname}' because nickname lookup failed "
                f"after {error_label} at resolve attempt {resolve_attempt}."
            )
            return None
        self._report(
            f"Resolved nickname '{seed_nickname}' to uid {resolved_uid} at attempt {resolve_attempt}"
        )
        if resolved_uid == uid:
            self._report(
                f"Stopping ingest for seed '{seed_nickname}' because resolved uid remained unchanged after {error_label} ({uid})."
            )
            return None
        if self._is_seed_uid_missing_uid(seed_nickname, resolved_uid):
            self._report(
                f"Stopping ingest for seed '{seed_nickname}' because resolved uid {resolved_uid} already returned payload 401/404 in this run."
            )
            return None
        return resolved_uid

    def _needs_uid_recheck(self, uid: str) -> bool:
        last_checked = self.store.get_user_last_checked(uid)
        if last_checked is None:
            return False
        try:
            checked_dt = dt.datetime.fromisoformat(last_checked)
        except ValueError:
            return True
        now = dt.datetime.now(dt.timezone.utc)
        return now - checked_dt > self.uid_recheck_interval

    def _mark_uid_checked(self, uid: str) -> None:
        now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
        try:
            self.store.update_user_last_checked(uid, now_iso)
        except Exception:
            pass

    def _validate_uid(self, uid: str, nickname: Optional[str]) -> Optional[str]:
        """Return a valid uid (possibly re-resolved) or None when not recoverable."""

        if not self._needs_uid_recheck(uid):
            return uid
        try:
            # Existence check only; ignore payload
            self.client.fetch_user_games(uid, None)
            self._mark_uid_checked(uid)
            return uid
        except (requests.HTTPError, ApiResponseError) as exc:
            if is_user_games_uid_missing_error(exc) and nickname:
                resolved_uid = self._try_recover_seed_uid(
                    uid=uid,
                    seed_nickname=nickname,
                    error_label="payload 401",
                )
                if resolved_uid:
                    self._mark_uid_checked(resolved_uid)
                    return resolved_uid
            if is_user_games_no_games_error(exc):
                resolved_uid = self._try_recover_seed_uid(
                    uid=uid,
                    seed_nickname=nickname,
                    error_label="payload 404",
                )
                if resolved_uid:
                    self._mark_uid_checked(resolved_uid)
                    return resolved_uid
                self._report(
                    f"Payload 404 from user/games for uid {uid} indicates no games; continuing without uid re-resolution."
                )
                self._mark_uid_checked(uid)
                return uid
            raise

    def ingest_user(self, uid: str, *, seed_nickname: Optional[str] = None) -> Set[str]:
        """Ingest matches for a single user.

        Returns a set of newly discovered nicknames from the processed games.
        """

        uid = str(uid)
        discovered: Set[str] = set()
        next_token: Optional[str] = None
        processed = 0

        if seed_nickname:
            try:
                validated = self._validate_uid(uid, seed_nickname)
                if validated is None:
                    self._report(
                        f"Aborting ingest for uid {uid}: failed to revalidate nickname '{seed_nickname}'"
                    )
                    return discovered
                uid = validated
            except (requests.HTTPError, ApiResponseError) as exc:
                if is_transport_not_found_error(exc):
                    self._report(
                        f"Aborting ingest for uid {uid} due to unrecoverable HTTP 404: {exc}"
                    )
                    raise
                self._report(
                    f"Aborting ingest for uid {uid} due to validation error: {exc}"
                )
                return discovered
            except Exception as exc:
                self._report(
                    f"Aborting ingest for uid {uid} due to validation error: {exc}"
                )
                return discovered

        self._report(f"Fetching games for uid {uid}")

        cutoff: Optional[dt.datetime] = None
        if self.only_newer_games:
            ingested_until = self.store.get_user_ingested_until(uid)
            if ingested_until:
                try:
                    cutoff = dt.datetime.fromisoformat(ingested_until)
                except ValueError:
                    cutoff = None
        prune_cutoff: Optional[dt.datetime] = None
        prune_before = self.store.get_prune_before()
        if prune_before:
            try:
                prune_cutoff = dt.datetime.fromisoformat(prune_before)
            except ValueError:
                self._report(
                    f"Ignoring invalid prune cutoff stored in DB: {prune_before}"
                )

        stop_due_to_cutoff = False
        stop_due_to_prune = False
        while True:
            try:
                payload = self.client.fetch_user_games(uid, next_token)
            except (requests.HTTPError, ApiResponseError) as exc:
                if is_user_games_uid_missing_error(exc):
                    resolved_uid = self._try_recover_seed_uid(
                        uid=uid,
                        seed_nickname=seed_nickname,
                        error_label="payload 401",
                    )
                    if resolved_uid:
                        uid = resolved_uid
                        next_token = None
                        continue
                    break
                if is_user_games_no_games_error(exc):
                    resolved_uid = self._try_recover_seed_uid(
                        uid=uid,
                        seed_nickname=seed_nickname,
                        error_label="payload 404",
                    )
                    if resolved_uid:
                        uid = resolved_uid
                        next_token = None
                        continue
                    self._report(
                        f"Payload 404 from user/games for uid {uid} indicates no games; stopping ingest for this uid."
                    )
                    break
                if is_transport_not_found_error(exc):
                    self._report(
                        f"Aborting ingest for uid {uid} due to unrecoverable HTTP 404: {exc}"
                    )
                    raise
                self._report(f"Aborting ingest for uid {uid} due to error: {exc}")
                break
            else:
                self._mark_uid_checked(uid)
            games = payload.get("userGames", [])
            deleted_ids = self.store.list_deleted_games(
                [
                    game_id
                    for game_id in (game.get("gameId") for game in games)
                    if game_id is not None
                ]
            )
            for game in games:
                start_iso = parse_start_time(game.get("startDtm"))
                start_dt = None
                if start_iso:
                    try:
                        start_dt = dt.datetime.fromisoformat(start_iso)
                    except ValueError:
                        start_dt = None
                if prune_cutoff and start_dt and start_dt <= prune_cutoff:
                    stop_due_to_prune = True
                    self._report(
                        "Encountered game older than prune cutoff "
                        f"{prune_before} for uid {uid}; stopping early"
                    )
                    break
                if cutoff and start_dt and start_dt <= cutoff:
                    stop_due_to_cutoff = True
                    self._report(
                        "Encountered previously ingested game "
                        f"{game.get('gameId')} for uid {uid}; stopping early"
                    )
                    break
                game_id = game.get("gameId")
                if game_id in deleted_ids:
                    self._report(f"Skipping deleted game {game_id} for uid {uid}")
                    continue
                game_already_known = bool(game_id and self.store.has_game(game_id))
                game["uid"] = uid
                parquet_payloads: Optional[List[Dict[str, Any]]] = (
                    [] if self._parquet is not None else None
                )
                with self.store.transaction():
                    self.store.upsert_from_game_payload(game, mark_ingested=True)
                    if self.fetch_game_details:
                        discovered.update(
                            self._ingest_game_participants(
                                game_id,
                                already_known=game_already_known,
                                parquet_buffer=parquet_payloads,
                            )
                        )
                if self._parquet is not None:
                    self._parquet.write_from_game_payload(game)
                    if parquet_payloads:
                        for participant in parquet_payloads:
                            self._parquet.write_from_game_payload(participant)
                processed += 1
                self._report(f"Processed game {processed}({game_id}) for uid {uid}")
                if self.max_games_per_user and processed >= self.max_games_per_user:
                    break
            if stop_due_to_prune or stop_due_to_cutoff:
                break
            if self.max_games_per_user and processed >= self.max_games_per_user:
                break
            next_token = payload.get("next")
            if not next_token:
                break
        return discovered

    def ingest_from_seeds(self, seeds: Iterable[str], *, depth: int = 1) -> None:
        """Recursively ingest matches starting from the provided seed nicknames."""

        queue = deque((seed, 0) for seed in seeds)
        seen_nicknames: Set[str] = set()
        while queue:
            self._report(f"Ingest queue left: {len(queue)} users")
            nickname, current_depth = queue.popleft()
            if nickname in seen_nicknames:
                continue
            seen_nicknames.add(nickname)
            uid = self._resolve_uid(nickname, None)
            if uid is None:
                self._report(
                    f"Skipping nickname '{nickname}'; could not resolve to uid"
                )
                continue
            self._report(
                f"Ingesting nickname '{nickname}' (uid {uid}) at depth {current_depth}"
            )
            new_users = self.ingest_user(uid, seed_nickname=nickname)
            self._report(
                f"Discovered {len(new_users)} new users from nickname '{nickname}'"
            )
            if current_depth + 1 > depth:
                continue
            for next_user in new_users:
                if next_user not in seen_nicknames:
                    queue.append((next_user, current_depth + 1))

    def _ingest_game_participants(
        self,
        game_id: Optional[int],
        *,
        already_known: bool = False,
        parquet_buffer: Optional[List[Dict[str, Any]]] = None,
    ) -> Set[str]:
        discovered, _, _ = self._ingest_game_participants_core(
            game_id,
            already_known=already_known,
            force_fetch=False,
            parquet_buffer=parquet_buffer,
        )
        return discovered

    def _ingest_game_participants_core(
        self,
        game_id: Optional[int],
        *,
        already_known: bool,
        force_fetch: bool,
        parquet_buffer: Optional[List[Dict[str, Any]]] = None,
    ) -> tuple[Set[str], bool, int]:
        if not game_id or game_id in self._seen_games:
            return set(), False, 0
        if self.store.is_game_deleted(game_id):
            self._report(f"Skipping deleted game {game_id} participant fetch")
            return set(), False, 0
        self._seen_games.add(game_id)
        if already_known and not force_fetch:
            cached_participants = self.store.get_participants_for_game(game_id)
            if cached_participants and len(cached_participants) > 1:
                self._report(
                    f"Skipping API fetch for known game {game_id}; "
                    f"loaded {len(cached_participants)} participants from cache"
                )
                cached_nicknames = {
                    n
                    for n in (
                        self.store.get_latest_nickname_for_uid(uid)
                        for uid in cached_participants
                    )
                    if isinstance(n, str) and n
                }
                return cached_nicknames, False, len(cached_participants)
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
                if self._needs_uid_recheck(uid):
                    try:
                        validated_uid = self._validate_uid(
                            uid, participant.get("nickname")
                        )
                    except Exception as exc:
                        self._report(
                            f"Skipping participant due to uid validation error: {exc}"
                        )
                        break
                    if validated_uid is None:
                        break
                    uid = validated_uid
                participant["uid"] = uid
                try:
                    self.store.upsert_from_game_payload(
                        participant, mark_ingested=False
                    )
                    success = True
                    self._queue_parquet_payload(participant, parquet_buffer)
                    participant_nickname = participant.get("nickname")
                    if isinstance(participant_nickname, str) and participant_nickname:
                        discovered.add(participant_nickname)
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
        return discovered, incomplete, len(participants)

    def _refetch_delay(self, attempts: int) -> dt.timedelta:
        base_days = 1
        max_days = 30
        delay_days = min(base_days * (2 ** max(attempts - 1, 0)), max_days)
        return dt.timedelta(days=delay_days)

    def _record_refetch_failure(
        self,
        game_id: int,
        *,
        status: str,
        error: str,
    ) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        previous_attempts = self.store.get_refetch_attempts(game_id)
        attempts = previous_attempts + 1
        next_refetch_at = (now + self._refetch_delay(attempts)).isoformat()
        self.store.upsert_refetch_status(
            game_id,
            status=status,
            attempts=attempts,
            last_refetch_at=now.isoformat(),
            next_refetch_at=next_refetch_at,
            last_error=error,
        )

    def refetch_incomplete_games(self, game_ids: Iterable[int]) -> dict[str, int]:
        """Refetch participant data for matches flagged as incomplete."""

        stats = {
            "total": 0,
            "cleared": 0,
            "not_found": 0,
            "empty": 0,
            "still_incomplete": 0,
        }
        for game_id in game_ids:
            stats["total"] += 1
            parquet_payloads: Optional[List[Dict[str, Any]]] = (
                [] if self._parquet is not None else None
            )
            try:
                with self.store.transaction():
                    _, incomplete, participant_count = (
                        self._ingest_game_participants_core(
                            int(game_id),
                            already_known=True,
                            force_fetch=True,
                            parquet_buffer=parquet_payloads,
                        )
                    )
                    if participant_count == 0:
                        self._report(
                            f"Game {game_id} returned 0 participants; keeping incomplete flag"
                        )
                        self._record_refetch_failure(
                            int(game_id),
                            status="error",
                            error="empty_participants",
                        )
                        stats["empty"] += 1
                    elif not incomplete:
                        self.store.clear_game_incomplete(int(game_id))
                        self.store.clear_refetch_status(int(game_id))
                        stats["cleared"] += 1
                    else:
                        self._record_refetch_failure(
                            int(game_id),
                            status="error",
                            error="incomplete_participants",
                        )
                        stats["still_incomplete"] += 1
            except (requests.HTTPError, ApiResponseError) as exc:
                if _is_game_result_payload_not_found_error(exc):
                    self._report(
                        f"Game {game_id} returned 404; keeping incomplete flag"
                    )
                    self._record_refetch_failure(
                        int(game_id),
                        status="missing",
                        error="http_404",
                    )
                    stats["not_found"] += 1
                    continue
                if is_transport_not_found_error(exc):
                    self._report(
                        f"Game {game_id} failed due to unrecoverable HTTP 404: {exc}"
                    )
                raise
            if self._parquet is not None and parquet_payloads:
                for participant in parquet_payloads:
                    self._parquet.write_from_game_payload(participant)
        return stats


__all__ = ["IngestionManager"]
