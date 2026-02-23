import datetime as dt
from typing import Any, Dict, Optional

import pytest
import requests

from er_stats.api_client import ApiResponseError
from er_stats.ingest import IngestionManager


class FakeClient:
    def __init__(
        self,
        pages: list[Dict[str, Any]],
        participants: Dict[int, Dict[str, Any]],
        users: Dict[str, str],
        nickname_failures: Optional[Dict[str, int]] = None,
    ) -> None:
        self.pages = pages
        self.participants = participants
        self.users = users
        self.fetch_user_games_calls: list[Optional[str]] = []
        self.fetch_user_games_uids: list[str] = []
        self.fetch_game_result_calls: list[int] = []
        self.fetch_user_by_nickname_calls: list[str] = []
        self.nickname_failures = dict(nickname_failures or {})

    def fetch_user_by_nickname(self, nickname: str) -> Dict[str, Any]:
        self.fetch_user_by_nickname_calls.append(nickname)
        failures_left = self.nickname_failures.get(nickname, 0)
        if failures_left > 0:
            self.nickname_failures[nickname] = failures_left - 1
            raise RuntimeError(f"simulated nickname lookup failure for {nickname}")
        uid = self.users.get(nickname)
        return {
            "code": 200,
            "message": "Success",
            "user": {
                "nickname": nickname,
                "userId": uid,
            },
        }

    def fetch_user_games(
        self, uid: str, next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        self.fetch_user_games_calls.append(next_token)
        self.fetch_user_games_uids.append(uid)
        if next_token is None:
            return self.pages[0]
        return self.pages[1]

    def fetch_game_result(self, game_id: int) -> Dict[str, Any]:
        self.fetch_game_result_calls.append(game_id)
        return self.participants.get(game_id, {"userGames": []})


def _generate_uids(nicknames: list[str]) -> Dict[str, str]:
    return {nickname: f"UID-{nickname}-TEST" for nickname in nicknames}


def test_ingest_user_and_participants(store, make_game):
    # Prepare two pages of userGames
    g1 = make_game(game_id=1, nickname="100")
    g2 = make_game(game_id=2, nickname="100")
    pages = [
        {"userGames": [g1], "next": "tok"},
        {"userGames": [g2]},
    ]

    # Participants for each game introduce new users
    p1_a = make_game(game_id=1, nickname="200")
    p1_b = make_game(game_id=1, nickname="201")
    p2_a = make_game(game_id=2, nickname="300")
    participants = {
        1: {"userGames": [p1_a, p1_b]},
        2: {"userGames": [p2_a]},
    }

    # nickname - UID map
    users = _generate_uids(["100", "200", "201", "300"])

    client = FakeClient(pages, participants, users)
    manager = IngestionManager(
        client, store, max_games_per_user=None, fetch_game_details=True
    )

    discovered = manager.ingest_user(users["100"])

    # Discovered users from participants (nicknames)
    assert {"200", "201", "300"}.issubset(discovered)
    assert client.fetch_user_games_uids == [users["100"], users["100"]]

    # Data persisted for seed and participants
    count = store.connection.execute(
        "SELECT COUNT(*) FROM user_match_stats"
    ).fetchone()[0]
    assert count == 5  # 2 seed matches + 3 participant entries


def test_ingest_skips_known_game_details(store, make_game):
    # users
    nicknames = [
        "100",
        "200",
        "201",
    ]
    users = _generate_uids(nicknames)

    # games
    existing = make_game(game_id=10, nickname=nicknames[0], uid=users[nicknames[0]])
    existing_participant_a = make_game(
        game_id=10, nickname=nicknames[1], uid=users[nicknames[1]]
    )
    existing_participant_b = make_game(
        game_id=10, nickname=nicknames[2], uid=users[nicknames[2]]
    )

    # store into db
    for payload in (existing, existing_participant_a, existing_participant_b):
        store.upsert_from_game_payload(payload)

    client = FakeClient(pages=[{"userGames": [existing]}], participants={}, users=users)
    manager = IngestionManager(client, store, fetch_game_details=True)

    discovered = manager.ingest_user(users["100"])

    assert {"200", "201"}.issubset(discovered)
    assert client.fetch_game_result_calls == []


def test_ingest_only_newer_games_breaks_at_cutoff(store, make_game):
    users = _generate_uids(["100", "200"])

    previous = make_game(game_id=1, nickname="100", uid=users["100"])
    previous["startDtm"] = "2025-01-01T00:00:00.000+0000"
    store.upsert_from_game_payload(previous)

    newer = make_game(game_id=2, nickname="100", uid=users["100"])
    newer["startDtm"] = "2025-01-02T00:00:00.000+0000"
    older = make_game(game_id=3, nickname="100", uid=users["100"])
    older["startDtm"] = "2025-01-01T00:00:00.000+0000"

    pages = [{"userGames": [newer, older], "next": "tok"}, {"userGames": []}]

    participants = {
        2: {"userGames": [make_game(game_id=2, nickname="200", uid=users["200"])]},
    }

    client = FakeClient(pages, participants, users)
    manager = IngestionManager(
        client,
        store,
        fetch_game_details=True,
        only_newer_games=True,
    )

    manager.ingest_user(users["100"])

    # Only the first page should be fetched and only the newer game processed
    assert client.fetch_user_games_calls == [None]
    assert client.fetch_game_result_calls == [2]
    assert store.has_game(2)
    assert not store.has_game(3)


def test_ingest_includes_older_games_when_cutoff_disabled(store, make_game):
    users = _generate_uids(["100"])

    existing = make_game(game_id=1, nickname="100", uid=users["100"])
    store.upsert_from_game_payload(existing)

    older = make_game(game_id=2, nickname="100", uid=users["100"])
    older["startDtm"] = "2025-01-01T00:00:00.000+0000"
    newest = make_game(game_id=3, nickname="100", uid=users["100"])
    newest["startDtm"] = "2025-01-03T00:00:00.000+0000"

    pages = [
        {"userGames": [existing, older], "next": "tok"},
        {"userGames": [newest]},
    ]

    client = FakeClient(pages, participants={}, users=users)
    manager = IngestionManager(
        client,
        store,
        fetch_game_details=False,
        only_newer_games=False,
    )

    manager.ingest_user(users["100"])

    # The paginator should continue despite encountering a known game.
    assert client.fetch_user_games_calls == [None, "tok"]
    assert store.has_game(2)
    assert store.has_game(3)


def test_ingest_cutoff_uses_ingested_until_not_last_seen(store, make_game):
    users = _generate_uids(["100"])

    observed = make_game(game_id=1, nickname="100", uid=users["100"])
    observed["startDtm"] = "2025-02-01T00:00:00.000+0000"
    store.upsert_from_game_payload(observed, mark_ingested=False)

    older = make_game(game_id=2, nickname="100", uid=users["100"])
    older["startDtm"] = "2025-01-01T00:00:00.000+0000"

    client = FakeClient(pages=[{"userGames": [older]}], participants={}, users=users)
    manager = IngestionManager(
        client, store, fetch_game_details=False, only_newer_games=True
    )

    manager.ingest_user(users["100"])

    assert client.fetch_user_games_calls == [None]
    assert store.has_game(2)


def test_ingest_uses_cached_uid_without_recheck(store, make_game):
    nickname = "dup"
    old_uid = "UID-old"
    old_game = make_game(game_id=1, nickname=nickname, uid=old_uid)
    old_game["startDtm"] = "2025-01-01T00:00:00+00:00"
    store.upsert_from_game_payload(old_game)

    seed_uid = "UID-seed"
    seed_game = make_game(game_id=10, nickname="seed", uid=seed_uid)
    seed_game["startDtm"] = "2025-01-03T00:00:00+00:00"
    pages = [{"userGames": [seed_game]}]

    participant = make_game(game_id=10, nickname=nickname)
    participant["startDtm"] = "2025-01-03T00:00:00+00:00"
    participants = {10: {"userGames": [participant]}}

    # New uid exists in API map, but cache should be used and API lookup skipped.
    users = {"seed": seed_uid, nickname: "UID-new"}
    client = FakeClient(pages, participants, users)
    manager = IngestionManager(
        client,
        store,
        nickname_recheck_interval=dt.timedelta(hours=1),
        max_nickname_attempts=2,
    )

    discovered = manager.ingest_user(seed_uid)

    assert nickname in discovered
    row = store.connection.execute(
        "SELECT uid FROM users WHERE nickname=? ORDER BY unixepoch(last_seen, 'auto') DESC LIMIT 1",
        (nickname,),
    ).fetchone()
    assert row is not None
    assert row[0] == old_uid
    assert nickname not in client.fetch_user_by_nickname_calls


def test_ingest_skips_unresolved_nickname_after_retries(store, make_game):
    seed_uid = "UID-seed"
    seed_game = make_game(game_id=20, nickname="seed", uid=seed_uid)
    pages = [{"userGames": [seed_game]}]

    missing_nickname = "ghost"
    participant = make_game(game_id=20, nickname=missing_nickname)
    participants = {20: {"userGames": [participant]}}

    client = FakeClient(
        pages, participants, {}, nickname_failures={missing_nickname: 2}
    )
    manager = IngestionManager(
        client,
        store,
        nickname_recheck_interval=dt.timedelta(seconds=0),
        max_nickname_attempts=2,
        participant_retry_attempts=1,
    )

    discovered = manager.ingest_user(seed_uid)

    assert discovered == set()
    assert client.fetch_user_by_nickname_calls == [missing_nickname, missing_nickname]
    count = store.connection.execute(
        "SELECT COUNT(*) FROM users WHERE nickname=?", (missing_nickname,)
    ).fetchone()[0]
    assert count == 0


def test_ingest_treats_payload_404_nickname_as_missing_in_current_run(store, make_game):
    class Nickname404Client(FakeClient):
        def fetch_user_by_nickname(self, nickname: str) -> Dict[str, Any]:
            self.fetch_user_by_nickname_calls.append(nickname)
            raise ApiResponseError(
                code=404,
                message="User Not Found",
                payload={"code": 404, "message": "User Not Found"},
                url="https://example.invalid/v1/user/nickname?query=ghost",
            )

    seed_uid = "UID-seed"
    seed_game = make_game(game_id=21, nickname="seed", uid=seed_uid)
    pages = [{"userGames": [seed_game]}]
    missing_nickname = "ghost"
    participant = make_game(game_id=21, nickname=missing_nickname)
    participants = {21: {"userGames": [participant]}}

    client = Nickname404Client(pages, participants, {})
    manager = IngestionManager(
        client,
        store,
        max_nickname_attempts=5,
        participant_retry_attempts=2,
        participant_retry_delay=0.0,
    )

    manager.ingest_user(seed_uid)

    # Payload 404 marks nickname as missing and suppresses additional calls.
    assert client.fetch_user_by_nickname_calls == [missing_nickname]


def test_ingest_reuses_missing_nickname_cache_across_participants(store, make_game):
    class Nickname404Client(FakeClient):
        def fetch_user_by_nickname(self, nickname: str) -> Dict[str, Any]:
            self.fetch_user_by_nickname_calls.append(nickname)
            raise ApiResponseError(
                code=404,
                message="User Not Found",
                payload={"code": 404, "message": "User Not Found"},
                url="https://example.invalid/v1/user/nickname?query=ghost",
            )

    seed_uid = "UID-seed"
    seed_game = make_game(game_id=22, nickname="seed", uid=seed_uid)
    pages = [{"userGames": [seed_game]}]
    missing_nickname = "ghost"
    participants = {
        22: {
            "userGames": [
                make_game(game_id=22, nickname=missing_nickname),
                make_game(game_id=22, nickname=missing_nickname),
            ]
        }
    }

    client = Nickname404Client(pages, participants, {})
    manager = IngestionManager(
        client,
        store,
        max_nickname_attempts=5,
        participant_retry_attempts=1,
    )

    manager.ingest_user(seed_uid)

    assert client.fetch_user_by_nickname_calls == [missing_nickname]


def test_ingest_keeps_cached_uid_when_start_missing(store, make_game):
    nickname = "dup"
    old_uid = "UID-old"
    old_game = make_game(game_id=1, nickname=nickname, uid=old_uid)
    old_game["startDtm"] = "2025-01-01T00:00:00+00:00"
    store.upsert_from_game_payload(old_game)

    seed_uid = "UID-seed"
    seed_game = make_game(game_id=30, nickname="seed", uid=seed_uid)
    pages = [{"userGames": [seed_game]}]

    participant = make_game(game_id=30, nickname=nickname)
    participant.pop("startDtm", None)
    participants = {30: {"userGames": [participant]}}

    users = {"seed": seed_uid, nickname: "UID-new"}
    client = FakeClient(pages, participants, users)
    manager = IngestionManager(
        client,
        store,
        nickname_recheck_interval=dt.timedelta(hours=1),
        max_nickname_attempts=2,
        participant_retry_attempts=1,
    )
    manager.ingest_started_at = dt.datetime(2025, 1, 5, tzinfo=dt.timezone.utc)

    manager.ingest_user(seed_uid)

    row = store.connection.execute(
        "SELECT uid FROM users WHERE nickname=? ORDER BY unixepoch(last_seen, 'auto') DESC LIMIT 1",
        (nickname,),
    ).fetchone()
    assert row is not None
    assert row[0] == old_uid
    assert nickname not in client.fetch_user_by_nickname_calls


def test_ingest_marks_incomplete_on_participant_fail(store, make_game):
    seed_uid = "UID-seed"
    seed_game = make_game(game_id=40, nickname="seed", uid=seed_uid)
    pages = [{"userGames": [seed_game]}]

    participant = make_game(game_id=40, nickname="ghost")
    participants = {40: {"userGames": [participant]}}

    client = FakeClient(pages, participants, {})
    manager = IngestionManager(
        client,
        store,
        max_nickname_attempts=1,
        participant_retry_attempts=1,
    )

    manager.ingest_user(seed_uid)

    row = store.connection.execute(
        "SELECT incomplete FROM matches WHERE game_id=?", (40,)
    ).fetchone()
    assert row is not None
    assert row[0] == 1


def test_ingest_rolls_back_on_interrupt(monkeypatch, store, make_game):
    seed_uid = "UID-seed"
    seed_game = make_game(game_id=60, nickname="seed", uid=seed_uid)
    pages = [{"userGames": [seed_game]}]

    participant = make_game(game_id=60, nickname="other")
    participants = {60: {"userGames": [participant]}}

    users = {"seed": seed_uid, "other": "UID-other"}
    client = FakeClient(pages, participants, users)
    manager = IngestionManager(client, store, fetch_game_details=True)

    original_upsert = store.upsert_from_game_payload
    call_count = {"count": 0}

    def interrupting_upsert(game, *, mark_ingested=True):
        call_count["count"] += 1
        original_upsert(game, mark_ingested=mark_ingested)
        if call_count["count"] == 2:
            raise KeyboardInterrupt()

    monkeypatch.setattr(store, "upsert_from_game_payload", interrupting_upsert)

    with pytest.raises(KeyboardInterrupt):
        manager.ingest_user(seed_uid)

    assert not store.has_game(60)
    count = store.connection.execute(
        "SELECT COUNT(*) FROM user_match_stats WHERE game_id=?", (60,)
    ).fetchone()[0]
    assert count == 0


def test_ingest_retries_uid_on_payload_404_using_nickname(store, make_game):
    class Payload404Client(FakeClient):
        def __init__(
            self,
            pages: list[Dict[str, Any]],
            participants: Dict[int, Dict[str, Any]],
            users: Dict[str, str],
        ):
            super().__init__(pages, participants, users)
            self.stale_uid = "UID-old"

        def fetch_user_games(
            self, uid: str, next_token: Optional[str] = None
        ) -> Dict[str, Any]:
            self.fetch_user_games_calls.append(next_token)
            self.fetch_user_games_uids.append(uid)
            if uid == self.stale_uid:
                raise ApiResponseError(
                    code=404,
                    message="User Not Found",
                    payload={"code": 404, "message": "User Not Found"},
                    url="https://example.invalid/v1/user/games/uid/stale",
                )
            if next_token is None:
                return self.pages[0]
            return self.pages[1]

    pages = [{"userGames": [make_game(game_id=50, nickname="seed")]}]
    client = Payload404Client(pages, {}, {"seed": "UID-new"})
    manager = IngestionManager(client, store, fetch_game_details=False)

    manager.ingest_user("UID-old", seed_nickname="seed")

    # First attempt with stale UID, then retry with fresh one
    assert client.fetch_user_games_uids == ["UID-old", "UID-new"]
    assert client.fetch_user_by_nickname_calls == ["seed"]


def test_ingest_stops_seed_on_repeated_payload_404_resolved_uid_cycle(store, make_game):
    class CyclingUidClient(FakeClient):
        def __init__(
            self,
            pages: list[Dict[str, Any]],
            participants: Dict[int, Dict[str, Any]],
            users: Dict[str, str],
        ):
            super().__init__(pages, participants, users)
            self._resolve_count = 0

        def fetch_user_by_nickname(self, nickname: str) -> Dict[str, Any]:
            self.fetch_user_by_nickname_calls.append(nickname)
            uid = "UID-b" if self._resolve_count == 0 else "UID-a"
            self._resolve_count += 1
            return {
                "code": 200,
                "message": "Success",
                "user": {
                    "nickname": nickname,
                    "userId": uid,
                },
            }

        def fetch_user_games(
            self, uid: str, next_token: Optional[str] = None
        ) -> Dict[str, Any]:
            self.fetch_user_games_calls.append(next_token)
            self.fetch_user_games_uids.append(uid)
            if uid in {"UID-a", "UID-b"}:
                raise ApiResponseError(
                    code=404,
                    message="User Not Found",
                    payload={"code": 404, "message": "User Not Found"},
                    url=f"https://example.invalid/v1/user/games/uid/{uid}",
                )
            return {"userGames": []}

    client = CyclingUidClient([], {}, {})
    manager = IngestionManager(client, store, fetch_game_details=False)

    discovered = manager.ingest_user("UID-a", seed_nickname="seed")

    assert discovered == set()
    assert client.fetch_user_games_uids == ["UID-a", "UID-b"]
    assert client.fetch_user_by_nickname_calls == ["seed", "seed"]


def test_ingest_stops_seed_when_payload_404_uid_variant_limit_reached(
    store, make_game
):
    class UniqueUidPayload404Client(FakeClient):
        def __init__(
            self,
            pages: list[Dict[str, Any]],
            participants: Dict[int, Dict[str, Any]],
            users: Dict[str, str],
        ):
            super().__init__(pages, participants, users)
            self._resolved_uids = ["UID-b", "UID-c", "UID-d", "UID-e"]
            self._resolve_count = 0

        def fetch_user_by_nickname(self, nickname: str) -> Dict[str, Any]:
            self.fetch_user_by_nickname_calls.append(nickname)
            uid = self._resolved_uids[self._resolve_count]
            self._resolve_count += 1
            return {
                "code": 200,
                "message": "Success",
                "user": {
                    "nickname": nickname,
                    "userId": uid,
                },
            }

        def fetch_user_games(
            self, uid: str, next_token: Optional[str] = None
        ) -> Dict[str, Any]:
            self.fetch_user_games_calls.append(next_token)
            self.fetch_user_games_uids.append(uid)
            raise ApiResponseError(
                code=404,
                message="User Not Found",
                payload={"code": 404, "message": "User Not Found"},
                url=f"https://example.invalid/v1/user/games/uid/{uid}",
            )

    client = UniqueUidPayload404Client([], {}, {})
    logs: list[str] = []
    manager = IngestionManager(
        client, store, fetch_game_details=False, progress_callback=logs.append
    )

    discovered = manager.ingest_user("UID-a", seed_nickname="seed")

    assert discovered == set()
    assert client.fetch_user_games_uids == ["UID-a", "UID-b", "UID-c"]
    assert client.fetch_user_by_nickname_calls == ["seed", "seed"]
    assert any("payload404 uid variants reached 3" in message for message in logs)


def test_ingest_stops_seed_when_payload_404_resolve_attempt_limit_reached(
    store, make_game
):
    class ResolveAttemptLimitClient(FakeClient):
        def __init__(
            self,
            pages: list[Dict[str, Any]],
            participants: Dict[int, Dict[str, Any]],
            users: Dict[str, str],
        ):
            super().__init__(pages, participants, users)
            self._resolve_count = 0

        def fetch_user_by_nickname(self, nickname: str) -> Dict[str, Any]:
            self.fetch_user_by_nickname_calls.append(nickname)
            self._resolve_count += 1
            return {
                "code": 200,
                "message": "Success",
                "user": {
                    "nickname": nickname,
                    "userId": f"UID-r{self._resolve_count}",
                },
            }

        def fetch_user_games(
            self, uid: str, next_token: Optional[str] = None
        ) -> Dict[str, Any]:
            self.fetch_user_games_calls.append(next_token)
            self.fetch_user_games_uids.append(uid)
            raise ApiResponseError(
                code=404,
                message="User Not Found",
                payload={"code": 404, "message": "User Not Found"},
                url=f"https://example.invalid/v1/user/games/uid/{uid}",
            )

    client = ResolveAttemptLimitClient([], {}, {})
    logs: list[str] = []
    manager = IngestionManager(
        client,
        store,
        fetch_game_details=False,
        max_payload404_uids_per_seed=99,
        max_seed_uid_resolve_attempts=5,
        progress_callback=logs.append,
    )

    discovered = manager.ingest_user("UID-r0", seed_nickname="seed")

    assert discovered == set()
    assert client.fetch_user_games_uids == ["UID-r0", "UID-r1", "UID-r2", "UID-r3", "UID-r4"]
    assert client.fetch_user_by_nickname_calls == ["seed", "seed", "seed", "seed"]
    assert any("resolve attempts reached 5" in message for message in logs)


def test_ingest_stops_seed_when_payload_404_resolves_to_same_uid(store, make_game):
    class SameUidClient(FakeClient):
        def fetch_user_by_nickname(self, nickname: str) -> Dict[str, Any]:
            self.fetch_user_by_nickname_calls.append(nickname)
            return {
                "code": 200,
                "message": "Success",
                "user": {
                    "nickname": nickname,
                    "userId": "UID-a",
                },
            }

        def fetch_user_games(
            self, uid: str, next_token: Optional[str] = None
        ) -> Dict[str, Any]:
            self.fetch_user_games_calls.append(next_token)
            self.fetch_user_games_uids.append(uid)
            raise ApiResponseError(
                code=404,
                message="User Not Found",
                payload={"code": 404, "message": "User Not Found"},
                url=f"https://example.invalid/v1/user/games/uid/{uid}",
            )

    client = SameUidClient([], {}, {})
    manager = IngestionManager(client, store, fetch_game_details=False)

    discovered = manager.ingest_user("UID-a", seed_nickname="seed")

    assert discovered == set()
    assert client.fetch_user_games_uids == ["UID-a"]
    assert client.fetch_user_by_nickname_calls == ["seed"]


def test_ingest_from_seeds_continues_after_payload_404_seed_stop(store, make_game):
    class MixedSeedClient(FakeClient):
        def __init__(
            self,
            pages: list[Dict[str, Any]],
            participants: Dict[int, Dict[str, Any]],
            users: Dict[str, str],
        ):
            super().__init__(pages, participants, users)
            self._seed1_resolve_count = 0

        def fetch_user_by_nickname(self, nickname: str) -> Dict[str, Any]:
            self.fetch_user_by_nickname_calls.append(nickname)
            if nickname == "seed1":
                if self._seed1_resolve_count == 0:
                    uid = "UID-a"
                elif self._seed1_resolve_count == 1:
                    uid = "UID-b"
                else:
                    uid = "UID-a"
                self._seed1_resolve_count += 1
                return {
                    "code": 200,
                    "message": "Success",
                    "user": {
                        "nickname": nickname,
                        "userId": uid,
                    },
                }
            if nickname == "seed2":
                return {
                    "code": 200,
                    "message": "Success",
                    "user": {
                        "nickname": nickname,
                        "userId": "UID-c",
                    },
                }
            return super().fetch_user_by_nickname(nickname)

        def fetch_user_games(
            self, uid: str, next_token: Optional[str] = None
        ) -> Dict[str, Any]:
            self.fetch_user_games_calls.append(next_token)
            self.fetch_user_games_uids.append(uid)
            if uid in {"UID-a", "UID-b"}:
                raise ApiResponseError(
                    code=404,
                    message="User Not Found",
                    payload={"code": 404, "message": "User Not Found"},
                    url=f"https://example.invalid/v1/user/games/uid/{uid}",
                )
            if uid == "UID-c":
                return {"userGames": [make_game(game_id=70, nickname="seed2", uid=uid)]}
            return {"userGames": []}

    client = MixedSeedClient([], {}, {})
    manager = IngestionManager(client, store, fetch_game_details=False)

    manager.ingest_from_seeds(["seed1", "seed2"], depth=0)

    assert client.fetch_user_games_uids[:2] == ["UID-a", "UID-b"]
    assert "UID-c" in client.fetch_user_games_uids


def test_ingest_raises_on_http_404_endpoint_error(store, make_game):
    class HTTP404Client(FakeClient):
        def __init__(
            self,
            pages: list[Dict[str, Any]],
            participants: Dict[int, Dict[str, Any]],
            users: Dict[str, str],
        ):
            super().__init__(pages, participants, users)
            self.stale_uid = "UID-old"

        def fetch_user_games(
            self, uid: str, next_token: Optional[str] = None
        ) -> Dict[str, Any]:
            self.fetch_user_games_calls.append(next_token)
            self.fetch_user_games_uids.append(uid)
            if uid == self.stale_uid:
                response = requests.Response()
                response.status_code = 404
                raise requests.HTTPError(response=response)
            if next_token is None:
                return self.pages[0]
            return self.pages[1]

    pages = [{"userGames": [make_game(game_id=50, nickname="seed")]}]
    client = HTTP404Client(pages, {}, {"seed": "UID-new"})
    manager = IngestionManager(client, store, fetch_game_details=False)

    with pytest.raises(requests.HTTPError):
        manager.ingest_user("UID-old", seed_nickname="seed")

    assert client.fetch_user_games_uids == ["UID-old"]
    assert client.fetch_user_by_nickname_calls == []


def test_ingest_skips_deleted_game(store, make_game):
    users = _generate_uids(["100"])
    deleted_game = make_game(game_id=90, nickname="100", uid=users["100"])
    deleted_game["startDtm"] = "2025-01-01T00:00:00.000+0000"

    store.connection.execute(
        """
        INSERT INTO deleted_matches (game_id, start_dtm, deleted_at, reason)
        VALUES (?, ?, ?, ?)
        """,
        (90, deleted_game["startDtm"], "2025-02-01T00:00:00+00:00", "test"),
    )
    store.connection.commit()

    client = FakeClient(
        pages=[{"userGames": [deleted_game]}], participants={}, users=users
    )
    manager = IngestionManager(client, store, fetch_game_details=True)

    manager.ingest_user(users["100"])

    assert not store.has_game(90)
    assert client.fetch_game_result_calls == []


def test_ingest_stops_at_prune_cutoff(store, make_game):
    users = _generate_uids(["100"])

    newer = make_game(game_id=91, nickname="100", uid=users["100"])
    newer["startDtm"] = "2025-01-02T00:00:00.000+0000"
    older = make_game(game_id=92, nickname="100", uid=users["100"])
    older["startDtm"] = "2024-12-31T00:00:00.000+0000"

    store.set_prune_before("2025-01-01T00:00:00+00:00")

    pages = [{"userGames": [newer, older], "next": "tok"}]
    client = FakeClient(pages, participants={}, users=users)
    manager = IngestionManager(
        client, store, fetch_game_details=False, only_newer_games=False
    )

    manager.ingest_user(users["100"])

    assert store.has_game(91)
    assert not store.has_game(92)
    assert client.fetch_user_games_calls == [None]
