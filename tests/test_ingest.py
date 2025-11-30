import datetime as dt
from typing import Any, Dict, Optional

import requests

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


def test_ingest_retries_uid_on_404_using_nickname(store, make_game):
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

    manager.ingest_user("UID-old", seed_nickname="seed")

    # First attempt with stale UID, then retry with fresh one
    assert client.fetch_user_games_uids == ["UID-old", "UID-new"]
    assert client.fetch_user_by_nickname_calls == ["seed"]
