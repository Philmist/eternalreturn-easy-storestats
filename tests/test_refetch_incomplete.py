import datetime as dt
from typing import Any, Dict, Optional, Set

import pytest
import requests

from er_stats.api_client import ApiResponseError
from er_stats.ingest import IngestionManager


class FakeClient:
    def __init__(
        self,
        participants: Dict[int, Dict[str, Any]],
        users: Dict[str, str],
        *,
        missing_game_ids: Optional[Set[int]] = None,
        missing_payload_game_ids: Optional[Set[int]] = None,
    ) -> None:
        self.participants = participants
        self.users = users
        self.missing_game_ids = set(missing_game_ids or set())
        self.missing_payload_game_ids = set(missing_payload_game_ids or set())
        self.fetch_game_result_calls: list[int] = []
        self.fetch_user_by_nickname_calls: list[str] = []

    def fetch_game_result(self, game_id: int) -> Dict[str, Any]:
        self.fetch_game_result_calls.append(game_id)
        if game_id in self.missing_game_ids:
            response = requests.Response()
            response.status_code = 404
            raise requests.HTTPError(response=response)
        if game_id in self.missing_payload_game_ids:
            raise ApiResponseError(
                code=404,
                message="User Not Found",
                payload={"code": 404, "message": "User Not Found"},
                url=f"https://example.invalid/v1/games/{game_id}",
            )
        return self.participants.get(game_id, {"userGames": []})

    def fetch_user_by_nickname(self, nickname: str) -> Dict[str, Any]:
        self.fetch_user_by_nickname_calls.append(nickname)
        uid = self.users.get(nickname)
        return {
            "code": 200,
            "message": "Success",
            "user": {
                "nickname": nickname,
                "userId": uid,
            },
        }


def _make_manager(client: FakeClient, store) -> IngestionManager:
    return IngestionManager(
        client,
        store,
        uid_recheck_interval=dt.timedelta(days=3650),
        nickname_recheck_interval=dt.timedelta(days=3650),
        max_nickname_attempts=1,
        participant_retry_attempts=1,
    )


def test_refetch_clears_incomplete(store, make_game):
    seed_game = make_game(game_id=1, nickname="seed", uid="UID-seed")
    store.upsert_from_game_payload(seed_game)
    store.mark_game_incomplete(1)

    participant = make_game(game_id=1, nickname="p1")
    client = FakeClient({1: {"userGames": [participant]}}, {"p1": "UID-p1"})
    manager = _make_manager(client, store)

    stats = manager.refetch_incomplete_games([1])

    row = store.connection.execute(
        "SELECT incomplete FROM matches WHERE game_id=?", (1,)
    ).fetchone()
    assert row is not None
    assert row[0] == 0
    assert stats["cleared"] == 1


def test_refetch_keeps_incomplete_on_empty_participants(store, make_game):
    seed_game = make_game(game_id=2, nickname="seed", uid="UID-seed")
    store.upsert_from_game_payload(seed_game)
    store.mark_game_incomplete(2)

    client = FakeClient({2: {"userGames": []}}, {})
    manager = _make_manager(client, store)

    stats = manager.refetch_incomplete_games([2])

    row = store.connection.execute(
        "SELECT incomplete FROM matches WHERE game_id=?", (2,)
    ).fetchone()
    assert row is not None
    assert row[0] == 1
    assert stats["empty"] == 1


def test_refetch_keeps_incomplete_on_payload_404(store, make_game):
    seed_game = make_game(game_id=3, nickname="seed", uid="UID-seed")
    store.upsert_from_game_payload(seed_game)
    store.mark_game_incomplete(3)

    client = FakeClient({}, {}, missing_payload_game_ids={3})
    manager = _make_manager(client, store)

    stats = manager.refetch_incomplete_games([3])

    row = store.connection.execute(
        "SELECT incomplete FROM matches WHERE game_id=?", (3,)
    ).fetchone()
    assert row is not None
    assert row[0] == 1
    assert stats["not_found"] == 1
    status_row = store.connection.execute(
        "SELECT status, next_refetch_at FROM match_refetch_status WHERE game_id=?",
        (3,),
    ).fetchone()
    assert status_row is not None
    assert status_row["status"] == "missing"
    assert status_row["next_refetch_at"] is not None


def test_refetch_raises_on_http_404_endpoint_error(store, make_game):
    seed_game = make_game(game_id=13, nickname="seed", uid="UID-seed")
    store.upsert_from_game_payload(seed_game)
    store.mark_game_incomplete(13)

    client = FakeClient({}, {}, missing_game_ids={13})
    manager = _make_manager(client, store)

    with pytest.raises(requests.HTTPError):
        manager.refetch_incomplete_games([13])


def test_list_refetch_candidates_filters(store, make_game):
    game_a = make_game(
        game_id=10,
        nickname="a",
        uid="UID-a",
        season_id=25,
        matching_mode=3,
        matching_team_mode=1,
    )
    game_b = make_game(
        game_id=11,
        nickname="b",
        uid="UID-b",
        season_id=26,
        matching_mode=2,
        matching_team_mode=3,
    )
    store.upsert_from_game_payload(game_a)
    store.upsert_from_game_payload(game_b)
    store.mark_game_incomplete(10)
    store.mark_game_incomplete(11)

    only_season = store.list_refetch_candidates(season_id=25)
    assert only_season == [10]

    only_mode = store.list_refetch_candidates(matching_mode=2)
    assert only_mode == [11]


def test_list_refetch_candidates_include_missing(store, make_game):
    game = make_game(game_id=12, nickname="c", uid="UID-c")
    store.upsert_from_game_payload(game)
    store.mark_game_incomplete(12)
    store.upsert_refetch_status(
        12,
        status="missing",
        attempts=1,
        last_refetch_at="2025-01-01T00:00:00+00:00",
        next_refetch_at="2025-01-02T00:00:00+00:00",
        last_error="http_404",
    )

    now = "2025-01-03T00:00:00+00:00"
    excluded = store.list_refetch_candidates(now=now)
    assert excluded == []

    included = store.list_refetch_candidates(include_missing=True, now=now)
    assert included == [12]
