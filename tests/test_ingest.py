from typing import Any, Dict, Optional

from er_stats.ingest import IngestionManager


class FakeClient:
    def __init__(
        self,
        pages: list[Dict[str, Any]],
        participants: Dict[int, Dict[str, Any]],
        users: Dict[str, str],
    ) -> None:
        self.pages = pages
        self.participants = participants
        self.users = users
        self.fetch_user_games_calls: list[Optional[str]] = []
        self.fetch_user_games_uids: list[str] = []
        self.fetch_game_result_calls: list[int] = []

    def fetch_user_by_nickname(self, nickname: str) -> Dict[str, Any]:
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

    # Discovered users from participants
    assert {users["200"], users["201"], users["300"]}.issubset(discovered)
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

    assert {i for i in [users["200"], users["201"]]}.issubset(discovered)
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
