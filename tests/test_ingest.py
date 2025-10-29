from typing import Any, Dict, Optional

from er_stats.ingest import IngestionManager


class FakeClient:
    def __init__(self, pages: list[Dict[str, Any]], participants: Dict[int, Dict[str, Any]]):
        self.pages = pages
        self.participants = participants
        self.calls = 0

    def fetch_user_games(self, user_num: int, next_token: Optional[str] = None) -> Dict[str, Any]:
        if next_token is None:
            return self.pages[0]
        return self.pages[1]

    def fetch_game_result(self, game_id: int) -> Dict[str, Any]:
        return self.participants.get(game_id, {"userGames": []})


def test_ingest_user_and_participants(store, make_game):
    # Prepare two pages of userGames
    g1 = make_game(game_id=1, user_num=100)
    g2 = make_game(game_id=2, user_num=100)
    pages = [
        {"userGames": [g1], "next": "tok"},
        {"userGames": [g2]},
    ]

    # Participants for each game introduce new users
    p1_a = make_game(game_id=1, user_num=200)
    p1_b = make_game(game_id=1, user_num=201)
    p2_a = make_game(game_id=2, user_num=300)
    participants = {
        1: {"userGames": [p1_a, p1_b]},
        2: {"userGames": [p2_a]},
    }

    client = FakeClient(pages, participants)
    manager = IngestionManager(client, store, max_games_per_user=None, fetch_game_details=True)

    discovered = manager.ingest_user(100)

    # Discovered users from participants
    assert {200, 201, 300}.issubset(discovered)

    # Data persisted for seed and participants
    count = store.connection.execute("SELECT COUNT(*) FROM user_match_stats").fetchone()[0]
    assert count == 5  # 2 seed matches + 3 participant entries

