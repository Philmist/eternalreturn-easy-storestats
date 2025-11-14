from typing import Any, Dict, Optional

from er_stats.cli import run as cli_run


class _FakeClient:
    def __init__(self, pages: list[Dict[str, Any]], participants: Dict[int, Dict[str, Any]], mapping: Dict[str, int]):
        self.pages = pages
        self.participants = participants
        self.mapping = mapping

    def fetch_user_by_nickname(self, nickname: str) -> Dict[str, Any]:
        user_num = self.mapping.get(nickname)
        if user_num is None:
            raise RuntimeError("user not found")
        return {"code": 200, "message": "Success", "user": {"userNum": user_num, "nickname": nickname}}

    def fetch_user_games(self, user_num: int, next_token: Optional[str] = None) -> Dict[str, Any]:
        if next_token is None:
            return self.pages[0]
        return self.pages[1]

    def fetch_game_result(self, game_id: int) -> Dict[str, Any]:
        return self.participants.get(game_id, {"userGames": []})

    def close(self) -> None:
        return None


def _make_pages(make_game, seed_user: int):
    g1 = make_game(game_id=1, user_num=seed_user)
    g2 = make_game(game_id=2, user_num=seed_user)
    pages = [
        {"userGames": [g1], "next": "tok"},
        {"userGames": [g2]},
    ]
    p1_a = make_game(game_id=1, user_num=200)
    p1_b = make_game(game_id=1, user_num=201)
    p2_a = make_game(game_id=2, user_num=300)
    participants = {
        1: {"userGames": [p1_a, p1_b]},
        2: {"userGames": [p2_a]},
    }
    return pages, participants


def test_cli_ingest_with_nickname(monkeypatch, store, make_game, tmp_path):
    # Monkeypatch the client used by CLI to our fake that can resolve nicknames
    from er_stats import cli as cli_mod

    seed_user = 500
    pages, participants = _make_pages(make_game, seed_user)

    def _fake_ctor(base_url: str, api_key: Optional[str] = None, session=None, timeout: float = 10.0, *, min_interval: float = 1.0, max_retries: int = 3):
        return _FakeClient(pages, participants, {"Alice": seed_user})

    monkeypatch.setattr(cli_mod, "EternalReturnAPIClient", _fake_ctor)

    args = [
        "--db", store.path,
        "ingest",
        "--base-url", "https://example.invalid",
        "--nickname", "Alice",
        "--depth", "1",
        "--min-interval", "0.0",
    ]

    code = cli_run(args)
    assert code == 0

    # Verify DB was populated by CLI ingest
    cur = store.connection.execute("SELECT COUNT(*) FROM matches")
    assert cur.fetchone()[0] == 2
    cur = store.connection.execute("SELECT COUNT(*) FROM user_match_stats")
    # 2 rows from seed user's games + 3 rows from participants
    assert cur.fetchone()[0] == 5

