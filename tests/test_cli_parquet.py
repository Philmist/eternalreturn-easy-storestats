import json
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from er_stats.cli import run as cli_run


pytest.importorskip("pyarrow")


class _FakeClient:
    def __init__(self, pages: list[Dict[str, Any]], participants: Dict[int, Dict[str, Any]]):
        self.pages = pages
        self.participants = participants

    def fetch_user_games(self, user_num: int, next_token: Optional[str] = None) -> Dict[str, Any]:
        if next_token is None:
            return self.pages[0]
        return self.pages[1]

    def fetch_game_result(self, game_id: int) -> Dict[str, Any]:
        return self.participants.get(game_id, {"userGames": []})

    def close(self) -> None:
        return None


def _make_pages(make_game):
    g1 = make_game(game_id=1, user_num=100)
    g2 = make_game(game_id=2, user_num=100)
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


def test_cli_ingest_with_parquet_dir(monkeypatch, store, tmp_path, make_game):
    # Monkeypatch the client used by CLI to our fake
    from er_stats import cli as cli_mod

    pages, participants = _make_pages(make_game)

    def _fake_ctor(base_url: str, api_key: Optional[str] = None, session=None, timeout: float = 10.0, *, min_interval: float = 1.0, max_retries: int = 3):
        return _FakeClient(pages, participants)

    monkeypatch.setattr(cli_mod, "EternalReturnAPIClient", _fake_ctor)

    out_dir = tmp_path / "out_parquet"
    args = [
        "--db", store.path,
        "ingest",
        "--base-url", "https://example.invalid",
        "--user", "12345",
        "--depth", "1",
        "--parquet-dir", str(out_dir),
        "--min-interval", "0.0",
    ]

    code = cli_run(args)
    assert code == 0

    # Verify DB was populated by CLI ingest
    cur = store.connection.execute("SELECT COUNT(*) FROM matches")
    assert cur.fetchone()[0] == 2
    cur = store.connection.execute("SELECT COUNT(*) FROM user_match_stats")
    assert cur.fetchone()[0] == 5

    # Parquet datasets were written
    matches_files = list((out_dir / "matches").rglob("*.parquet"))
    participants_files = list((out_dir / "participants").rglob("*.parquet"))
    assert matches_files and participants_files
    # Ensure partition path excludes matching_team_mode
    dir_names = {p.name for p in participants_files[0].parents}
    assert "season_id=25" in dir_names
    assert "server_name=NA" in dir_names
    assert "matching_mode=3" in dir_names
    assert any(n.startswith("date=") for n in dir_names)
    assert not any(n.startswith("matching_team_mode=") for n in dir_names)
