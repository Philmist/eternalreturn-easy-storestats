from typing import Any, Dict, Optional

import pytest

from er_stats.ingest import IngestionManager
from er_stats.parquet_export import ParquetExporter


pytest.importorskip("pyarrow")


class FakeClient:
    def __init__(
        self, pages: list[Dict[str, Any]], participants: Dict[int, Dict[str, Any]]
    ):
        self.pages = pages
        self.participants = participants

    def fetch_user_games(
        self, user_num: int, next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        if next_token is None:
            return self.pages[0]
        return self.pages[1]

    def fetch_game_result(self, game_id: int) -> Dict[str, Any]:
        return self.participants.get(game_id, {"userGames": []})

    def close(self) -> None:
        return None


def _prepare_pages(make_game):
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


def test_ingestion_manager_writes_sqlite_and_parquet(store, tmp_path, make_game):
    pages, participants = _prepare_pages(make_game)
    client = FakeClient(pages, participants)

    out_dir = tmp_path / "parquet"
    exporter = ParquetExporter(out_dir)
    manager = IngestionManager(client, store, parquet_exporter=exporter)

    discovered = manager.ingest_user(100)
    # Ensure buffered Parquet rows are flushed
    exporter.close()
    assert {200, 201, 300}.issubset(discovered)

    cur = store.connection.execute("SELECT COUNT(*) FROM matches")
    assert cur.fetchone()[0] == 2
    cur = store.connection.execute("SELECT COUNT(*) FROM user_match_stats")
    assert cur.fetchone()[0] == 5

    # Parquet files exist under both datasets
    matches_files = list((out_dir / "matches").rglob("*.parquet"))
    participants_files = list((out_dir / "participants").rglob("*.parquet"))
    assert matches_files
    assert participants_files

    import pyarrow.parquet as pq

    # Row counts should match unique rows written (use metadata to avoid schema merge issues)
    matches_rows = sum(pq.ParquetFile(p).metadata.num_rows for p in matches_files)
    participants_rows = sum(
        pq.ParquetFile(p).metadata.num_rows for p in participants_files
    )
    assert matches_rows == 2
    assert participants_rows == 5

    # infer schema
    schema = pq.read_schema(participants_files[0])

    # Validate expected columns exist in participants (no partition columns inside file)
    t = pq.read_table(participants_files[0], schema=schema)
    cols = set(t.column_names)
    assert {"game_id", "user_num", "character_num", "game_rank"}.issubset(cols)

    # Verify hive partition directories (season/server/mode/date), and no matching_team_mode
    any_participant = participants_files[0]
    dir_names = {p.name for p in any_participant.parents}
    assert "season_id=25" in dir_names
    assert "server_name=NA" in dir_names
    assert "matching_mode=3" in dir_names
    assert any(n.startswith("date=") for n in dir_names)
    assert not any(n.startswith("matching_team_mode=") for n in dir_names)


def test_schema_has_no_raw_json(store):
    # Ensure fresh DB schema does not include deprecated raw_json columns
    cols_matches = [
        r[1] for r in store.connection.execute("PRAGMA table_info(matches)").fetchall()
    ]
    cols_ums = [
        r[1]
        for r in store.connection.execute(
            "PRAGMA table_info(user_match_stats)"
        ).fetchall()
    ]
    assert "raw_json" not in cols_matches
    assert "raw_json" not in cols_ums
