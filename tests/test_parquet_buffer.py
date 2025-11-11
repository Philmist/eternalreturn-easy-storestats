import os
from pathlib import Path
from typing import Any, Dict

import pytest

from er_stats.parquet_export import ParquetExporter
from er_stats.ingest import IngestionManager


pytest.importorskip("pyarrow")


def _make_participant(make_game, *, game_id: int, user_num: int) -> Dict[str, Any]:
    return make_game(game_id=game_id, user_num=user_num)


def test_exporter_buffers_and_flushes(tmp_path, make_game):
    # Use small flush size to force multiple files within a single partition
    out = tmp_path / "parquet"
    exp = ParquetExporter(out, flush_rows=2)

    # Five rows in the same partition (season/server/mode/date)
    rows = [
        _make_participant(make_game, game_id=1, user_num=100+i) for i in range(5)
    ]
    for r in rows:
        exp.write_from_game_payload(r)
    exp.close()

    # Expect ceil(5/2)=3 files in the participants partition
    participants_files = list((out / "participants").rglob("*.parquet"))
    assert len(participants_files) == 3

    # Validate total rows read back
    import pyarrow.parquet as pq

    total = sum(pq.read_table(p).num_rows for p in participants_files)
    assert total == 5


def test_cli_parquet_compact_merges_small_files(monkeypatch, tmp_path, make_game, store):
    # Prepare a tiny dataset with many small files using the exporter
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    exp = ParquetExporter(src, flush_rows=1)  # one row per file to start

    # Create three rows in one partition via ingest manager to exercise path
    pages = [{"userGames": [make_game(game_id=1, user_num=100), make_game(game_id=2, user_num=100)]}]
    participants = {1: {"userGames": [make_game(game_id=1, user_num=200)]}}

    class _FakeClient:
        def __init__(self, pages, participants):
            self.pages = pages
            self.participants = participants

        def fetch_user_games(self, user_num, next_token=None):
            return self.pages[0]

        def fetch_game_result(self, game_id):
            return self.participants.get(game_id, {"userGames": []})

        def close(self):
            return None

    client = _FakeClient(pages, participants)
    manager = IngestionManager(client, store, parquet_exporter=exp)
    manager.ingest_user(100)
    exp.close()

    # Sanity: many small files exist at src
    small_files = list((src / "participants").rglob("*.parquet"))
    assert len(small_files) >= 3

    # Run compaction CLI
    from er_stats.cli import run as cli_run
    code = cli_run([
        "--db", store.path,
        "parquet-compact",
        "--src", str(src / "participants"),
        "--dst", str(dst / "participants"),
        "--compression", "zstd",
        "--max-rows-per-file", "100000",
    ])
    assert code == 0

    # After compaction, expect fewer files and same row count
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds

    out_files = list((dst / "participants").rglob("*.parquet"))
    assert len(out_files) <= len(small_files)

    # Count rows via dataset read
    dset = ds.dataset(str(dst / "participants"), format="parquet", partitioning="hive")
    total_rows = sum(fragment.count_rows() for fragment in dset.get_fragments())
    # Original had at least 3 participant rows
    assert total_rows >= 3

