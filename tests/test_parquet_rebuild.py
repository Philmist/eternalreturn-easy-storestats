from typing import Any, Dict

import pytest

from er_stats.parquet_export import (
    MATCH_SCHEMA,
    PARTICIPANT_SCHEMA,
    ParquetExporter,
)
from er_stats.tools_cli import run as tools_run


pytest.importorskip("pyarrow")


def _write_row(
    path,
    schema,
    row: Dict[str, Any],
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table(
        {name: [row.get(name)] for name in schema.names},
        schema=schema,
    )
    pq.write_table(table, path)


def test_cli_parquet_rebuild_dedupes_and_aligns(tmp_path, make_game):
    src = tmp_path / "src"
    dst = tmp_path / "dst"

    exp = ParquetExporter(src, flush_rows=1)
    game = make_game(game_id=1, nickname="Alice", uid="uid-1")
    exp.write_from_game_payload(game)
    exp.close()

    match_dup = {name: None for name in MATCH_SCHEMA.names}
    match_dup["game_id"] = 1
    match_dir = (
        src
        / "matches"
        / "season_id=null"
        / "server_name=null"
        / "matching_mode=null"
        / "date=null"
    )
    match_dir.mkdir(parents=True, exist_ok=True)
    _write_row(match_dir / "matches-dup.parquet", MATCH_SCHEMA, match_dup)

    participant_dup = {name: None for name in PARTICIPANT_SCHEMA.names}
    participant_dup["game_id"] = 1
    participant_dup["uid"] = "uid-1"
    participant_dup["nickname"] = "Alice"
    participant_dup["season_id"] = 999
    participant_dup["matching_mode"] = 99
    participant_dup["matching_team_mode"] = 9
    participant_dup["server_name"] = "XX"
    participant_dir = (
        src
        / "participants"
        / "season_id=999"
        / "server_name=XX"
        / "matching_mode=99"
        / "date=2099-01-01"
    )
    participant_dir.mkdir(parents=True, exist_ok=True)
    _write_row(
        participant_dir / "participants-dup.parquet",
        PARTICIPANT_SCHEMA,
        participant_dup,
    )

    code = tools_run(
        [
            "parquet-rebuild",
            "--src",
            str(src),
            "--dst",
            str(dst),
            "--compression",
            "zstd",
            "--max-rows-per-file",
            "100000",
        ]
    )
    assert code == 0

    import pyarrow.dataset as ds

    matches = ds.dataset(str(dst / "matches"), format="parquet", partitioning="hive")
    match_rows = matches.to_table().to_pylist()
    assert len(match_rows) == 1
    match_row = match_rows[0]
    assert match_row["season_id"] == 25
    assert match_row["server_name"] == "NA"
    assert match_row["matching_mode"] == 3
    assert match_row["matching_team_mode"] == 1
    assert match_row["start_dtm"]

    participants = ds.dataset(
        str(dst / "participants"), format="parquet", partitioning="hive"
    )
    participant_rows = participants.to_table().to_pylist()
    assert len(participant_rows) == 1
    participant_row = participant_rows[0]
    assert participant_row["uid"] == "uid-1"
    assert participant_row["season_id"] == 25
    assert participant_row["server_name"] == "NA"
    assert participant_row["matching_mode"] == 3
    assert participant_row["matching_team_mode"] == 1
