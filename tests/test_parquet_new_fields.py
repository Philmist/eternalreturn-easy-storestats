import pytest

from er_stats.parquet_export import ParquetExporter


pytest.importorskip("pyarrow")


def _write_and_fetch_row(tmp_path, game):
    """Write a single game payload and return the first row as a dict."""

    import pyarrow.parquet as pq

    out = tmp_path / "parquet"
    exp = ParquetExporter(out, flush_rows=1)
    exp.write_from_game_payload(game)
    exp.close()
    files = list((out / "participants").rglob("*.parquet"))
    assert files, "no parquet files were written"
    table = pq.ParquetFile(files[0]).read()
    return table.to_pydict()


def test_mmr_gain_falls_back_to_in_game(tmp_path, make_game):
    game = make_game(game_id=1, nickname="alice", uid="uid-1")
    game.pop("mmrGain", None)
    game["mmrGainInGame"] = 77

    row = _write_and_fetch_row(tmp_path, game)
    assert row["mmr_gain"][0] == 77


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        (
            {
                "isLeavingBeforeCreditRevivalTerminate": False,
                "IsLeavingBeforeCreditRevivalTerminate": True,
            },
            True,
        ),
        (
            {
                "isLeavingBeforeCreditRevivalTerminate": False,
                "IsLeavingBeforeCreditRevivalTerminate": False,
            },
            False,
        ),
        ({}, None),
    ],
)
def test_leaving_flag_prefers_any_true_then_false(tmp_path, make_game, flags, expected):
    game = make_game(game_id=2, nickname="bob", uid="uid-2")
    game.update(flags)

    row = _write_and_fetch_row(tmp_path, game)
    assert row["is_leaving_before_credit_revival_terminate"][0] is expected


def test_equipment_raw_and_maps_are_emitted(tmp_path, make_game):
    equipment = {"0": 999001, "2": 999003}
    equipment_grade = {"0": 3, "2": 5}

    game = make_game(game_id=3, nickname="carol", uid="uid-3")
    game["equipment"] = equipment
    game["equipmentGrade"] = equipment_grade
    game["preMade"] = 1
    game["premadeMatchingType"] = 2

    row = _write_and_fetch_row(tmp_path, game)

    # Map columns round-trip to Python dicts
    assert dict(row["equipment_raw"][0]) == equipment
    assert dict(row["equipment_map"][0]) == equipment
    assert dict(row["equipment_grade_map"][0]) == equipment_grade
    assert row["pre_made"][0] == 1
    assert row["premade_matching_type"][0] == 2
