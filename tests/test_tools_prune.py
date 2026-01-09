from er_stats.db import SQLiteStore
from er_stats.tools_cli import run as tools_run


def test_cli_sqlite_prune_uses_config(tmp_path, make_game):
    db_path = tmp_path / "stats.sqlite"
    store = SQLiteStore(str(db_path))
    store.setup_schema()
    try:
        older = make_game(game_id=201, nickname="Alice", uid="uid-201")
        older["startDtm"] = "2025-01-01T00:00:00.000+0000"
        newer = make_game(game_id=202, nickname="Alice", uid="uid-201")
        newer["startDtm"] = "2025-02-01T00:00:00.000+0000"
        store.upsert_from_game_payload(older)
        store.upsert_from_game_payload(newer)
    finally:
        store.close()

    config_path = tmp_path / "ingest.toml"
    config_path.write_text(
        "\n".join(
            [
                "[ingest]",
                f'db_path = "{db_path.as_posix()}"',
                f'parquet_dir = "{(tmp_path / "parquet").as_posix()}"',
            ]
        )
    )

    cutoff = "2025-01-15T00:00:00+00:00"
    code = tools_run(
        [
            "sqlite-prune",
            "--config",
            str(config_path),
            "--before",
            cutoff,
            "--apply",
        ]
    )
    assert code == 0

    store = SQLiteStore(str(db_path))
    store.setup_schema()
    try:
        assert not store.has_game(201)
        assert store.has_game(202)
        deleted = store.list_deleted_games([201, 202])
        assert deleted == {201}
        assert store.get_prune_before() == cutoff
    finally:
        store.close()
