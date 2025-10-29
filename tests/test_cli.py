import json

from er_stats.cli import run


def test_cli_character_outputs_json(store, tmp_path, make_game, capsys):
    # Pre-populate DB with one record matching the context
    store.upsert_from_game_payload(make_game(game_id=1, user_num=1, character_num=1, game_rank=2))

    code = run([
        "--db", store.path,
        "character",
        "--season", "25",
        "--server", "NA",
        "--mode", "3",
        "--team-mode", "1",
    ])
    assert code == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data and "character_num" in data[0]

