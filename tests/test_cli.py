import json
from typing import Any, Dict, Optional

import pytest

from er_stats.cli import run


class _DummyClient:
    last_instance: Optional["_DummyClient"] = None

    def __init__(
        self,
        base_url: str,
        api_key=None,
        session=None,
        timeout: float = 10.0,
        *,
        min_interval: float = 1.0,
        max_retries: int = 3,
    ) -> None:
        self.base_url = base_url
        self.api_key = api_key
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.fetch_character_attributes_calls = 0
        self.fetch_item_armor_calls = 0
        self.fetch_item_weapon_calls = 0
        self._characters_payload: Dict[str, Any] = {
            "data": [
                {"characterCode": 1, "character": "Jackie"},
                {"characterCode": 2, "character": "Aya"},
            ]
        }
        self._item_armor_payload: Dict[str, Any] = {
            "data": [
                {
                    "code": 201101,
                    "name": "Basic Helmet",
                    "modeType": 0,
                    "itemType": "Armor",
                    "itemGrade": "Common",
                    "isCompletedItem": False,
                }
            ]
        }
        self._item_weapon_payload: Dict[str, Any] = {
            "data": [
                {
                    "code": 101101,
                    "name": "Basic Sword",
                    "modeType": 0,
                    "itemType": "Weapon",
                    "itemGrade": "Common",
                    "isCompletedItem": True,
                }
            ]
        }
        _DummyClient.last_instance = self

    def close(self) -> None:
        return None

    def fetch_character_attributes(self) -> Dict[str, Any]:
        self.fetch_character_attributes_calls += 1
        return self._characters_payload

    def fetch_item_armor(self) -> Dict[str, Any]:
        self.fetch_item_armor_calls += 1
        return self._item_armor_payload

    def fetch_item_weapon(self) -> Dict[str, Any]:
        self.fetch_item_weapon_calls += 1
        return self._item_weapon_payload


def test_cli_character_outputs_json(store, tmp_path, make_game, capsys):
    # Pre-populate DB with one record matching the context
    store.upsert_from_game_payload(
        make_game(game_id=1, user_num=1, character_num=1, game_rank=2)
    )
    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
        ]
    )

    code = run(
        [
            "--db",
            store.path,
            "stats",
            "character",
            "--season",
            "25",
            "--server",
            "NA",
            "--mode",
            "3",
            "--team-mode",
            "1",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data
    assert data[0]["character_name"] == "Jackie"


def test_cli_character_aggregations_match_expected(store, make_game, capsys):
    # Reuse the three-match, two-team, team-of-three scenario with team_mode=3.
    def add_player(
        game_id: int,
        user_num: int,
        character_num: int,
        game_rank: int,
        team_number: int,
    ) -> None:
        game = make_game(
            game_id=game_id,
            user_num=user_num,
            character_num=character_num,
            game_rank=game_rank,
            matching_team_mode=3,
        )
        game["teamNumber"] = team_number
        store.upsert_from_game_payload(game)

    # Game 1: team 1 rank 1, team 2 rank 2
    add_player(1, 101, 1, 1, 1)
    add_player(1, 102, 2, 1, 1)
    add_player(1, 103, 9, 1, 1)
    add_player(1, 104, 3, 2, 2)
    add_player(1, 105, 4, 2, 2)
    add_player(1, 106, 5, 2, 2)

    # Game 2: team 1 rank 1, team 2 rank 2
    add_player(2, 201, 1, 1, 1)
    add_player(2, 202, 2, 1, 1)
    add_player(2, 203, 9, 1, 1)
    add_player(2, 204, 3, 2, 2)
    add_player(2, 205, 4, 2, 2)
    add_player(2, 206, 5, 2, 2)

    # Game 3: team 1 rank 2, team 2 rank 1
    add_player(3, 301, 1, 2, 1)
    add_player(3, 302, 2, 2, 1)
    add_player(3, 303, 9, 2, 1)
    add_player(3, 304, 3, 1, 2)
    add_player(3, 305, 4, 1, 2)
    add_player(3, 306, 5, 1, 2)

    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
            {"characterCode": 2, "character": "Aya"},
            {"characterCode": 3, "character": "Hyunwoo"},
            {"characterCode": 4, "character": "Fiora"},
            {"characterCode": 5, "character": "Zahir"},
            {"characterCode": 9, "character": "Nadine"},
        ]
    )

    code = run(
        [
            "--db",
            store.path,
            "stats",
            "character",
            "--season",
            "25",
            "--server",
            "NA",
            "--mode",
            "3",
            "--team-mode",
            "3",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    rows = json.loads(out)

    by_char = {row["character_num"]: row for row in rows}

    jackie = by_char[1]
    assert jackie["character_name"] == "Jackie"
    assert jackie["matches"] == 3
    assert jackie["rank_1"] == 2
    assert jackie["rank_2_3"] == 1
    assert jackie["rank_4_6"] == 0
    assert jackie["average_rank"] == pytest.approx(4 / 3)

    hyunwoo = by_char[3]
    assert hyunwoo["character_name"] == "Hyunwoo"
    assert hyunwoo["matches"] == 3
    assert hyunwoo["rank_1"] == 1
    assert hyunwoo["rank_2_3"] == 2
    assert hyunwoo["rank_4_6"] == 0
    assert hyunwoo["average_rank"] == pytest.approx(5 / 3)


def test_cli_character_time_filter_via_args(store, make_game, capsys):
    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
            {"characterCode": 2, "character": "Aya"},
        ]
    )

    early = make_game(
        game_id=1001, user_num=1, character_num=1, game_rank=1, season_id=25
    )
    early["startDtm"] = "2025-11-24T00:00:00+00:00"
    store.upsert_from_game_payload(early)

    late = make_game(
        game_id=1002, user_num=2, character_num=2, game_rank=2, season_id=25
    )
    late["startDtm"] = "2025-11-25T00:00:00+00:00"
    store.upsert_from_game_payload(late)

    code = run(
        [
            "--db",
            store.path,
            "stats",
            "character",
            "--server",
            "NA",
            "--mode",
            "3",
            "--team-mode",
            "1",
            "--season",
            "25",
            "--start-dtm",
            "2025-11-24T12:00:00+00:00",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    rows = json.loads(out)
    assert {row["character_num"] for row in rows} == {2}


def test_cli_patch_latest_picks_highest_version(store, make_game, capsys):
    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
            {"characterCode": 2, "character": "Aya"},
        ]
    )
    g1 = make_game(
        game_id=2001,
        user_num=10,
        character_num=1,
        game_rank=1,
        season_id=25,
    )
    g1["versionMajor"] = 1
    store.upsert_from_game_payload(g1)

    g2 = make_game(
        game_id=2002,
        user_num=11,
        character_num=2,
        game_rank=2,
        season_id=26,
    )
    g2["versionMajor"] = 2
    store.upsert_from_game_payload(g2)

    code = run(
        [
            "--db",
            store.path,
            "stats",
            "character",
            "--server",
            "NA",
            "--mode",
            "3",
            "--team-mode",
            "1",
            "--patch",
            "latest",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    rows = json.loads(out)
    assert {row["character_num"] for row in rows} == {2}


def test_cli_patch_and_season_conflict_returns_error(store):
    code = run(
        [
            "--db",
            store.path,
            "stats",
            "character",
            "--server",
            "NA",
            "--mode",
            "3",
            "--team-mode",
            "1",
            "--season",
            "25",
            "--patch",
            "26.1",
        ]
    )
    assert code == 2


def test_cli_range_conflicts_with_explicit_window(store):
    code = run(
        [
            "--db",
            store.path,
            "stats",
            "character",
            "--server",
            "NA",
            "--mode",
            "3",
            "--team-mode",
            "1",
            "--season",
            "25",
            "--start-dtm",
            "2025-01-01T00:00:00+00:00",
            "--range",
            "last:1d",
        ]
    )
    assert code == 2


def test_cli_invalid_range_returns_error(store):
    code = run(
        [
            "--db",
            store.path,
            "stats",
            "character",
            "--server",
            "NA",
            "--mode",
            "3",
            "--team-mode",
            "1",
            "--season",
            "25",
            "--range",
            "last:xd",
        ]
    )
    assert code == 2


def test_cli_equipment_aggregations_match_expected(store, make_game, capsys):
    # Two matches; item 101101 is used twice, 101102 once.
    game1 = make_game(
        game_id=1,
        user_num=1,
        character_num=1,
        game_rank=1,
        matching_team_mode=3,
    )
    game1["teamNumber"] = 1
    store.upsert_from_game_payload(game1)

    game2 = make_game(
        game_id=2,
        user_num=2,
        character_num=2,
        game_rank=3,
        matching_team_mode=3,
    )
    # Only equip item 101101 in game2 so it has two uses, 101102 has one.
    game2["equipment"] = {"0": 101101}
    game2["equipmentGrade"] = {"0": 2}
    game2["teamNumber"] = 1
    store.upsert_from_game_payload(game2)

    store.refresh_items(
        [
            {
                "code": 101101,
                "name": "Basic Sword",
                "modeType": 0,
                "itemType": "Weapon",
                "itemGrade": "Common",
                "isCompletedItem": True,
            },
            {
                "code": 101102,
                "name": "Basic Armor",
                "modeType": 0,
                "itemType": "Armor",
                "itemGrade": "Common",
                "isCompletedItem": False,
            },
        ]
    )

    # With min-samples=1 both items should appear.
    code = run(
        [
            "--db",
            store.path,
            "stats",
            "equipment",
            "--season",
            "25",
            "--server",
            "NA",
            "--mode",
            "3",
            "--team-mode",
            "3",
            "--min-samples",
            "1",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    rows = json.loads(out)
    by_item = {row["item_id"]: row for row in rows}

    sword = by_item[101101]
    assert sword["item_name"] == "Basic Sword"
    assert sword["usage_count"] == 2
    assert sword["average_rank"] == pytest.approx(2.0)

    armor = by_item[101102]
    assert armor["item_name"] == "Basic Armor"
    assert armor["usage_count"] == 1
    assert armor["average_rank"] == pytest.approx(1.0)

    # With min-samples=2 only the sword should remain.
    code = run(
        [
            "--db",
            store.path,
            "stats",
            "equipment",
            "--season",
            "25",
            "--server",
            "NA",
            "--mode",
            "3",
            "--team-mode",
            "3",
            "--min-samples",
            "2",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    rows = json.loads(out)
    ids = {row["item_id"] for row in rows}
    assert ids == {101101}


def test_cli_bot_aggregations_match_expected(store, make_game, capsys):
    # BotA (user 1001, Jackie) plays three matches with ranks [1, 1, 3].
    # BotB (user 1004, Fiora) plays two matches with ranks [2, 1].
    # BotC (user 1007, LiDailin) plays one match with rank [1].
    # A fourth match with a different season_id should be ignored.

    def add_player(
        game_id: int,
        user_num: int,
        character_num: int,
        game_rank: int,
        team_number: int,
        *,
        mlbot: bool | None = None,
        season_id: int = 25,
    ) -> None:
        game = make_game(
            game_id=game_id,
            user_num=user_num,
            character_num=character_num,
            game_rank=game_rank,
            matching_team_mode=3,
            season_id=season_id,
            mlbot=mlbot,
        )
        game["teamNumber"] = team_number
        store.upsert_from_game_payload(game)

    # Game 1 (season 25)
    add_player(1, 1001, 1, 1, 1, mlbot=True)
    add_player(1, 1002, 2, 1, 1)
    add_player(1, 1003, 3, 1, 1)
    add_player(1, 1004, 4, 2, 2, mlbot=True)
    add_player(1, 1005, 5, 2, 2)
    add_player(1, 1006, 6, 2, 2)

    # Game 2 (season 25)
    add_player(2, 2001, 2, 2, 1)
    add_player(2, 2002, 3, 2, 1)
    add_player(2, 2003, 6, 2, 1)
    add_player(2, 1001, 1, 1, 2, mlbot=True)
    add_player(2, 1004, 4, 1, 2, mlbot=True)
    add_player(2, 2006, 5, 1, 2)

    # Game 3 (season 25)
    add_player(3, 1001, 1, 3, 1, mlbot=True)
    add_player(3, 3002, 2, 3, 1)
    add_player(3, 3003, 3, 3, 1)
    add_player(3, 1007, 7, 1, 2, mlbot=True)
    add_player(3, 3005, 5, 1, 2)
    add_player(3, 3006, 6, 1, 2)

    # Game 4: different season, should be ignored.
    add_player(4, 1001, 1, 1, 1, mlbot=True, season_id=26)

    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
            {"characterCode": 4, "character": "Fiora"},
            {"characterCode": 7, "character": "LiDailin"},
        ]
    )

    # With min-matches=2, Jackie and Fiora should appear, LiDailin should not.
    code = run(
        [
            "--db",
            store.path,
            "stats",
            "bot",
            "--season",
            "25",
            "--server",
            "NA",
            "--mode",
            "3",
            "--team-mode",
            "3",
            "--min-matches",
            "2",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    rows = json.loads(out)
    keys = {row["character_num"] for row in rows}
    assert keys == {1, 4}

    bot_a = next(row for row in rows if row["character_num"] == 1)
    assert bot_a["ml_bot"] == 1
    assert bot_a["character_name"] == "Jackie"
    assert bot_a["matches"] == 3
    assert bot_a["average_rank"] == pytest.approx(5 / 3)

    bot_b = next(row for row in rows if row["character_num"] == 4)
    assert bot_b["ml_bot"] == 1
    assert bot_b["character_name"] == "Fiora"
    assert bot_b["matches"] == 2
    assert bot_b["average_rank"] == pytest.approx(1.5)

    # With min-matches=3 only BotA should remain.
    code = run(
        [
            "--db",
            store.path,
            "stats",
            "bot",
            "--season",
            "25",
            "--server",
            "NA",
            "--mode",
            "3",
            "--team-mode",
            "3",
            "--min-matches",
            "3",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    rows = json.loads(out)
    keys = {row["character_num"] for row in rows}
    assert keys == {1}
    only_bot = rows[0]
    assert only_bot["matches"] == 3
    assert only_bot["average_rank"] == pytest.approx(5 / 3)


def test_cli_mmr_aggregations_match_expected(store, make_game, capsys):
    # Single character with three matches and varying MMR gain.
    store.upsert_from_game_payload(
        make_game(
            game_id=1,
            user_num=1,
            character_num=1,
            game_rank=2,
            matching_team_mode=3,
            mmr_gain=10,
        )
    )
    store.upsert_from_game_payload(
        make_game(
            game_id=2,
            user_num=2,
            character_num=1,
            game_rank=1,
            matching_team_mode=3,
            mmr_gain=20,
        )
    )
    store.upsert_from_game_payload(
        make_game(
            game_id=3,
            user_num=3,
            character_num=1,
            game_rank=3,
            matching_team_mode=3,
            mmr_gain=-5,
        )
    )

    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
        ]
    )

    code = run(
        [
            "--db",
            store.path,
            "stats",
            "mmr",
            "--season",
            "25",
            "--server",
            "NA",
            "--mode",
            "3",
            "--team-mode",
            "3",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    rows = json.loads(out)
    assert rows

    jackie = next(row for row in rows if row["character_num"] == 1)
    assert jackie["character_name"] == "Jackie"
    assert jackie["matches"] == 3
    assert jackie["avg_mmr_gain"] == pytest.approx((10 + 20 - 5) / 3)
    # Default mmrLossEntryCost in the payload is 5 for all matches.
    assert jackie["avg_entry_cost"] == pytest.approx(5.0)


def test_cli_mode_accepts_string_and_infers_team_mode(store, make_game, capsys):
    store.upsert_from_game_payload(
        make_game(
            game_id=1,
            user_num=1,
            character_num=1,
            game_rank=1,
            matching_mode=6,
            matching_team_mode=4,
        )
    )
    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
        ]
    )

    code = run(
        [
            "--db",
            store.path,
            "stats",
            "character",
            "--season",
            "25",
            "--server",
            "NA",
            "--mode",
            "CoBaLt",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    rows = json.loads(out)
    assert any(row["character_num"] == 1 for row in rows)


def test_cli_default_season_ranked_uses_latest(store, make_game, capsys):
    store.upsert_from_game_payload(
        make_game(
            game_id=1,
            user_num=1,
            character_num=1,
            game_rank=1,
            matching_team_mode=3,
            season_id=1,
            mmr_gain=10,
        )
    )
    store.upsert_from_game_payload(
        make_game(
            game_id=2,
            user_num=1,
            character_num=1,
            game_rank=1,
            matching_team_mode=3,
            season_id=3,
            mmr_gain=20,
        )
    )
    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
        ]
    )

    code = run(
        [
            "--db",
            store.path,
            "stats",
            "mmr",
            "--server",
            "NA",
            "--mode",
            "RANKED",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    rows = json.loads(out)
    jackie = next(row for row in rows if row["character_num"] == 1)
    assert jackie["matches"] == 1
    assert jackie["avg_mmr_gain"] == pytest.approx(20.0)


def test_cli_default_season_non_ranked_is_zero(store, make_game, capsys):
    store.upsert_from_game_payload(
        make_game(
            game_id=1,
            user_num=1,
            character_num=1,
            matching_mode=2,
            matching_team_mode=3,
            season_id=25,
        )
    )
    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
        ]
    )

    code = run(
        [
            "--db",
            store.path,
            "stats",
            "character",
            "--server",
            "NA",
            "--mode",
            "normal",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    rows = json.loads(out)
    assert rows == []


def test_cli_team_stats_include_all_servers_and_names(store, make_game, capsys):
    def add_player(
        game_id: int,
        team_number: int,
        user_num: int,
        character_num: int,
        game_rank: int,
        server_name: str,
    ) -> None:
        game = make_game(
            game_id=game_id,
            user_num=user_num,
            character_num=character_num,
            game_rank=game_rank,
            matching_team_mode=3,
            server_name=server_name,
        )
        game["teamNumber"] = team_number
        store.upsert_from_game_payload(game)

    # Two games across different servers with the same composition (1,2,3).
    add_player(1, 1, 101, 1, 1, "NA")
    add_player(1, 1, 102, 2, 1, "NA")
    add_player(1, 1, 103, 3, 1, "NA")
    add_player(1, 2, 104, 4, 2, "NA")
    add_player(1, 2, 105, 5, 2, "NA")
    add_player(1, 2, 106, 6, 2, "NA")

    add_player(2, 1, 201, 1, 2, "Asia")
    add_player(2, 1, 202, 2, 2, "Asia")
    add_player(2, 1, 203, 3, 2, "Asia")
    add_player(2, 2, 204, 7, 1, "Asia")
    add_player(2, 2, 205, 8, 1, "Asia")
    add_player(2, 2, 206, 9, 1, "Asia")

    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
            {"characterCode": 2, "character": "Aya"},
            {"characterCode": 3, "character": "Hyunwoo"},
        ]
    )

    code = run(
        [
            "--db",
            store.path,
            "stats",
            "team",
            "--mode",
            "3",
            "--team-mode",
            "3",
            "--season",
            "25",
            "--min-matches",
            "2",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    rows = json.loads(out)
    assert rows
    comp = rows[0]
    assert comp["team_signature"] == "1+2+3"
    assert comp["matches"] == 2
    assert comp["wins"] == 1
    assert comp["top_finishes"] == 2
    assert comp["character_names"] == ["Jackie", "Aya", "Hyunwoo"]

    code = run(
        [
            "--db",
            store.path,
            "stats",
            "team",
            "--mode",
            "3",
            "--team-mode",
            "3",
            "--season",
            "25",
            "--min-matches",
            "1",
            "--no-include-names",
        ]
    )
    assert code == 0
    rows_no_names = json.loads(capsys.readouterr().out)
    assert any("character_names" not in row for row in rows_no_names)


def test_cli_ingest_only_newer_games_enabled_by_default(monkeypatch, store):
    from er_stats import cli as cli_mod

    recorded_kwargs: dict = {}

    class _RecorderManager:
        def __init__(self, client, db_store, **kwargs):
            recorded_kwargs.update(kwargs)

        def ingest_from_seeds(self, seeds, depth=1):  # pragma: no cover - trivial
            recorded_kwargs["seeds"] = list(seeds)
            recorded_kwargs["depth"] = depth

    monkeypatch.setattr(cli_mod, "EternalReturnAPIClient", _DummyClient)
    monkeypatch.setattr(cli_mod, "IngestionManager", _RecorderManager)

    code = run(
        [
            "--db",
            store.path,
            "ingest",
            "--base-url",
            "https://example.invalid",
            "--user",
            "12345",
        ]
    )

    assert code == 0
    assert recorded_kwargs["only_newer_games"] is True
    assert recorded_kwargs["max_games_per_user"] is None
    assert recorded_kwargs["seeds"] == [12345]
    client = _DummyClient.last_instance
    assert client is not None
    assert client.fetch_character_attributes_calls == 1
    assert client.fetch_item_armor_calls == 1
    assert client.fetch_item_weapon_calls == 1
    count = store.connection.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
    assert count == 2
    item_count = store.connection.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    assert item_count == 2


def test_cli_ingest_can_include_older_games(monkeypatch, store):
    from er_stats import cli as cli_mod

    recorded_kwargs: dict = {}

    class _RecorderManager:
        def __init__(self, client, db_store, **kwargs):
            recorded_kwargs.update(kwargs)

        def ingest_from_seeds(self, seeds, depth=1):  # pragma: no cover - trivial
            recorded_kwargs["seeds"] = list(seeds)
            recorded_kwargs["depth"] = depth

    monkeypatch.setattr(cli_mod, "EternalReturnAPIClient", _DummyClient)
    monkeypatch.setattr(cli_mod, "IngestionManager", _RecorderManager)

    code = run(
        [
            "--db",
            store.path,
            "ingest",
            "--base-url",
            "https://example.invalid",
            "--user",
            "777",
            "--include-older-games",
        ]
    )

    assert code == 0
    assert recorded_kwargs["only_newer_games"] is False
    assert recorded_kwargs["seeds"] == [777]
    client = _DummyClient.last_instance
    assert client is not None
    assert client.fetch_character_attributes_calls == 1


def test_cli_ingest_require_metadata_refresh_success(monkeypatch, store):
    from er_stats import cli as cli_mod

    recorded_kwargs: dict = {}

    class _RecorderManager:
        def __init__(self, client, db_store, **kwargs):
            recorded_kwargs.update(kwargs)

        def ingest_from_seeds(self, seeds, depth=1):  # pragma: no cover - trivial
            recorded_kwargs["seeds"] = list(seeds)
            recorded_kwargs["depth"] = depth

    monkeypatch.setattr(cli_mod, "EternalReturnAPIClient", _DummyClient)
    monkeypatch.setattr(cli_mod, "IngestionManager", _RecorderManager)

    code = run(
        [
            "--db",
            store.path,
            "ingest",
            "--base-url",
            "https://example.invalid",
            "--user",
            "12345",
            "--require-metadata-refresh",
        ]
    )

    assert code == 0


def test_cli_ingest_require_metadata_refresh_fails_on_error(monkeypatch, store):
    from er_stats import cli as cli_mod

    recorded_kwargs: dict = {}

    class _RecorderManager:
        def __init__(self, client, db_store, **kwargs):
            recorded_kwargs.update(kwargs)

        def ingest_from_seeds(self, seeds, depth=1):  # pragma: no cover - trivial
            recorded_kwargs["seeds"] = list(seeds)
            recorded_kwargs["depth"] = depth

    class _FailingClient(_DummyClient):
        def fetch_item_weapon(self) -> Dict[str, Any]:
            raise RuntimeError("simulated failure")

    monkeypatch.setattr(cli_mod, "EternalReturnAPIClient", _FailingClient)
    monkeypatch.setattr(cli_mod, "IngestionManager", _RecorderManager)

    code = run(
        [
            "--db",
            store.path,
            "ingest",
            "--base-url",
            "https://example.invalid",
            "--user",
            "12345",
            "--require-metadata-refresh",
        ]
    )

    assert code == 2


def test_cli_ingest_require_metadata_refresh_fails_on_character_error(
    monkeypatch, store
) -> None:
    from er_stats import cli as cli_mod

    recorded_kwargs: dict = {}

    class _RecorderManager:
        def __init__(self, client, db_store, **kwargs):
            recorded_kwargs.update(kwargs)

        def ingest_from_seeds(self, seeds, depth=1):  # pragma: no cover - trivial
            recorded_kwargs["seeds"] = list(seeds)
            recorded_kwargs["depth"] = depth

    class _FailingCharacterClient(_DummyClient):
        def fetch_character_attributes(self) -> Dict[str, Any]:
            raise RuntimeError("simulated character failure")

    monkeypatch.setattr(cli_mod, "EternalReturnAPIClient", _FailingCharacterClient)
    monkeypatch.setattr(cli_mod, "IngestionManager", _RecorderManager)

    code = run(
        [
            "--db",
            store.path,
            "ingest",
            "--base-url",
            "https://example.invalid",
            "--user",
            "12345",
            "--require-metadata-refresh",
        ]
    )

    assert code == 2
