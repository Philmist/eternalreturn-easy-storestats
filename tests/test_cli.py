import json
from typing import Any, Dict, Optional

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
