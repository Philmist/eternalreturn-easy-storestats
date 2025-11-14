import json

from er_stats.cli import run


class _DummyClient:
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

    def close(self) -> None:
        return None


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

    code = run([
        "--db",
        store.path,
        "ingest",
        "--base-url",
        "https://example.invalid",
        "--user",
        "12345",
    ])

    assert code == 0
    assert recorded_kwargs["only_newer_games"] is True
    assert recorded_kwargs["max_games_per_user"] is None
    assert recorded_kwargs["seeds"] == [12345]


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

    code = run([
        "--db",
        store.path,
        "ingest",
        "--base-url",
        "https://example.invalid",
        "--user",
        "777",
        "--include-older-games",
    ])

    assert code == 0
    assert recorded_kwargs["only_newer_games"] is False
    assert recorded_kwargs["seeds"] == [777]

