from er_stats.cli import run


class _RecorderClient:
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

    def fetch_user_by_nickname(self, nickname: str) -> dict:
        return {
            "user": {
                "userNum": 999999,
            },
        }

    def fetch_character_attributes(self) -> dict:
        return {"data": []}

    def close(self) -> None:
        return None


def test_ingest_uses_config_defaults(monkeypatch, tmp_path):
    from er_stats import cli as cli_mod

    config_db = tmp_path / "config.db"
    config_path = tmp_path / "ingest.toml"

    config_text = """
[ingest]
db_path = "{db_path}"
base_url = "https://config.example/"
depth = 2
max_games_per_user = 50
min_interval = 2.5
max_retries = 5
only_newer_games = false

[ingest.seeds]
users = [111]
nicknames = ["FromConfig"]

[auth]
api_key_env = "ER_API_KEY"
""".format(db_path=str(config_db).replace("\\", "\\\\"))
    config_path.write_text(config_text, encoding="utf-8")

    recorded_kwargs: dict = {}

    class _RecorderManager:
        def __init__(self, client, db_store, **kwargs):
            recorded_kwargs.update(kwargs)
            recorded_kwargs["client"] = client
            recorded_kwargs["db_store"] = db_store

        def ingest_from_seeds(self, seeds, depth=1):
            recorded_kwargs["seeds"] = list(seeds)
            recorded_kwargs["depth"] = depth

    monkeypatch.setenv("ER_API_KEY", "from-env")
    monkeypatch.setattr(cli_mod, "EternalReturnAPIClient", _RecorderClient)
    monkeypatch.setattr(cli_mod, "IngestionManager", _RecorderManager)

    code = run(
        [
            "ingest",
            "--config",
            str(config_path),
        ]
    )

    assert code == 0
    assert recorded_kwargs["max_games_per_user"] == 50
    assert recorded_kwargs["only_newer_games"] is False
    assert recorded_kwargs["depth"] == 2
    assert recorded_kwargs["seeds"] == [111, 999999]

    client = recorded_kwargs.get("client")
    assert client is not None
    assert client.base_url == "https://config.example/"
    assert client.min_interval == 2.5
    assert client.max_retries == 5
    assert client.api_key == "from-env"


def test_ingest_cli_overrides_config(monkeypatch, tmp_path):
    from er_stats import cli as cli_mod

    config_db = tmp_path / "config.db"
    config_path = tmp_path / "ingest_override.toml"

    config_text = """
[ingest]
db_path = "{db_path}"
max_games_per_user = 50
only_newer_games = true

[ingest.seeds]
users = [1]
""".format(db_path=str(config_db).replace("\\", "\\\\"))
    config_path.write_text(config_text, encoding="utf-8")

    recorded_kwargs: dict = {}

    class _RecorderManager:
        def __init__(self, client, db_store, **kwargs):
            recorded_kwargs.update(kwargs)

        def ingest_from_seeds(self, seeds, depth=1):
            recorded_kwargs["seeds"] = list(seeds)
            recorded_kwargs["depth"] = depth

    monkeypatch.setattr(cli_mod, "EternalReturnAPIClient", _RecorderClient)
    monkeypatch.setattr(cli_mod, "IngestionManager", _RecorderManager)

    code = run(
        [
            "ingest",
            "--config",
            str(config_path),
            "--max-games",
            "10",
            "--include-older-games",
            "--user",
            "2",
        ]
    )

    assert code == 0
    assert recorded_kwargs["max_games_per_user"] == 10
    assert recorded_kwargs["only_newer_games"] is False
    assert recorded_kwargs["seeds"] == [1, 2]
