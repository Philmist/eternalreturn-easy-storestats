"""Configuration loading utilities for er-stats."""

from __future__ import annotations

from pathlib import Path
from typing import Any, List, Mapping, Optional


class ConfigError(Exception):
    """Raised when configuration loading fails."""


def _load_toml_bytes(data: bytes) -> Mapping[str, Any]:
    try:
        import tomllib  # type: ignore[attr-defined]
    except (
        ModuleNotFoundError
    ):  # pragma: no cover - import guarded by tomllib availability
        try:
            import tomli as tomllib  # type: ignore[import-not-found]
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on environment
            raise ConfigError(
                "TOML configuration requires Python 3.11+ or the 'tomli' package."
            ) from exc
    return tomllib.loads(data.decode("utf-8"))


def load_ingest_config(path: Path) -> Mapping[str, Any]:
    """Load ingest configuration from a TOML file."""
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise ConfigError(f"Failed to read config file: {path}") from exc

    try:
        raw = dict(_load_toml_bytes(data))
    except Exception as exc:
        raise ConfigError(f"Failed to parse TOML config: {path}") from exc

    ingest = raw.get("ingest")
    if not isinstance(ingest, Mapping):
        raise ConfigError("Config file must contain an [ingest] table.")

    seeds = ingest.get("seeds", {})
    if seeds is None:
        seeds = {}
    if not isinstance(seeds, Mapping):
        raise ConfigError("[ingest.seeds] must be a table when present.")

    auth = raw.get("auth", {})
    if auth is None:
        auth = {}
    if not isinstance(auth, Mapping):
        raise ConfigError("[auth] must be a table when present.")

    def _as_int_list(value: Any, field: str) -> List[int]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ConfigError(f"{field} must be a list of integers.")
        result: List[int] = []
        for item in value:
            if not isinstance(item, int):
                raise ConfigError(f"{field} must be a list of integers.")
            result.append(item)
        return result

    def _as_str_list(value: Any, field: str) -> List[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ConfigError(f"{field} must be a list of strings.")
        result: List[str] = []
        for item in value:
            if not isinstance(item, str):
                raise ConfigError(f"{field} must be a list of strings.")
            result.append(item)
        return result

    uids = _as_str_list(seeds.get("uids"), "ingest.seeds.uids")
    users = _as_int_list(seeds.get("users"), "ingest.seeds.users")
    nicknames = _as_str_list(seeds.get("nicknames"), "ingest.seeds.nicknames")

    api_key_env: Optional[str]
    api_key_env_value = auth.get("api_key_env")
    if api_key_env_value is None:
        api_key_env = None
    elif isinstance(api_key_env_value, str):
        api_key_env = api_key_env_value
    else:
        raise ConfigError("auth.api_key_env must be a string when present.")

    return {
        "raw": raw,
        "ingest": ingest,
        "seeds": {
            "uids": uids,
            "users": users,
            "nicknames": nicknames,
        },
        "auth": {
            "api_key_env": api_key_env,
        },
    }
