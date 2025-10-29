"""HTTP client for interacting with the Eternal Return developer API."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

import requests


class EternalReturnAPIClient:
    """Lightweight client for the Eternal Return API."""

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        session: Optional[requests.Session] = None,
        timeout: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._session = session or requests.Session()
        self.timeout = timeout

    @property
    def session(self) -> requests.Session:
        """Return the configured :class:`requests.Session`."""

        return self._session

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self.api_key:
            headers["x-api-key"] = self.api_key
        if extra:
            headers.update(extra)
        return headers

    def fetch_user_games(self, user_num: int, next_token: Optional[str] = None) -> Dict[str, Any]:
        """Fetch the paginated match list for the given user."""

        url = f"{self.base_url}/v1/user/games/{user_num}"
        headers = self._headers({"next": next_token} if next_token else None)
        response = self.session.get(url, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def fetch_game_result(self, game_id: int) -> Dict[str, Any]:
        """Fetch the full participant list for a game."""

        url = f"{self.base_url}/v1/games/{game_id}"
        response = self.session.get(url, headers=self._headers(), timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        """Close the underlying :class:`requests.Session`."""

        self.session.close()

    def iter_user_games(self, user_num: int) -> Iterable[Dict[str, Any]]:
        """Iterate through all available games for a user."""

        next_token: Optional[str] = None
        while True:
            payload = self.fetch_user_games(user_num, next_token)
            for game in payload.get("userGames", []):
                yield game
            next_token = payload.get("next")
            if not next_token:
                break


__all__ = ["EternalReturnAPIClient"]
