"""HTTP client for interacting with the Eternal Return developer API.

Applies a default rate limit of 1 request per second, as required by the
Eternal Return Developer API. The interval can be customized for tests.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

import time

import requests


class EternalReturnAPIClient:
    """Lightweight client for the Eternal Return API."""

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        session: Optional[requests.Session] = None,
        timeout: float = 10.0,
        *,
        min_interval: float = 1.0,
        max_retries: int = 3,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._session = session or requests.Session()
        self.timeout = timeout
        self.min_interval = float(min_interval)
        self.max_retries = int(max_retries)
        self._last_request_at: Optional[float] = None

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

    def fetch_user_games(
        self, uid: str, next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch the paginated match list for the given user."""

        url = f"{self.base_url}/v1/user/games/uid/{uid}"
        if next_token is not None and type(next_token) is not str:
            next_token = str(next_token)
        headers = self._headers({"next": next_token} if next_token else None)
        return self._get_json_with_rate_limit(url, headers)

    def fetch_game_result(self, game_id: int) -> Dict[str, Any]:
        """Fetch the full participant list for a game."""

        url = f"{self.base_url}/v1/games/{game_id}"
        return self._get_json_with_rate_limit(url, self._headers())

    def fetch_user_by_nickname(self, nickname: str) -> Dict[str, Any]:
        """Resolve a user's public nickname to their user record.

        Returns the API payload which includes at least:
          {"code": 200, "message": "Success", "user": {"userId": str, "nickname": str}}

        Raises for non-2xx responses.
        """
        # Endpoint example:
        #   GET /v1/user/nickname?query=Philmist
        url = f"{self.base_url}/v1/user/nickname?query={requests.utils.quote(nickname)}"
        return self._get_json_with_rate_limit(
            url, self._headers({"accept": "application/json"})
        )

    def fetch_character_attributes(self) -> Dict[str, Any]:
        """Fetch the official character attributes catalog."""

        url = f"{self.base_url}/v2/data/CharacterAttributes"
        return self._get_json_with_rate_limit(
            url, self._headers({"accept": "application/json"})
        )

    def fetch_item_armor(self) -> Dict[str, Any]:
        """Fetch the official armor item catalog."""

        url = f"{self.base_url}/v2/data/ItemArmor"
        return self._get_json_with_rate_limit(
            url, self._headers({"accept": "application/json"})
        )

    def fetch_item_weapon(self) -> Dict[str, Any]:
        """Fetch the official weapon item catalog."""

        url = f"{self.base_url}/v2/data/ItemWeapon"
        return self._get_json_with_rate_limit(
            url, self._headers({"accept": "application/json"})
        )

    def close(self) -> None:
        """Close the underlying :class:`requests.Session`."""

        self.session.close()

    def iter_user_games(self, uid: str) -> Iterable[Dict[str, Any]]:
        """Iterate through all available games for a user."""

        next_token: Optional[str] = None
        while True:
            payload = self.fetch_user_games(uid, next_token)
            for game in payload.get("userGames", []):
                yield game
            next_token = payload.get("next")
            if not next_token:
                break

    # Internal helpers
    def _wait_for_slot(self) -> None:
        """Sleep if needed to respect the minimum interval between requests."""

        if self.min_interval <= 0:
            return
        now = time.monotonic()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            remaining = self.min_interval - elapsed
            if remaining > 0:
                time.sleep(remaining)
                now = time.monotonic()
        # Reserve the slot at request start to avoid bursts across threads
        self._last_request_at = now

    def _get_json_with_rate_limit(
        self, url: str, headers: Dict[str, str]
    ) -> Dict[str, Any]:
        """Perform a GET with rate limiting and simple 429 retry."""

        attempts = 0
        while True:
            attempts += 1
            self._wait_for_slot()
            response = self.session.get(url, headers=headers, timeout=self.timeout)

            status = getattr(response, "status_code", None)
            # Handle 429 Too Many Requests (and 403 when used as rate-limit) with basic backoff
            if status in (403, 429):
                # Honor Retry-After if present; default to min_interval
                retry_after = None
                try:
                    retry_after_hdr = getattr(response, "headers", {}).get(
                        "Retry-After"
                    )
                    if retry_after_hdr is not None:
                        retry_after = float(retry_after_hdr)
                except Exception:
                    retry_after = None
                time.sleep(
                    retry_after
                    if retry_after is not None
                    else max(self.min_interval, 1.0)
                )
                if attempts <= self.max_retries:
                    # After wait, try again
                    continue
                # Exhausted retries, raise the HTTP error if available
                response.raise_for_status()
            # Normal happy path
            response.raise_for_status()
            return response.json()


__all__ = ["EternalReturnAPIClient"]
