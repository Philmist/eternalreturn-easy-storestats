from typing import Any, Dict

import pytest
import requests

from er_stats.api_client import (
    ApiResponseError,
    EternalReturnAPIClient,
    is_nickname_not_found_error,
    is_user_games_no_games_error,
    is_user_games_uid_missing_error,
)


class _Resp:
    def __init__(self, payload: Dict[str, Any], *, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.headers: Dict[str, str] = {}

    def json(self) -> Dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            response = requests.Response()
            response.status_code = self.status_code
            raise requests.HTTPError(response=response)


class _Session:
    def __init__(self):
        self.calls = []

    def get(self, url: str, headers: Dict[str, str], timeout: float):
        # First call has no next header, return page 1
        self.calls.append((url, headers, timeout))
        if headers.get("next") is None:
            return _Resp(
                {
                    "userGames": [
                        {"gameId": 1},
                    ],
                    "next": "tok",
                }
            )
        return _Resp({"userGames": [{"gameId": 2}, {"gameId": 3}]})

    def close(self) -> None:
        return None


def test_iter_user_games_paginates():
    session = _Session()
    client = EternalReturnAPIClient(
        base_url="https://example.invalid",
        api_key="key",
        session=session,
        min_interval=0.0,  # disable delay in tests
    )

    items = list(client.iter_user_games(123))
    assert [i["gameId"] for i in items] == [1, 2, 3]
    # API key header present
    assert any("x-api-key" in h for _, h, _ in session.calls)


def test_fetch_user_games_accepts_payload_code_200():
    class _SingleSession:
        def get(self, url: str, headers: Dict[str, str], timeout: float):
            return _Resp({"code": 200, "message": "Success", "userGames": []})

        def close(self) -> None:
            return None

    client = EternalReturnAPIClient(
        base_url="https://example.invalid",
        session=_SingleSession(),
        min_interval=0.0,
    )

    payload = client.fetch_user_games("UID-1")
    assert payload["code"] == 200


def test_fetch_user_games_raises_on_payload_not_found():
    class _SingleSession:
        def get(self, url: str, headers: Dict[str, str], timeout: float):
            return _Resp({"code": 404, "message": "User Not Found"})

        def close(self) -> None:
            return None

    client = EternalReturnAPIClient(
        base_url="https://example.invalid",
        session=_SingleSession(),
        min_interval=0.0,
    )

    with pytest.raises(ApiResponseError) as exc:
        client.fetch_user_games("UID-1")
    assert exc.value.code == 404
    assert exc.value.message == "User Not Found"


def test_fetch_user_games_raises_on_payload_error_code():
    class _SingleSession:
        def get(self, url: str, headers: Dict[str, str], timeout: float):
            return _Resp({"code": 500, "message": "Internal Error"})

        def close(self) -> None:
            return None

    client = EternalReturnAPIClient(
        base_url="https://example.invalid",
        session=_SingleSession(),
        min_interval=0.0,
    )

    with pytest.raises(ApiResponseError) as exc:
        client.fetch_user_games("UID-1")
    assert exc.value.code == 500


def test_fetch_user_games_raises_on_http_404():
    class _SingleSession:
        def get(self, url: str, headers: Dict[str, str], timeout: float):
            return _Resp({}, status_code=404)

        def close(self) -> None:
            return None

    client = EternalReturnAPIClient(
        base_url="https://example.invalid",
        session=_SingleSession(),
        min_interval=0.0,
    )

    with pytest.raises(requests.HTTPError):
        client.fetch_user_games("UID-1")


def test_endpoint_specific_error_classification_for_user_games():
    uid_missing = ApiResponseError(
        code=401,
        message="Unauthorized",
        payload={"code": 401, "message": "Unauthorized"},
        url="https://example.invalid/v1/user/games/uid/UID-1",
    )
    no_games = ApiResponseError(
        code=404,
        message="User Not Found",
        payload={"code": 404, "message": "User Not Found"},
        url="https://example.invalid/v1/user/games/uid/UID-1",
    )

    assert is_user_games_uid_missing_error(uid_missing)
    assert not is_user_games_no_games_error(uid_missing)
    assert not is_nickname_not_found_error(uid_missing)

    assert is_user_games_no_games_error(no_games)
    assert not is_user_games_uid_missing_error(no_games)
    assert not is_nickname_not_found_error(no_games)


def test_endpoint_specific_error_classification_for_nickname():
    nickname_missing = ApiResponseError(
        code=404,
        message="User Not Found",
        payload={"code": 404, "message": "User Not Found"},
        url="https://example.invalid/v1/user/nickname?query=ghost",
    )
    assert is_nickname_not_found_error(nickname_missing)
    assert not is_user_games_uid_missing_error(nickname_missing)
    assert not is_user_games_no_games_error(nickname_missing)
