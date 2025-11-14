from typing import Any, Dict

from er_stats.api_client import EternalReturnAPIClient


class _Resp:
    def __init__(self, payload: Dict[str, Any]):
        self._payload = payload

    def json(self) -> Dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class _Session:
    def __init__(self):
        self.calls = []

    def get(self, url: str, headers: Dict[str, str], timeout: float):
        # First call has no next header, return page 1
        self.calls.append((url, headers, timeout))
        if headers.get("next") is None:
            return _Resp({"userGames": [
                {"gameId": 1},
            ], "next": "tok"})
        return _Resp({"userGames": [
            {"gameId": 2}, {"gameId": 3}
        ]})

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
