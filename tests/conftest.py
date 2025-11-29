import sys
from pathlib import Path
from typing import Dict, Any

import pytest
from er_stats import SQLiteStore

# Make tests robust to both flat and src/ layouts without requiring installation
_HERE = Path(__file__).resolve()
_ROOT = _HERE.parents[1]
_SRC = _ROOT / "src"
for _p in (_SRC, _ROOT):
    if _p.exists() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))


@pytest.fixture
def store(tmp_path):
    db_path = tmp_path / "er_stats.sqlite"
    s = SQLiteStore(str(db_path))
    s.setup_schema()
    try:
        yield s
    finally:
        s.close()


def _make_game(
    *,
    game_id: int,
    uid: str | None = None,
    user_num: int | None = None,
    season_id: int = 25,
    server_name: str = "NA",
    matching_mode: int = 3,
    matching_team_mode: int = 1,
    character_num: int = 1,
    game_rank: int = 3,
    mmr_gain: int = 10,
    mlbot: bool | None = None,
) -> Dict[str, Any]:
    if uid is None and user_num is not None:
        uid = str(user_num)
    if uid is None:
        uid = "uid-unknown"
    uid = str(uid)
    payload: Dict[str, Any] = {
        "gameId": game_id,
        "seasonId": season_id,
        "matchingMode": matching_mode,
        "matchingTeamMode": matching_team_mode,
        "serverName": server_name,
        "versionSeason": 1,
        "versionMajor": 1,
        "versionMinor": 0,
        "startDtm": "2025-10-27T23:24:03.003+0900",
        "duration": 900,
        "uid": uid,
        "nickname": f"user{uid}",
        "mmrAfter": 1200,
        "language": "en",
        "characterNum": character_num,
        "skinCode": 0,
        "gameRank": game_rank,
        "playerKill": 3,
        "playerAssistant": 2,
        "monsterKill": 10,
        "mmrGain": mmr_gain,
        "mmrLossEntryCost": 5,
        "victory": int(game_rank == 1),
        "playTime": 900,
        "damageToPlayer": 1000,
        "damageFromPlayer": 500,
        "damageFromMonster": 120,
        "damageToMonster": 2500,
        "damageToPlayer_Shield": 50,
        "characterLevel": 15,
        "bestWeapon": 1,
        "bestWeaponLevel": 10,
        "teamNumber": 1,
        "preMade": 0,
        "equipment": {"0": 101101, "1": 101102},
        "equipmentGrade": {"0": 2, "1": 3},
        "equipFirstItemForLog": {"0": [101101], "1": [101102]},
        "masteryLevel": {"401": 7, "402": 6},
        "skillLevelInfo": {"1015101": 5, "1015102": 4},
        "skillOrderInfo": {"1": 1015101, "2": 1015102},
        "creditRevivalCount": 1,
        "creditRevivedOthersCount": 0,
        "totalGainVFCredit": 150,
        "totalUseVFCredit": 100,
    }
    if mlbot is not None:
        payload["mlbot"] = mlbot
    return payload


@pytest.fixture(name="make_game")
def make_game_fixture():
    return _make_game
