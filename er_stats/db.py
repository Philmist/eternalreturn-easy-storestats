"""SQLite persistence layer for Eternal Return statistics."""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def parse_start_time(value: Optional[str]) -> Optional[str]:
    """Convert the API timestamp into ISO-8601 with colon separator."""

    if not value:
        return None
    try:
        # Example: 2025-10-27T23:24:03.003+0900
        if value.endswith("Z"):
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            if value[-3] == ":":
                parsed = dt.datetime.fromisoformat(value)
            else:
                parsed = dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
        return parsed.isoformat()
    except ValueError:
        return value


def _resolve_ml_bot(game: Dict[str, Any]) -> Optional[int]:
    """Return 1 when either mlbot flag is truthy, 0 when explicitly false, else None."""

    flags = [game.get("mlbot"), game.get("isMLBot")]
    for flag in flags:
        if flag is None:
            continue
        return int(bool(flag))
    return None


class SQLiteStore:
    """SQLite-backed repository for match data."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.connection = sqlite3.connect(
            path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")

    def close(self) -> None:
        self.connection.close()

    @contextmanager
    def cursor(self) -> Iterator[sqlite3.Cursor]:
        cur = self.connection.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def setup_schema(self) -> None:
        with self.cursor() as cur:
            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_num INTEGER PRIMARY KEY,
                    nickname TEXT,
                    first_seen TEXT,
                    last_seen TEXT,
                    last_mmr INTEGER,
                    last_mlbot INTEGER DEFAULT 0,
                    last_language TEXT
                );

                CREATE TABLE IF NOT EXISTS matches (
                    game_id INTEGER PRIMARY KEY,
                    season_id INTEGER NOT NULL,
                    matching_mode INTEGER NOT NULL,
                    matching_team_mode INTEGER NOT NULL,
                    server_name TEXT NOT NULL,
                    version_major INTEGER,
                    version_minor INTEGER,
                    start_dtm TEXT,
                    duration INTEGER,
                    raw_json TEXT,
                    UNIQUE(game_id)
                );

                CREATE TABLE IF NOT EXISTS user_match_stats (
                    game_id INTEGER NOT NULL,
                    user_num INTEGER NOT NULL,
                    character_num INTEGER,
                    skin_code INTEGER,
                    game_rank INTEGER,
                    player_kill INTEGER,
                    player_assistant INTEGER,
                    monster_kill INTEGER,
                    mmr_after INTEGER,
                    mmr_gain INTEGER,
                    mmr_loss_entry_cost INTEGER,
                    victory INTEGER,
                    play_time INTEGER,
                    damage_to_player INTEGER,
                    character_level INTEGER,
                    best_weapon INTEGER,
                    best_weapon_level INTEGER,
                    team_number INTEGER,
                    premade INTEGER,
                    language TEXT,
                    ml_bot INTEGER,
                    raw_json TEXT,
                    PRIMARY KEY (game_id, user_num),
                    FOREIGN KEY (game_id) REFERENCES matches(game_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS equipment (
                    game_id INTEGER NOT NULL,
                    user_num INTEGER NOT NULL,
                    slot INTEGER NOT NULL,
                    item_id INTEGER,
                    grade INTEGER,
                    PRIMARY KEY (game_id, user_num, slot),
                    FOREIGN KEY (game_id, user_num) REFERENCES user_match_stats(game_id, user_num)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS mastery_levels (
                    game_id INTEGER NOT NULL,
                    user_num INTEGER NOT NULL,
                    mastery_id INTEGER NOT NULL,
                    level INTEGER,
                    PRIMARY KEY (game_id, user_num, mastery_id),
                    FOREIGN KEY (game_id, user_num) REFERENCES user_match_stats(game_id, user_num)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS skill_levels (
                    game_id INTEGER NOT NULL,
                    user_num INTEGER NOT NULL,
                    skill_code INTEGER NOT NULL,
                    level INTEGER,
                    PRIMARY KEY (game_id, user_num, skill_code),
                    FOREIGN KEY (game_id, user_num) REFERENCES user_match_stats(game_id, user_num)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS skill_orders (
                    game_id INTEGER NOT NULL,
                    user_num INTEGER NOT NULL,
                    sequence INTEGER NOT NULL,
                    skill_code INTEGER,
                    PRIMARY KEY (game_id, user_num, sequence),
                    FOREIGN KEY (game_id, user_num) REFERENCES user_match_stats(game_id, user_num)
                        ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_matches_context
                    ON matches (season_id, server_name, matching_mode, matching_team_mode);

                CREATE INDEX IF NOT EXISTS idx_user_match_character
                    ON user_match_stats (character_num, game_rank);

                CREATE INDEX IF NOT EXISTS idx_user_match_user
                    ON user_match_stats (user_num);
                """
            )
        self.connection.commit()

    def upsert_user(self, game: Dict[str, Any]) -> None:
        user_num = game.get("userNum")
        nickname = game.get("nickname")
        start_time = parse_start_time(game.get("startDtm"))
        mmr_after = game.get("mmrAfter")
        ml_bot_flag = _resolve_ml_bot(game)
        language = game.get("language")
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (
                    user_num, nickname, first_seen, last_seen, last_mmr, last_mlbot, last_language
                ) VALUES (
                    :user_num, :nickname, :first_seen, :last_seen, :last_mmr, :last_mlbot, :last_language
                )
                ON CONFLICT(user_num) DO UPDATE SET
                    nickname=excluded.nickname,
                    last_seen=MAX(users.last_seen, excluded.last_seen),
                    last_mmr=excluded.last_mmr,
                    last_mlbot=excluded.last_mlbot,
                    last_language=excluded.last_language
                """,
                {
                    "user_num": user_num,
                    "nickname": nickname,
                    "first_seen": start_time,
                    "last_seen": start_time,
                    "last_mmr": mmr_after,
                    "last_mlbot": ml_bot_flag,
                    "last_language": language,
                },
            )
        self.connection.commit()

    def upsert_match(self, game: Dict[str, Any]) -> None:
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO matches (
                    game_id,
                    season_id,
                    matching_mode,
                    matching_team_mode,
                    server_name,
                    version_major,
                    version_minor,
                    start_dtm,
                    duration,
                    raw_json
                ) VALUES (
                    :game_id, :season_id, :matching_mode, :matching_team_mode, :server_name,
                    :version_major, :version_minor, :start_dtm, :duration, :raw_json
                )
                ON CONFLICT(game_id) DO UPDATE SET
                    season_id=excluded.season_id,
                    matching_mode=excluded.matching_mode,
                    matching_team_mode=excluded.matching_team_mode,
                    server_name=excluded.server_name,
                    version_major=excluded.version_major,
                    version_minor=excluded.version_minor,
                    start_dtm=excluded.start_dtm,
                    duration=excluded.duration,
                    raw_json=excluded.raw_json
                """,
                {
                    "game_id": game.get("gameId"),
                    "season_id": game.get("seasonId"),
                    "matching_mode": game.get("matchingMode"),
                    "matching_team_mode": game.get("matchingTeamMode"),
                    "server_name": game.get("serverName"),
                    "version_major": game.get("versionMajor"),
                    "version_minor": game.get("versionMinor"),
                    "start_dtm": parse_start_time(game.get("startDtm")),
                    "duration": game.get("duration"),
                    "raw_json": json.dumps(game, ensure_ascii=False),
                },
            )
        self.connection.commit()

    def upsert_user_match_stats(self, game: Dict[str, Any]) -> None:
        payload = {
            "game_id": game.get("gameId"),
            "user_num": game.get("userNum"),
            "character_num": game.get("characterNum"),
            "skin_code": game.get("skinCode"),
            "game_rank": game.get("gameRank"),
            "player_kill": game.get("playerKill"),
            "player_assistant": game.get("playerAssistant"),
            "monster_kill": game.get("monsterKill"),
            "mmr_after": game.get("mmrAfter"),
            "mmr_gain": game.get("mmrGain"),
            "mmr_loss_entry_cost": game.get("mmrLossEntryCost"),
            "victory": game.get("victory"),
            "play_time": game.get("playTime"),
            "damage_to_player": game.get("damageToPlayer"),
            "character_level": game.get("characterLevel"),
            "best_weapon": game.get("bestWeapon"),
            "best_weapon_level": game.get("bestWeaponLevel"),
            "team_number": game.get("teamNumber"),
            "premade": game.get("preMade"),
            "language": game.get("language"),
            "ml_bot": _resolve_ml_bot(game),
            "raw_json": json.dumps(game, ensure_ascii=False),
        }
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_match_stats (
                    game_id, user_num, character_num, skin_code, game_rank,
                    player_kill, player_assistant, monster_kill, mmr_after,
                    mmr_gain, mmr_loss_entry_cost, victory, play_time,
                    damage_to_player, character_level, best_weapon,
                    best_weapon_level, team_number, premade, language, ml_bot, raw_json
                ) VALUES (
                    :game_id, :user_num, :character_num, :skin_code, :game_rank,
                    :player_kill, :player_assistant, :monster_kill, :mmr_after,
                    :mmr_gain, :mmr_loss_entry_cost, :victory, :play_time,
                    :damage_to_player, :character_level, :best_weapon,
                    :best_weapon_level, :team_number, :premade, :language, :ml_bot, :raw_json
                )
                ON CONFLICT(game_id, user_num) DO UPDATE SET
                    character_num=excluded.character_num,
                    skin_code=excluded.skin_code,
                    game_rank=excluded.game_rank,
                    player_kill=excluded.player_kill,
                    player_assistant=excluded.player_assistant,
                    monster_kill=excluded.monster_kill,
                    mmr_after=excluded.mmr_after,
                    mmr_gain=excluded.mmr_gain,
                    mmr_loss_entry_cost=excluded.mmr_loss_entry_cost,
                    victory=excluded.victory,
                    play_time=excluded.play_time,
                    damage_to_player=excluded.damage_to_player,
                    character_level=excluded.character_level,
                    best_weapon=excluded.best_weapon,
                    best_weapon_level=excluded.best_weapon_level,
                    team_number=excluded.team_number,
                    premade=excluded.premade,
                    language=excluded.language,
                    ml_bot=excluded.ml_bot,
                    raw_json=excluded.raw_json
                """,
                payload,
            )
        self.connection.commit()

    def replace_equipment(self, game: Dict[str, Any]) -> None:
        game_id = game.get("gameId")
        user_num = game.get("userNum")
        equipment = game.get("equipment") or {}
        grades = game.get("equipmentGrade") or {}
        with self.cursor() as cur:
            cur.execute(
                "DELETE FROM equipment WHERE game_id=? AND user_num=?",
                (game_id, user_num),
            )
            for slot_str, item_id in equipment.items():
                slot = int(slot_str)
                grade = grades.get(slot_str)
                cur.execute(
                    """
                    INSERT INTO equipment (game_id, user_num, slot, item_id, grade)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (game_id, user_num, slot, item_id, grade),
                )
        self.connection.commit()

    def replace_mastery_levels(self, game: Dict[str, Any]) -> None:
        mastery_levels = game.get("masteryLevel") or {}
        if not mastery_levels:
            return
        game_id = game.get("gameId")
        user_num = game.get("userNum")
        with self.cursor() as cur:
            cur.execute(
                "DELETE FROM mastery_levels WHERE game_id=? AND user_num=?",
                (game_id, user_num),
            )
            for mastery_id, level in mastery_levels.items():
                cur.execute(
                    """
                    INSERT INTO mastery_levels (game_id, user_num, mastery_id, level)
                    VALUES (?, ?, ?, ?)
                    """,
                    (game_id, user_num, int(mastery_id), level),
                )
        self.connection.commit()

    def replace_skill_levels(self, game: Dict[str, Any]) -> None:
        skill_levels = game.get("skillLevelInfo") or {}
        game_id = game.get("gameId")
        user_num = game.get("userNum")
        if not skill_levels:
            return
        with self.cursor() as cur:
            cur.execute(
                "DELETE FROM skill_levels WHERE game_id=? AND user_num=?",
                (game_id, user_num),
            )
            for code, level in skill_levels.items():
                cur.execute(
                    """
                    INSERT INTO skill_levels (game_id, user_num, skill_code, level)
                    VALUES (?, ?, ?, ?)
                    """,
                    (game_id, user_num, int(code), level),
                )
        self.connection.commit()

    def replace_skill_orders(self, game: Dict[str, Any]) -> None:
        skill_orders = game.get("skillOrderInfo") or {}
        game_id = game.get("gameId")
        user_num = game.get("userNum")
        if not skill_orders:
            return
        with self.cursor() as cur:
            cur.execute(
                "DELETE FROM skill_orders WHERE game_id=? AND user_num=?",
                (game_id, user_num),
            )
            for sequence, skill_code in skill_orders.items():
                cur.execute(
                    """
                    INSERT INTO skill_orders (game_id, user_num, sequence, skill_code)
                    VALUES (?, ?, ?, ?)
                    """,
                    (game_id, user_num, int(sequence), skill_code),
                )
        self.connection.commit()

    def upsert_from_game_payload(self, game: Dict[str, Any]) -> None:
        self.upsert_user(game)
        self.upsert_match(game)
        self.upsert_user_match_stats(game)
        self.replace_equipment(game)
        self.replace_mastery_levels(game)
        self.replace_skill_levels(game)
        self.replace_skill_orders(game)

    def has_game(self, game_id: int) -> bool:
        with self.cursor() as cur:
            cur.execute("SELECT 1 FROM matches WHERE game_id=?", (game_id,))
            return cur.fetchone() is not None

    def transaction(self) -> sqlite3.Connection:
        return self.connection


__all__ = ["SQLiteStore"]
