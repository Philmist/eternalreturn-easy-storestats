"""SQLite persistence layer for Eternal Return statistics."""

from __future__ import annotations

import datetime as dt
import functools
import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, Optional, Set

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


def _resolve_ml_bot(game: Dict[str, Any]) -> int:
    """Return 1 when either mlbot flag is truthy, 0 when explicitly false, else return 0."""

    flags = [game.get("mlbot"), game.get("isMLBot")]
    flag = functools.reduce(
        lambda lv, rv: True
        if lv is True or rv is True
        else False
        if lv is False or rv is False
        else None,
        flags,
        None,
    )
    if flag is None:
        return 0
    return int(bool(flag))


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
                    ml_bot INTEGER DEFAULT 0,
                    last_language TEXT
                );

                CREATE TABLE IF NOT EXISTS matches (
                    game_id INTEGER PRIMARY KEY,
                    season_id INTEGER NOT NULL,
                    matching_mode INTEGER NOT NULL,
                    matching_team_mode INTEGER NOT NULL,
                    server_name TEXT NOT NULL,
                    version_season INTEGER,
                    version_major INTEGER,
                    version_minor INTEGER,
                    start_dtm TEXT,
                    duration INTEGER,
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

                CREATE TABLE IF NOT EXISTS characters (
                    character_code INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS items (
                    item_code INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    mode_type INTEGER,
                    item_type TEXT,
                    item_grade TEXT,
                    is_completed_item INTEGER
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
                    user_num, nickname, first_seen, last_seen, last_mmr, ml_bot, last_language
                ) VALUES (
                    :user_num, :nickname, :first_seen, :last_seen, :last_mmr, :ml_bot, :last_language
                )
                ON CONFLICT(user_num) DO UPDATE SET
                    nickname=excluded.nickname,
                    last_seen=MAX(users.last_seen, excluded.last_seen),
                    last_mmr=excluded.last_mmr,
                    ml_bot=excluded.ml_bot,
                    last_language=excluded.last_language
                WHERE
                    excluded.last_seen > users.last_seen
                """,
                {
                    "user_num": user_num,
                    "nickname": nickname,
                    "first_seen": start_time,
                    "last_seen": start_time,
                    "last_mmr": mmr_after,
                    "ml_bot": ml_bot_flag,
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
                    version_season,
                    version_major,
                    version_minor,
                    start_dtm,
                    duration
                ) VALUES (
                    :game_id, :season_id, :matching_mode, :matching_team_mode, :server_name,
                    :version_season, :version_major, :version_minor, :start_dtm, :duration
                )
                ON CONFLICT(game_id) DO UPDATE SET
                    season_id=excluded.season_id,
                    matching_mode=excluded.matching_mode,
                    matching_team_mode=excluded.matching_team_mode,
                    server_name=excluded.server_name,
                    version_season=excluded.version_season,
                    version_major=excluded.version_major,
                    version_minor=excluded.version_minor,
                    start_dtm=excluded.start_dtm,
                    duration=excluded.duration
                """,
                {
                    "game_id": game.get("gameId"),
                    "season_id": game.get("seasonId"),
                    "matching_mode": game.get("matchingMode"),
                    "matching_team_mode": game.get("matchingTeamMode"),
                    "server_name": game.get("serverName"),
                    "version_season": game.get("versionSeason"),
                    "version_major": game.get("versionMajor"),
                    "version_minor": game.get("versionMinor"),
                    "start_dtm": parse_start_time(game.get("startDtm")),
                    "duration": game.get("duration"),
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
        }
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_match_stats (
                    game_id, user_num, character_num, skin_code, game_rank,
                    player_kill, player_assistant, monster_kill, mmr_after,
                    mmr_gain, mmr_loss_entry_cost, victory, play_time,
                    damage_to_player, character_level, best_weapon,
                    best_weapon_level, team_number, premade, language, ml_bot
                ) VALUES (
                    :game_id, :user_num, :character_num, :skin_code, :game_rank,
                    :player_kill, :player_assistant, :monster_kill, :mmr_after,
                    :mmr_gain, :mmr_loss_entry_cost, :victory, :play_time,
                    :damage_to_player, :character_level, :best_weapon,
                    :best_weapon_level, :team_number, :premade, :language, :ml_bot
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
                    ml_bot=excluded.ml_bot
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

    def refresh_characters(self, characters: Iterable[Dict[str, Any]]) -> int:
        """Replace the character catalog with the provided API payload."""

        rows = []
        for entry in characters:
            code = entry.get("characterCode")
            name = entry.get("character")
            if not isinstance(code, int) or not isinstance(name, str):
                continue
            rows.append(
                {
                    "character_code": code,
                    "name": name,
                }
            )

        with self.cursor() as cur:
            cur.execute("DELETE FROM characters")
            if rows:
                cur.executemany(
                    """
                    INSERT INTO characters (character_code, name)
                    VALUES (:character_code, :name)
                    ON CONFLICT DO NOTHING
                    """,
                    rows,
                )
        self.connection.commit()
        return len(rows)

    def refresh_items(self, items: Iterable[Dict[str, Any]]) -> int:
        """Replace the item catalog with the provided API payload."""

        rows = []
        for entry in items:
            code = entry.get("code")
            name = entry.get("name")
            if not isinstance(code, int) or not isinstance(name, str):
                continue
            mode_type = entry.get("modeType")
            if not isinstance(mode_type, int):
                mode_type = None
            item_type = entry.get("itemType")
            if not isinstance(item_type, str):
                item_type = None
            item_grade = entry.get("itemGrade")
            if not isinstance(item_grade, str):
                item_grade = None
            is_completed_raw = entry.get("isCompletedItem")
            is_completed_item = (
                int(bool(is_completed_raw)) if is_completed_raw is not None else 0
            )

            rows.append(
                {
                    "item_code": code,
                    "name": name,
                    "mode_type": mode_type,
                    "item_type": item_type,
                    "item_grade": item_grade,
                    "is_completed_item": is_completed_item,
                }
            )

        with self.cursor() as cur:
            cur.execute("DELETE FROM items")
            if rows:
                cur.executemany(
                    """
                    INSERT INTO items (
                        item_code,
                        name,
                        mode_type,
                        item_type,
                        item_grade,
                        is_completed_item
                    )
                    VALUES (
                        :item_code,
                        :name,
                        :mode_type,
                        :item_type,
                        :item_grade,
                        :is_completed_item
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    rows,
                )
        self.connection.commit()
        return len(rows)

    def has_game(self, game_id: int) -> bool:
        with self.cursor() as cur:
            cur.execute("SELECT 1 FROM matches WHERE game_id=?", (game_id,))
            return cur.fetchone() is not None

    def get_user_last_seen(self, user_num: int) -> Optional[str]:
        with self.cursor() as cur:
            cur.execute("SELECT last_seen FROM users WHERE user_num=?", (user_num,))
            row = cur.fetchone()
            return row["last_seen"] if row else None

    def get_participants_for_game(self, game_id: int) -> Set[int]:
        with self.cursor() as cur:
            cur.execute(
                "SELECT user_num FROM user_match_stats WHERE game_id=?",
                (game_id,),
            )
            return {row["user_num"] for row in cur.fetchall()}

    def transaction(self) -> sqlite3.Connection:
        return self.connection


__all__ = ["SQLiteStore"]
