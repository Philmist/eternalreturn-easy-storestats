"""Parquet export helpers for Eternal Return ingestion.

Writes two datasets under a base directory:
- matches/: one row per match
- participants/: one row per user per match

Both datasets are partitioned by season/server/mode/team/date to enable
efficient queries with engines like DuckDB.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple, List

import pyarrow as pa
import pyarrow.parquet as pq

from .db import parse_start_time

# Fixed schemas to ensure consistent types across files
MATCH_SCHEMA = pa.schema([
    pa.field("game_id", pa.int64()),
    pa.field("version_major", pa.int64()),
    pa.field("version_minor", pa.int64()),
    pa.field("start_dtm", pa.string()),
    pa.field("duration", pa.int64()),
])

PARTICIPANT_SCHEMA = pa.schema([
    pa.field("game_id", pa.int64()),
    pa.field("user_num", pa.int64()),
    pa.field("character_num", pa.int64()),
    pa.field("skin_code", pa.int64()),
    pa.field("game_rank", pa.int64()),
    pa.field("player_kill", pa.int64()),
    pa.field("player_assistant", pa.int64()),
    pa.field("monster_kill", pa.int64()),
    pa.field("mmr_after", pa.int64()),
    pa.field("mmr_gain", pa.int64()),
    pa.field("mmr_loss_entry_cost", pa.int64()),
    pa.field("victory", pa.int64()),
    pa.field("play_time", pa.int64()),
    pa.field("damage_to_player", pa.int64()),
    pa.field("character_level", pa.int64()),
    pa.field("best_weapon", pa.int64()),
    pa.field("best_weapon_level", pa.int64()),
    pa.field("team_number", pa.int64()),
    pa.field("premade", pa.int64()),
    pa.field("language", pa.string()),
    pa.field("ml_bot", pa.int64()),
    # Extended scalar stats
    pa.field("mmr_before", pa.int64()),
    pa.field("watch_time", pa.int64()),
    pa.field("total_time", pa.int64()),
    pa.field("survivable_time", pa.int64()),
    pa.field("bot_added", pa.int64()),
    pa.field("bot_remain", pa.int64()),
    pa.field("restricted_area_accelerated", pa.int64()),
    pa.field("safe_areas", pa.int64()),
    pa.field("team_kill", pa.int64()),
    pa.field("total_field_kill", pa.int64()),
    pa.field("account_level", pa.int64()),
    pa.field("rank_point", pa.int64()),
    pa.field("mmr_avg", pa.int64()),
    pa.field("match_size", pa.int64()),
    pa.field("gained_normal_mmr_k_factor", pa.float64()),
    # Combat stats (subset)
    pa.field("max_hp", pa.int64()),
    pa.field("max_sp", pa.int64()),
    pa.field("hp_regen", pa.float64()),
    pa.field("sp_regen", pa.float64()),
    pa.field("attack_power", pa.int64()),
    pa.field("defense", pa.int64()),
    pa.field("attack_speed", pa.float64()),
    pa.field("move_speed", pa.float64()),
    pa.field("out_of_combat_move_speed", pa.float64()),
    pa.field("sight_range", pa.float64()),
    pa.field("attack_range", pa.float64()),
    pa.field("critical_strike_chance", pa.float64()),
    pa.field("critical_strike_damage", pa.float64()),
    pa.field("cool_down_reduction", pa.float64()),
    pa.field("life_steal", pa.float64()),
    pa.field("normal_life_steal", pa.float64()),
    pa.field("skill_life_steal", pa.float64()),
    pa.field("amplifier_to_monster", pa.float64()),
    pa.field("trap_damage", pa.float64()),
    # Event / misc
    pa.field("bonus_coin", pa.int64()),
    pa.field("gain_exp", pa.int64()),
    pa.field("base_exp", pa.int64()),
    pa.field("bonus_exp", pa.int64()),
    pa.field("killer_user_num", pa.int64()),
    pa.field("killer", pa.string()),
    pa.field("kill_detail", pa.string()),
    pa.field("cause_of_death", pa.string()),
    pa.field("place_of_death", pa.string()),
    pa.field("killer_character", pa.string()),
    pa.field("killer_weapon", pa.string()),
    pa.field("killer_user_num2", pa.int64()),
    pa.field("killer_user_num3", pa.int64()),
    pa.field("fishing_count", pa.int64()),
    pa.field("use_emoticon_count", pa.int64()),
    pa.field("expire_dtm", pa.string()),
    pa.field("route_id_of_start", pa.int64()),
    pa.field("route_slot_id", pa.int64()),
    pa.field("place_of_start", pa.string()),
    pa.field("give_up", pa.int64()),
    pa.field("team_spectator", pa.int64()),
    pa.field("add_surveillance_camera", pa.int64()),
    pa.field("add_telephoto_camera", pa.int64()),
    pa.field("remove_surveillance_camera", pa.int64()),
    pa.field("remove_telephoto_camera", pa.int64()),
    pa.field("use_hyper_loop", pa.int64()),
    pa.field("use_security_console", pa.int64()),
    pa.field("trait_first_core", pa.int64()),
    pa.field("trait_first_sub", pa.list_(pa.int64())),
    pa.field("trait_second_sub", pa.list_(pa.int64())),
    pa.field("food_craft_count", pa.list_(pa.int64())),
    pa.field("total_vf_credits", pa.list_(pa.int64())),
    pa.field("actively_gained_credits", pa.int64()),
    pa.field("used_vf_credits", pa.list_(pa.int64())),
    pa.field("sum_used_vf_credits", pa.int64()),
    pa.field("craft_mythic", pa.int64()),
    pa.field("player_deaths", pa.int64()),
    pa.field("kill_gamma", pa.bool_()),
    pa.field("scored_point", pa.list_(pa.int64())),
    pa.field("kill_details", pa.string()),
    pa.field("death_details", pa.string()),
    pa.field("kills_phase_one", pa.int64()),
    pa.field("kills_phase_two", pa.int64()),
    pa.field("kills_phase_three", pa.int64()),
    pa.field("deaths_phase_one", pa.int64()),
    pa.field("deaths_phase_two", pa.int64()),
    pa.field("deaths_phase_three", pa.int64()),
    pa.field("used_pair_loop", pa.int64()),
    pa.field("cc_time_to_player", pa.float64()),
    pa.field("item_transferred_console", pa.list_(pa.int64())),
    # Nested maps
    pa.field("mastery_level", pa.map_(pa.string(), pa.int64())),
    pa.field("equipment_map", pa.map_(pa.string(), pa.int64())),
    pa.field("equipment_grade_map", pa.map_(pa.string(), pa.int64())),
    pa.field("skill_level_info", pa.map_(pa.string(), pa.int64())),
    pa.field("skill_order_info", pa.map_(pa.string(), pa.int64())),
    pa.field("kill_monsters", pa.map_(pa.string(), pa.int64())),
    pa.field("credit_source", pa.map_(pa.string(), pa.float64())),
    pa.field("event_mission_result", pa.map_(pa.string(), pa.int64())),
])


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _date_part(start_dtm: Optional[str]) -> Optional[str]:
    iso = parse_start_time(start_dtm)
    if not iso:
        return None
    return str(iso)[:10]


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _safe_list_int(value: Any) -> Optional[List[Optional[int]]]:
    if value is None:
        return None
    try:
        return [(_safe_int(v)) for v in list(value)]
    except Exception:
        return None


def _safe_list_float(value: Any) -> Optional[List[Optional[float]]]:
    if value is None:
        return None
    try:
        return [(_safe_float(v)) for v in list(value)]
    except Exception:
        return None


class ParquetExporter:
    """Export match and participant rows to Parquet datasets."""

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = Path(base_dir)
        self.matches_root = self.base_dir / "matches"
        self.participants_root = self.base_dir / "participants"
        self.matches_root.mkdir(parents=True, exist_ok=True)
        self.participants_root.mkdir(parents=True, exist_ok=True)
        self._seen_matches: Set[int] = set()
        self._seen_participants: Set[Tuple[int, int]] = set()

    def _partition_dir(self, root: Path, row: Dict[str, Any]) -> Path:
        def as_str(v: Any) -> str:
            return "null" if v is None else str(v)
        parts = [
            f"season_id={as_str(row.get('season_id'))}",
            f"server_name={as_str(row.get('server_name'))}",
            f"matching_mode={as_str(row.get('matching_mode'))}",
            f"date={as_str(row.get('date'))}",
        ]
        d = root
        for p in parts:
            d = d / p
        d.mkdir(parents=True, exist_ok=True)
        return d

    def write_from_game_payload(self, game: Dict[str, Any]) -> None:
        """Write both match and participant row(s) from a single userGame payload.

        De-duplicates using in-memory sets to avoid writing the same match or
        participant twice across pages and seed/participants flows.
        """

        game_id = _safe_int(game.get("gameId"))
        user_num = _safe_int(game.get("userNum"))
        if game_id is None or user_num is None:
            return

        # Always attempt participant first; safe due to de-dup set
        self._write_participant(game)

        # Then the match one-liner (only once per game_id)
        if game_id not in self._seen_matches:
            self._seen_matches.add(game_id)
            self._write_match(game)

    def _write_match(self, game: Dict[str, Any]) -> None:
        row = {
            "game_id": _safe_int(game.get("gameId")),
            "version_major": _safe_int(game.get("versionMajor")),
            "version_minor": _safe_int(game.get("versionMinor")),
            "start_dtm": parse_start_time(game.get("startDtm")),
            "duration": _safe_int(game.get("duration")),
        }
        table = pa.table({k: [row.get(k)] for k in MATCH_SCHEMA.names}, schema=MATCH_SCHEMA)
        # Build partition context from the game payload (season/server/mode/date)
        part_row = {
            "season_id": _safe_int(game.get("seasonId")),
            "server_name": str(game.get("serverName") or ""),
            "matching_mode": _safe_int(game.get("matchingMode")),
            "date": _date_part(game.get("startDtm")),
        }
        dirpath = self._partition_dir(self.matches_root, part_row)
        filename = dirpath / f"part-{row['game_id']}.parquet"
        if not filename.exists():
            pq.write_table(table, filename)

    def _write_participant(self, game: Dict[str, Any]) -> None:
        game_id = _safe_int(game.get("gameId"))
        user_num = _safe_int(game.get("userNum"))
        key = (game_id or -1, user_num or -1)
        if key in self._seen_participants:
            return
        self._seen_participants.add(key)

        row = {
            # Identifiers
            "game_id": game_id,
            "user_num": user_num,
            # Stats (subset aligned with SQLite schema)
            "character_num": _safe_int(game.get("characterNum")),
            "skin_code": _safe_int(game.get("skinCode")),
            "game_rank": _safe_int(game.get("gameRank")),
            "player_kill": _safe_int(game.get("playerKill")),
            "player_assistant": _safe_int(game.get("playerAssistant")),
            "monster_kill": _safe_int(game.get("monsterKill")),
            "mmr_after": _safe_int(game.get("mmrAfter")),
            "mmr_gain": _safe_int(game.get("mmrGain")),
            "mmr_loss_entry_cost": _safe_int(game.get("mmrLossEntryCost")),
            "victory": _safe_int(game.get("victory")),
            "play_time": _safe_int(game.get("playTime")),
            "damage_to_player": _safe_int(game.get("damageToPlayer")),
            "character_level": _safe_int(game.get("characterLevel")),
            "best_weapon": _safe_int(game.get("bestWeapon")),
            "best_weapon_level": _safe_int(game.get("bestWeaponLevel")),
            "team_number": _safe_int(game.get("teamNumber")),
            "premade": _safe_int(game.get("preMade")),
            "language": str(game.get("language") or ""),
        }
        # ML bot flag may be present under different keys; standardize to int 0/1
        ml_bot_flag = game.get("mlbot")
        if ml_bot_flag is None:
            ml_bot_flag = game.get("isMLBot")
        row["ml_bot"] = int(bool(ml_bot_flag)) if ml_bot_flag is not None else 0

        # Extended scalar stats
        row.update({
            "mmr_before": _safe_int(game.get("mmrBefore")),
            "watch_time": _safe_int(game.get("watchTime")),
            "total_time": _safe_int(game.get("totalTime")),
            "survivable_time": _safe_int(game.get("survivableTime")),
            "bot_added": _safe_int(game.get("botAdded")),
            "bot_remain": _safe_int(game.get("botRemain")),
            "restricted_area_accelerated": _safe_int(game.get("restrictedAreaAccelerated")),
            "safe_areas": _safe_int(game.get("safeAreas")),
            "team_kill": _safe_int(game.get("teamKill")),
            "total_field_kill": _safe_int(game.get("totalFieldKill")),
            "account_level": _safe_int(game.get("accountLevel")),
            "rank_point": _safe_int(game.get("rankPoint")),
            "mmr_avg": _safe_int(game.get("mmrAvg")),
            "match_size": _safe_int(game.get("matchSize")),
            "gained_normal_mmr_k_factor": _safe_float(game.get("gainedNormalMmrKFactor")),
            # Combat
            "max_hp": _safe_int(game.get("maxHp")),
            "max_sp": _safe_int(game.get("maxSp")),
            "hp_regen": _safe_float(game.get("hpRegen")),
            "sp_regen": _safe_float(game.get("spRegen")),
            "attack_power": _safe_int(game.get("attackPower")),
            "defense": _safe_int(game.get("defense")),
            "attack_speed": _safe_float(game.get("attackSpeed")),
            "move_speed": _safe_float(game.get("moveSpeed")),
            "out_of_combat_move_speed": _safe_float(game.get("outOfCombatMoveSpeed")),
            "sight_range": _safe_float(game.get("sightRange")),
            "attack_range": _safe_float(game.get("attackRange")),
            "critical_strike_chance": _safe_float(game.get("criticalStrikeChance")),
            "critical_strike_damage": _safe_float(game.get("criticalStrikeDamage")),
            "cool_down_reduction": _safe_float(game.get("coolDownReduction")),
            "life_steal": _safe_float(game.get("lifeSteal")),
            "normal_life_steal": _safe_float(game.get("normalLifeSteal")),
            "skill_life_steal": _safe_float(game.get("skillLifeSteal")),
            "amplifier_to_monster": _safe_float(game.get("amplifierToMonster")),
            "trap_damage": _safe_float(game.get("trapDamage")),
            # Event
            "bonus_coin": _safe_int(game.get("bonusCoin")),
            "gain_exp": _safe_int(game.get("gainExp")),
            "base_exp": _safe_int(game.get("baseExp")),
            "bonus_exp": _safe_int(game.get("bonusExp")),
            "killer_user_num": _safe_int(game.get("killerUserNum")),
            "killer": str(game.get("killer") or ""),
            "kill_detail": str(game.get("killDetail") or ""),
            "cause_of_death": str(game.get("causeOfDeath") or ""),
            "place_of_death": str(game.get("placeOfDeath") or ""),
            "killer_character": str(game.get("killerCharacter") or ""),
            "killer_weapon": str(game.get("killerWeapon") or ""),
            "killer_user_num2": _safe_int(game.get("killerUserNum2")),
            "killer_user_num3": _safe_int(game.get("killerUserNum3")),
            "fishing_count": _safe_int(game.get("fishingCount")),
            "use_emoticon_count": _safe_int(game.get("useEmoticonCount")),
            "expire_dtm": parse_start_time(game.get("expireDtm")),
            "route_id_of_start": _safe_int(game.get("routeIdOfStart")),
            "route_slot_id": _safe_int(game.get("routeSlotId")),
            "place_of_start": str(game.get("placeOfStart") or ""),
            "give_up": _safe_int(game.get("giveUp")),
            "team_spectator": _safe_int(game.get("teamSpectator")),
            "add_surveillance_camera": _safe_int(game.get("addSurveillanceCamera")),
            "add_telephoto_camera": _safe_int(game.get("addTelephotoCamera")),
            "remove_surveillance_camera": _safe_int(game.get("removeSurveillanceCamera")),
            "remove_telephoto_camera": _safe_int(game.get("removeTelephotoCamera")),
            "use_hyper_loop": _safe_int(game.get("useHyperLoop")),
            "use_security_console": _safe_int(game.get("useSecurityConsole")),
            "trait_first_core": _safe_int(game.get("traitFirstCore")),
            "trait_first_sub": _safe_list_int(game.get("traitFirstSub")),
            "trait_second_sub": _safe_list_int(game.get("traitSecondSub")),
            "food_craft_count": _safe_list_int(game.get("foodCraftCount")),
            "total_vf_credits": _safe_list_int(game.get("totalVFCredits")),
            "actively_gained_credits": _safe_int(game.get("activelyGainedCredits")),
            "used_vf_credits": _safe_list_int(game.get("usedVFCredits")),
            "sum_used_vf_credits": _safe_int(game.get("sumUsedVFCredits")),
            "craft_mythic": _safe_int(game.get("craftMythic")),
            "player_deaths": _safe_int(game.get("playerDeaths")),
            "kill_gamma": bool(game.get("killGamma")) if game.get("killGamma") is not None else None,
            "scored_point": _safe_list_int(game.get("scoredPoint")),
            "kill_details": str(game.get("killDetails") or ""),
            "death_details": str(game.get("deathDetails") or ""),
            "kills_phase_one": _safe_int(game.get("killsPhaseOne")),
            "kills_phase_two": _safe_int(game.get("killsPhaseTwo")),
            "kills_phase_three": _safe_int(game.get("killsPhaseThree")),
            "deaths_phase_one": _safe_int(game.get("deathsPhaseOne")),
            "deaths_phase_two": _safe_int(game.get("deathsPhaseTwo")),
            "deaths_phase_three": _safe_int(game.get("deathsPhaseThree")),
            "used_pair_loop": _safe_int(game.get("usedPairLoop")),
            "cc_time_to_player": _safe_float(game.get("ccTimeToPlayer")),
            "item_transferred_console": _safe_list_int(game.get("itemTransferredConsole")),
        })

        # Nested maps
        row["mastery_level"] = game.get("masteryLevel") or None
        row["equipment_map"] = game.get("equipment") or None
        row["equipment_grade_map"] = game.get("equipmentGrade") or None
        row["skill_level_info"] = game.get("skillLevelInfo") or None
        row["skill_order_info"] = game.get("skillOrderInfo") or None
        row["kill_monsters"] = game.get("killMonsters") or None
        row["credit_source"] = game.get("creditSource") or None
        row["event_mission_result"] = game.get("eventMissionResult") or None

        table = pa.table({k: [row.get(k)] for k in PARTICIPANT_SCHEMA.names}, schema=PARTICIPANT_SCHEMA)
        # Build partition directory using original game dict fields
        part_row = {
            "season_id": _safe_int(game.get("seasonId")),
            "server_name": str(game.get("serverName") or ""),
            "matching_mode": _safe_int(game.get("matchingMode")),
            "date": _date_part(game.get("startDtm")),
        }
        dirpath = self._partition_dir(self.participants_root, part_row)
        filename = dirpath / f"part-{row['game_id']}-{row['user_num']}.parquet"
        if not filename.exists():
            pq.write_table(table, filename)


__all__ = ["ParquetExporter"]
