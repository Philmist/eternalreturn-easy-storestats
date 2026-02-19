"""Parquet export helpers for Eternal Return ingestion.

Writes two datasets under a base directory:
- matches/: one row per match
- participants/: one row per user per match

Both datasets are partitioned by season/server/mode/team/date to enable
efficient queries with engines like DuckDB.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple, List, DefaultDict
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

from .db import extract_uid, parse_start_time

# Fixed schemas to ensure consistent types across files
MATCH_SCHEMA = pa.schema(
    [
        pa.field("game_id", pa.int64()),
        pa.field("season_id", pa.int32()),
        pa.field("matching_mode", pa.int32()),
        pa.field("matching_team_mode", pa.int32()),
        pa.field("version_season", pa.int64()),
        pa.field("version_major", pa.int64()),
        pa.field("version_minor", pa.int64()),
        pa.field("start_dtm", pa.string()),
        pa.field("server_name", pa.string()),
    ]
)

PARTICIPANT_SCHEMA = pa.schema(
    [
        pa.field("game_id", pa.int64()),
        pa.field("uid", pa.string()),
        pa.field("nickname", pa.string()),
        pa.field("character_num", pa.int64()),
        pa.field("skin_code", pa.int64()),
        pa.field("game_rank", pa.int64()),
        pa.field("player_kill", pa.int64()),
        pa.field("player_assistant", pa.int64()),
        pa.field("monster_kill", pa.int64()),
        pa.field("mmr_gain", pa.int64()),
        pa.field("mmr_loss_entry_cost", pa.int64()),
        pa.field("victory", pa.int64()),
        pa.field("play_time", pa.int64()),
        pa.field("duration", pa.int64()),
        pa.field("damage_to_player", pa.int64()),
        pa.field("damage_from_player", pa.int64()),
        pa.field("damage_from_monster", pa.int64()),
        pa.field("damage_to_monster", pa.int64()),
        pa.field("damage_to_player_shield", pa.int64()),
        pa.field("character_level", pa.int64()),
        pa.field("best_weapon", pa.int64()),
        pa.field("best_weapon_level", pa.int64()),
        pa.field("team_number", pa.int64()),
        pa.field("premade", pa.int64()),
        pa.field("pre_made", pa.int64()),
        pa.field("premade_matching_type", pa.int64()),
        pa.field("language", pa.string()),
        pa.field("ml_bot", pa.int64()),
        pa.field("is_ml_bot", pa.int64()),
        pa.field("mlbot", pa.int64()),
        pa.field("bot_level", pa.int64()),
        pa.field("season_id", pa.int32()),
        pa.field("matching_mode", pa.int32()),
        pa.field("matching_team_mode", pa.int32()),
        pa.field("server_name", pa.string()),
        # Extended scalar stats
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
        pa.field("adaptive_force", pa.int64()),
        pa.field("adaptive_force_attack", pa.int64()),
        pa.field("adaptive_force_amplify", pa.int64()),
        pa.field("skill_amp", pa.int64()),
        pa.field("heal_amount", pa.int64()),
        pa.field("team_recover", pa.int64()),
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
        pa.field("tactical_skill_group", pa.int64()),
        pa.field("tactical_skill_level", pa.int64()),
        pa.field("tactical_skill_use_count", pa.int64()),
        pa.field("trait_first_core", pa.int64()),
        pa.field("trait_first_sub", pa.list_(pa.int64())),
        pa.field("trait_second_sub", pa.list_(pa.int64())),
        pa.field("food_craft_count", pa.list_(pa.int64())),
        pa.field("total_vf_credits", pa.list_(pa.int64())),
        pa.field("actively_gained_credits", pa.int64()),
        pa.field("used_vf_credits", pa.list_(pa.int64())),
        pa.field("sum_used_vf_credits", pa.int64()),
        pa.field("total_use_vf_credit", pa.int64()),
        # VF credit cumulative counters (scalar companions to the history arrays above)
        pa.field("credit_revival_count", pa.int64()),
        pa.field("credit_revived_others_count", pa.int64()),
        pa.field("total_gain_vf_credit", pa.int64()),
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
        pa.field("used_normal_heal_pack", pa.int64()),
        pa.field("used_reinforced_heal_pack", pa.int64()),
        pa.field("used_normal_shield_pack", pa.int64()),
        pa.field("used_reinforce_shield_pack", pa.int64()),
        pa.field("item_transferred_console", pa.list_(pa.int64())),
        pa.field("item_transferred_drone", pa.list_(pa.int64())),
        pa.field("collect_item_for_log", pa.list_(pa.int64())),
        pa.field("bought_infusion", pa.string()),
        pa.field("kiosk_exchange_credit", pa.int64()),
        pa.field("tree_of_life_spawn", pa.int64()),
        pa.field("use_gadget", pa.map_(pa.string(), pa.int64())),
        pa.field("use_guide_robot", pa.int64()),
        pa.field("guide_robot_radial", pa.int64()),
        pa.field("guide_robot_flag_ship", pa.int64()),
        pa.field("guide_robot_signature", pa.int64()),
        pa.field("use_recon_drone", pa.int64()),
        pa.field("use_emp_drone", pa.int64()),
        pa.field("active_installation", pa.map_(pa.string(), pa.int64())),
        pa.field("get_bori_reward", pa.map_(pa.string(), pa.int64())),
        pa.field("except_pre_made_team", pa.bool_()),
        pa.field("squad_rumble_rank", pa.int64()),
        pa.field("view_contribution", pa.int64()),
        pa.field("break_count", pa.int64()),
        pa.field("escape_state", pa.int64()),
        # Nested maps
        pa.field("mastery_level", pa.map_(pa.string(), pa.int64())),
        pa.field("equipment_map", pa.map_(pa.string(), pa.int64())),
        pa.field("equipment_grade_map", pa.map_(pa.string(), pa.int64())),
        pa.field("skill_level_info", pa.map_(pa.string(), pa.int64())),
        pa.field("skill_order_info", pa.map_(pa.string(), pa.int64())),
        pa.field("kill_monsters", pa.map_(pa.string(), pa.int64())),
        pa.field("credit_source", pa.map_(pa.string(), pa.float64())),
        pa.field("event_mission_result", pa.map_(pa.string(), pa.int64())),
        pa.field(
            "equip_first_item_for_log", pa.map_(pa.string(), pa.list_(pa.int64()))
        ),
        pa.field("cr_use_remote_drone", pa.int64()),
        pa.field("cr_use_upgrade_tactical_skill", pa.int64()),
        pa.field("cr_use_tree_of_life", pa.int64()),
        pa.field("cr_use_meteorite", pa.int64()),
        pa.field("cr_use_mythril", pa.int64()),
        pa.field("cr_use_force_core", pa.int64()),
        pa.field("cr_use_vf_blood_sample", pa.int64()),
        pa.field("cr_use_activation_module", pa.int64()),
        pa.field("cr_use_rootkit", pa.int64()),
        pa.field("cr_get_animal", pa.int64()),
        pa.field("cr_get_mutant", pa.int64()),
        pa.field("cr_get_phase_start", pa.int64()),
        pa.field("cr_get_kill", pa.int64()),
        pa.field("cr_get_assist", pa.int64()),
        pa.field("cr_get_time_elapsed", pa.int64()),
        pa.field("cr_get_credit_bonus", pa.int64()),
        pa.field("cr_get_by_guide_robot", pa.int64()),
        pa.field("team_elimination", pa.int64()),
        pa.field("team_down", pa.int64()),
        pa.field("team_battle_zone_down", pa.int64()),
        pa.field("team_repeat_down", pa.int64()),
        pa.field("team_down_can_not_eliminate", pa.int64()),
        pa.field("team_down_can_eliminate", pa.int64()),
        pa.field("team_repeat_down_can_not_eliminate", pa.int64()),
        pa.field("team_repeat_down_can_eliminate", pa.int64()),
        pa.field("terminate_count", pa.int64()),
        pa.field("terminate_count_can_not_eliminate", pa.int64()),
        pa.field("clutch_count", pa.int64()),
        pa.field("total_tk_per_min", pa.list_(pa.int64())),
        pa.field("enter_dimension_rift", pa.int64()),
        pa.field("enter_dimension_empowered_rift", pa.int64()),
        pa.field("enter_turbulent_rift", pa.int64()),
        pa.field("win_from_dimension_rift", pa.int64()),
        pa.field("win_from_dimension_empowered_rift", pa.int64()),
        pa.field("item_shredder_gain_vf_credit", pa.int64()),
        pa.field("remote_drone_use_vf_credit_my_self", pa.int64()),
        pa.field("remote_drone_use_vf_credit_ally", pa.int64()),
        pa.field("kiosk_from_material_use_vf_credit", pa.int64()),
        pa.field("kiosk_from_escape_key_use_vf_credit", pa.int64()),
        pa.field("kiosk_from_revival_use_vf_credit", pa.int64()),
        pa.field("tactical_skill_upgrade_use_vf_credit", pa.int64()),
        pa.field("infusion_re_roll_use_vf_credit", pa.int64()),
        pa.field("infusion_trait_use_vf_credit", pa.int64()),
        pa.field("infusion_relic_use_vf_credit", pa.int64()),
        pa.field("infusion_store_use_vf_credit", pa.int64()),
        pa.field("get_buff_cube_red", pa.int64()),
        pa.field("get_buff_cube_purple", pa.int64()),
        pa.field("get_buff_cube_green", pa.int64()),
        pa.field("get_buff_cube_gold", pa.int64()),
        pa.field("get_buff_cube_sky_blue", pa.int64()),
        pa.field("sum_get_buff_cube", pa.int64()),
        pa.field("using_default_game_option", pa.bool_()),
        pa.field("reunited_count", pa.int64()),
        pa.field("time_spent_in_briefing_room", pa.int64()),
        pa.field("main_weather", pa.int64()),
        pa.field("sub_weather", pa.int64()),
        pa.field("total_turbine_take_over", pa.int64()),
        pa.field("equipment_raw", pa.map_(pa.string(), pa.int64())),
        pa.field("is_leaving_before_credit_revival_terminate", pa.bool_()),
    ]
)


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


def _safe_str(value: Any) -> Optional[str]:
    try:
        if value is None:
            return None
        return str(value)
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


def _safe_map_list_int(value: Any) -> Optional[Dict[str, List[Optional[int]]]]:
    if value is None:
        return None
    try:
        result: Dict[str, List[Optional[int]]] = {}
        for key, items in dict(value).items():
            converted = _safe_list_int(items)
            result[str(key)] = converted or []
        return result
    except Exception:
        return None


class ParquetExporter:
    """Export match and participant rows to Parquet datasets."""

    def __init__(
        self,
        base_dir: Path,
        *,
        flush_rows: int = 10000,
        compression: Optional[str] = None,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.matches_root = self.base_dir / "matches"
        self.participants_root = self.base_dir / "participants"
        self.matches_root.mkdir(parents=True, exist_ok=True)
        self.participants_root.mkdir(parents=True, exist_ok=True)
        self._seen_matches: Set[int] = set()
        self._seen_participants: Set[Tuple[int, str]] = set()
        self._flush_rows = int(flush_rows)
        self._compression = compression
        # Buffers keyed by (season_id, server_name, matching_mode, date)
        self._buf_matches: DefaultDict[
            Tuple[Optional[int], str, Optional[int], Optional[str]],
            List[Dict[str, Any]],
        ] = defaultdict(list)
        self._buf_participants: DefaultDict[
            Tuple[Optional[int], str, Optional[int], Optional[str]],
            List[Dict[str, Any]],
        ] = defaultdict(list)
        self._file_counters: DefaultDict[
            Tuple[Optional[int], str, Optional[int], Optional[str]], int
        ] = defaultdict(int)

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

    def _partition_key(
        self, game: Dict[str, Any]
    ) -> Tuple[Optional[int], str, Optional[int], Optional[str]]:
        return (
            _safe_int(game.get("seasonId")),
            str(game.get("serverName") or ""),
            _safe_int(game.get("matchingMode")),
            _date_part(game.get("startDtm")),
        )

    def _dir_from_key(
        self, root: Path, key: Tuple[Optional[int], str, Optional[int], Optional[str]]
    ) -> Path:
        season_id, server_name, matching_mode, date = key
        parts = {
            "season_id": season_id,
            "server_name": server_name,
            "matching_mode": matching_mode,
            "date": date,
        }
        return self._partition_dir(root, parts)

    def write_from_game_payload(self, game: Dict[str, Any]) -> None:
        """Write both match and participant row(s) from a single userGame payload.

        De-duplicates using in-memory sets to avoid writing the same match or
        participant twice across pages and seed/participants flows.
        """

        game_id = _safe_int(game.get("gameId"))
        uid = extract_uid(game)
        if game_id is None or uid is None:
            return

        key = self._partition_key(game)

        # Participant buffer
        self._enqueue_participant(game)

        # Then the match one-liner (only once per game_id)
        if game_id not in self._seen_matches:
            self._seen_matches.add(game_id)
            self._enqueue_match(game, key)

    def _enqueue_match(
        self,
        game: Dict[str, Any],
        key: Tuple[Optional[int], str, Optional[int], Optional[str]],
    ) -> None:
        row = {
            "game_id": _safe_int(game.get("gameId")),
            "season_id": _safe_int(game.get("seasonId")),
            "matching_mode": _safe_int(game.get("matchingMode")),
            "matching_team_mode": _safe_int(game.get("matchingTeamMode")),
            "version_season": _safe_int(game.get("versionSeason")),
            "version_major": _safe_int(game.get("versionMajor")),
            "version_minor": _safe_int(game.get("versionMinor")),
            "start_dtm": parse_start_time(game.get("startDtm")),
            "server_name": str(game.get("serverName") or ""),
        }
        self._buf_matches[key].append(row)
        if len(self._buf_matches[key]) >= self._flush_rows:
            self._flush_partition(
                self.matches_root,
                key,
                self._buf_matches[key],
                MATCH_SCHEMA,
                prefix="matches",
            )
            self._buf_matches[key].clear()

    def _enqueue_participant(self, game: Dict[str, Any]) -> None:
        game_id = _safe_int(game.get("gameId"))
        uid = extract_uid(game)
        if game_id is None or uid is None:
            return
        dup_key = (game_id, uid)
        if dup_key in self._seen_participants:
            return
        self._seen_participants.add(dup_key)

        row = {
            # Identifiers
            "game_id": game_id,
            "uid": _safe_str(uid),
            "nickname": _safe_str(game.get("nickname")),
            # Core stats mirrored from the SQLite schema along with key combat totals
            "character_num": _safe_int(game.get("characterNum")),
            "skin_code": _safe_int(game.get("skinCode")),
            "game_rank": _safe_int(game.get("gameRank")),
            "player_kill": _safe_int(game.get("playerKill")),
            "player_assistant": _safe_int(game.get("playerAssistant")),
            "monster_kill": _safe_int(game.get("monsterKill")),
            "mmr_gain": _safe_int(
                game.get("mmrGain")
                if game.get("mmrGain") is not None
                else game.get("mmrGainInGame")
            ),
            "mmr_loss_entry_cost": _safe_int(game.get("mmrLossEntryCost")),
            "victory": _safe_int(game.get("victory")),
            "play_time": _safe_int(game.get("playTime")),
            "duration": _safe_int(game.get("duration")),
            "damage_to_player": _safe_int(game.get("damageToPlayer")),
            "damage_from_player": _safe_int(game.get("damageFromPlayer")),
            "damage_from_monster": _safe_int(game.get("damageFromMonster")),
            "damage_to_monster": _safe_int(game.get("damageToMonster")),
            "damage_to_player_shield": _safe_int(game.get("damageToPlayer_Shield")),
            "heal_amount": _safe_int(game.get("healAmount")),
            "team_recover": _safe_int(game.get("teamRecover")),
            "character_level": _safe_int(game.get("characterLevel")),
            "best_weapon": _safe_int(game.get("bestWeapon")),
            "best_weapon_level": _safe_int(game.get("bestWeaponLevel")),
            "team_number": _safe_int(game.get("teamNumber")),
            "premade": _safe_int(game.get("preMade")),
            "pre_made": _safe_int(game.get("preMade")),
            "premade_matching_type": _safe_int(game.get("premadeMatchingType")),
            "language": str(game.get("language") or ""),
            "season_id": _safe_int(game.get("seasonId")),
            "matching_mode": _safe_int(game.get("matchingMode")),
            "matching_team_mode": _safe_int(game.get("matchingTeamMode")),
            "server_name": str(game.get("serverName") or ""),
        }
        # ML bot flag may be present under different keys; standardize to int 0/1
        ml_bot_flag = game.get("mlbot")
        if ml_bot_flag is None:
            ml_bot_flag = game.get("isMLBot")
        row["ml_bot"] = int(bool(ml_bot_flag)) if ml_bot_flag is not None else 0
        row["is_ml_bot"] = _safe_int(game.get("isMLBot"))
        row["mlbot"] = _safe_int(game.get("mlbot"))
        row["bot_level"] = _safe_int(game.get("botLevel"))
        # Leaving flag may appear with different casing; prioritize any True
        leave_flags = [
            game.get("isLeavingBeforeCreditRevivalTerminate"),
            game.get("IsLeavingBeforeCreditRevivalTerminate"),
        ]
        leave_value = None
        if any(flag is True for flag in leave_flags):
            leave_value = True
        elif any(flag is False for flag in leave_flags):
            leave_value = False
        row["is_leaving_before_credit_revival_terminate"] = leave_value

        # Extended scalar stats
        row.update(
            {
                "watch_time": _safe_int(game.get("watchTime")),
                "total_time": _safe_int(game.get("totalTime")),
                "survivable_time": _safe_int(game.get("survivableTime")),
                "bot_added": _safe_int(game.get("botAdded")),
                "bot_remain": _safe_int(game.get("botRemain")),
                "restricted_area_accelerated": _safe_int(
                    game.get("restrictedAreaAccelerated")
                ),
                "safe_areas": _safe_int(game.get("safeAreas")),
                "team_kill": _safe_int(game.get("teamKill")),
                "total_field_kill": _safe_int(game.get("totalFieldKill")),
                "account_level": _safe_int(game.get("accountLevel")),
                "rank_point": _safe_int(game.get("rankPoint")),
                "mmr_avg": _safe_int(game.get("mmrAvg")),
                "match_size": _safe_int(game.get("matchSize")),
                "gained_normal_mmr_k_factor": _safe_float(
                    game.get("gainedNormalMmrKFactor")
                ),
                # Combat
                "max_hp": _safe_int(game.get("maxHp")),
                "max_sp": _safe_int(game.get("maxSp")),
                "hp_regen": _safe_float(game.get("hpRegen")),
                "sp_regen": _safe_float(game.get("spRegen")),
                "attack_power": _safe_int(game.get("attackPower")),
                "defense": _safe_int(game.get("defense")),
                "attack_speed": _safe_float(game.get("attackSpeed")),
                "move_speed": _safe_float(game.get("moveSpeed")),
                "out_of_combat_move_speed": _safe_float(
                    game.get("outOfCombatMoveSpeed")
                ),
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
                "adaptive_force": _safe_int(game.get("adaptiveForce")),
                "adaptive_force_attack": _safe_int(game.get("adaptiveForceAttack")),
                "adaptive_force_amplify": _safe_int(game.get("adaptiveForceAmplify")),
                "skill_amp": _safe_int(game.get("skillAmp")),
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
                "remove_surveillance_camera": _safe_int(
                    game.get("removeSurveillanceCamera")
                ),
                "remove_telephoto_camera": _safe_int(game.get("removeTelephotoCamera")),
                "use_hyper_loop": _safe_int(game.get("useHyperLoop")),
                "use_security_console": _safe_int(game.get("useSecurityConsole")),
                "tactical_skill_group": _safe_int(game.get("tacticalSkillGroup")),
                "tactical_skill_level": _safe_int(game.get("tacticalSkillLevel")),
                "tactical_skill_use_count": _safe_int(
                    game.get("tacticalSkillUseCount")
                ),
                "trait_first_core": _safe_int(game.get("traitFirstCore")),
                "trait_first_sub": _safe_list_int(game.get("traitFirstSub")),
                "trait_second_sub": _safe_list_int(game.get("traitSecondSub")),
                "food_craft_count": _safe_list_int(game.get("foodCraftCount")),
                "total_vf_credits": _safe_list_int(game.get("totalVFCredits")),
                "actively_gained_credits": _safe_int(game.get("activelyGainedCredits")),
                "used_vf_credits": _safe_list_int(game.get("usedVFCredits")),
                "sum_used_vf_credits": _safe_int(game.get("sumUsedVFCredits")),
                "total_use_vf_credit": _safe_int(game.get("totalUseVFCredit")),
                # Scalar rollups corresponding to the VF credit histories above
                "credit_revival_count": _safe_int(game.get("creditRevivalCount")),
                "credit_revived_others_count": _safe_int(
                    game.get("creditRevivedOthersCount")
                ),
                "total_gain_vf_credit": _safe_int(game.get("totalGainVFCredit")),
                "craft_mythic": _safe_int(game.get("craftMythic")),
                "player_deaths": _safe_int(game.get("playerDeaths")),
                "kill_gamma": bool(game.get("killGamma"))
                if game.get("killGamma") is not None
                else None,
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
                "used_normal_heal_pack": _safe_int(game.get("usedNormalHealPack")),
                "used_reinforced_heal_pack": _safe_int(
                    game.get("usedReinforcedHealPack")
                ),
                "used_normal_shield_pack": _safe_int(game.get("usedNormalShieldPack")),
                "used_reinforce_shield_pack": _safe_int(
                    game.get("usedReinforceShieldPack")
                ),
                "item_transferred_console": _safe_list_int(
                    game.get("itemTransferredConsole")
                ),
                "item_transferred_drone": _safe_list_int(
                    game.get("itemTransferredDrone")
                ),
                "collect_item_for_log": _safe_list_int(game.get("collectItemForLog")),
                "bought_infusion": _safe_str(game.get("boughtInfusion")),
                "kiosk_exchange_credit": _safe_int(game.get("kioskExchangeCredit")),
                "tree_of_life_spawn": _safe_int(game.get("treeOfLifeSpawn")),
                "use_gadget": game.get("useGadget") or None,
                "use_guide_robot": _safe_int(game.get("useGuideRobot")),
                "guide_robot_radial": _safe_int(game.get("guideRobotRadial")),
                "guide_robot_flag_ship": _safe_int(game.get("guideRobotFlagShip")),
                "guide_robot_signature": _safe_int(game.get("guideRobotSignature")),
                "use_recon_drone": _safe_int(game.get("useReconDrone")),
                "use_emp_drone": _safe_int(game.get("useEmpDrone")),
                "active_installation": game.get("activeInstallation") or None,
                "get_bori_reward": game.get("getBoriReward") or None,
                "except_pre_made_team": game.get("exceptPreMadeTeam"),
                "squad_rumble_rank": _safe_int(game.get("squadRumbleRank")),
                "view_contribution": _safe_int(game.get("viewContribution")),
                "break_count": _safe_int(game.get("breakCount")),
                "escape_state": _safe_int(game.get("escapeState")),
            }
        )

        # Nested maps (equipFirstItem log normalised to map[str, list[int]])
        row["mastery_level"] = game.get("masteryLevel") or None
        row["equipment_map"] = game.get("equipment") or None
        row["equipment_grade_map"] = game.get("equipmentGrade") or None
        row["equip_first_item_for_log"] = _safe_map_list_int(
            game.get("equipFirstItemForLog")
        )
        row["skill_level_info"] = game.get("skillLevelInfo") or None
        row["skill_order_info"] = game.get("skillOrderInfo") or None
        row["kill_monsters"] = game.get("killMonsters") or None
        row["credit_source"] = game.get("creditSource") or None
        row["event_mission_result"] = game.get("eventMissionResult") or None
        row["equipment_raw"] = game.get("equipment") or None
        row["cr_use_remote_drone"] = _safe_int(game.get("crUseRemoteDrone"))
        row["cr_use_upgrade_tactical_skill"] = _safe_int(
            game.get("crUseUpgradeTacticalSkill")
        )
        row["cr_use_tree_of_life"] = _safe_int(game.get("crUseTreeOfLife"))
        row["cr_use_meteorite"] = _safe_int(game.get("crUseMeteorite"))
        row["cr_use_mythril"] = _safe_int(game.get("crUseMythril"))
        row["cr_use_force_core"] = _safe_int(game.get("crUseForceCore"))
        row["cr_use_vf_blood_sample"] = _safe_int(game.get("crUseVFBloodSample"))
        row["cr_use_activation_module"] = _safe_int(game.get("crUseActivationModule"))
        row["cr_use_rootkit"] = _safe_int(game.get("crUseRootkit"))
        row["cr_get_animal"] = _safe_int(game.get("crGetAnimal"))
        row["cr_get_mutant"] = _safe_int(game.get("crGetMutant"))
        row["cr_get_phase_start"] = _safe_int(game.get("crGetPhaseStart"))
        row["cr_get_kill"] = _safe_int(game.get("crGetKill"))
        row["cr_get_assist"] = _safe_int(game.get("crGetAssist"))
        row["cr_get_time_elapsed"] = _safe_int(game.get("crGetTimeElapsed"))
        row["cr_get_credit_bonus"] = _safe_int(game.get("crGetCreditBonus"))
        row["cr_get_by_guide_robot"] = _safe_int(game.get("crGetByGuideRobot"))
        row["team_elimination"] = _safe_int(game.get("teamElimination"))
        row["team_down"] = _safe_int(game.get("teamDown"))
        row["team_battle_zone_down"] = _safe_int(game.get("teamBattleZoneDown"))
        row["team_repeat_down"] = _safe_int(game.get("teamRepeatDown"))
        row["team_down_can_not_eliminate"] = _safe_int(
            game.get("teamDownCanNotEliminate")
        )
        row["team_down_can_eliminate"] = _safe_int(game.get("teamDownCanEliminate"))
        row["team_repeat_down_can_not_eliminate"] = _safe_int(
            game.get("teamRepeatDownCanNotEliminate")
        )
        row["team_repeat_down_can_eliminate"] = _safe_int(
            game.get("teamRepeatDownCanEliminate")
        )
        row["terminate_count"] = _safe_int(game.get("terminateCount"))
        row["terminate_count_can_not_eliminate"] = _safe_int(
            game.get("terminateCountCanNotEliminate")
        )
        row["clutch_count"] = _safe_int(game.get("clutchCount"))
        row["total_tk_per_min"] = _safe_list_int(game.get("totalTKPerMin"))
        row["enter_dimension_rift"] = _safe_int(game.get("enterDimensionRift"))
        row["enter_dimension_empowered_rift"] = _safe_int(
            game.get("enterDimensionEmpoweredRift")
        )
        row["enter_turbulent_rift"] = _safe_int(game.get("enterTurbulentRift"))
        row["win_from_dimension_rift"] = _safe_int(game.get("winFromDimensionRift"))
        row["win_from_dimension_empowered_rift"] = _safe_int(
            game.get("winFromDimensionEmpoweredRift")
        )
        row["remote_drone_use_vf_credit_my_self"] = _safe_int(
            game.get("remoteDroneUseVFCreditMySelf")
        )
        row["remote_drone_use_vf_credit_ally"] = _safe_int(
            game.get("remoteDroneUseVFCreditAlly")
        )
        row["kiosk_from_material_use_vf_credit"] = _safe_int(
            game.get("kioskFromMaterialUseVFCredit")
        )
        row["kiosk_from_escape_key_use_vf_credit"] = _safe_int(
            game.get("kioskFromEscapeKeyUseVFCredit")
        )
        row["kiosk_from_revival_use_vf_credit"] = _safe_int(
            game.get("kioskFromRevivalUseVFCredit")
        )
        row["tactical_skill_upgrade_use_vf_credit"] = _safe_int(
            game.get("tacticalSkillUpgradeUseVFCredit")
        )
        row["infusion_re_roll_use_vf_credit"] = _safe_int(
            game.get("infusionReRollUseVFCredit")
        )
        row["infusion_trait_use_vf_credit"] = _safe_int(
            game.get("infusionTraitUseVFCredit")
        )
        row["infusion_relic_use_vf_credit"] = _safe_int(
            game.get("infusionRelicUseVFCredit")
        )
        row["infusion_store_use_vf_credit"] = _safe_int(
            game.get("infusionStoreUseVFCredit")
        )
        row["get_buff_cube_red"] = _safe_int(game.get("getBuffCubeRed"))
        row["get_buff_cube_purple"] = _safe_int(game.get("getBuffCubePurple"))
        row["get_buff_cube_green"] = _safe_int(game.get("getBuffCubeGreen"))
        row["get_buff_cube_gold"] = _safe_int(game.get("getBuffCubeGold"))
        row["get_buff_cube_sky_blue"] = _safe_int(game.get("getBuffCubeSkyBlue"))
        row["sum_get_buff_cube"] = _safe_int(game.get("sumGetBuffCube"))
        row["using_default_game_option"] = (
            bool(game.get("usingDefaultGameOption"))
            if game.get("usingDefaultGameOption") is not None
            else None
        )
        row["reunited_count"] = _safe_int(game.get("reunitedCount"))
        row["time_spent_in_briefing_room"] = _safe_int(
            game.get("timeSpentInBriefingRoom")
        )
        row["item_shredder_gain_vf_credit"] = _safe_int(
            game.get("itemShredderGainVFCredit")
        )
        row["main_weather"] = _safe_int(game.get("mainWeather"))
        row["sub_weather"] = _safe_int(game.get("subWeather"))
        row["total_turbine_take_over"] = _safe_int(game.get("totalTurbineTakeOver"))

        self._buf_participants[self._partition_key(game)].append(row)
        if len(self._buf_participants[self._partition_key(game)]) >= self._flush_rows:
            self._flush_partition(
                self.participants_root,
                self._partition_key(game),
                self._buf_participants[self._partition_key(game)],
                PARTICIPANT_SCHEMA,
                prefix="participants",
            )
            self._buf_participants[self._partition_key(game)].clear()

    def _flush_partition(
        self,
        root: Path,
        key: Tuple[Optional[int], str, Optional[int], Optional[str]],
        rows: List[Dict[str, Any]],
        schema: pa.Schema,
        *,
        prefix: str,
    ) -> None:
        if not rows:
            return
        dirpath = self._dir_from_key(root, key)
        # Unique filename per flush
        self._file_counters[key] += 1
        filename = dirpath / f"{prefix}-part-{self._file_counters[key]:05d}.parquet"
        columns = {name: [r.get(name) for r in rows] for name in schema.names}
        table = pa.table(columns, schema=schema)
        pq.write_table(
            table,
            filename,
            compression=self._compression,
            use_dictionary=["server_name"],
        )

    def close(self) -> None:
        # Flush remaining buffers
        for key, rows in list(self._buf_matches.items()):
            if rows:
                self._flush_partition(
                    self.matches_root, key, rows, MATCH_SCHEMA, prefix="matches"
                )
                rows.clear()
        for key, rows in list(self._buf_participants.items()):
            if rows:
                self._flush_partition(
                    self.participants_root,
                    key,
                    rows,
                    PARTICIPANT_SCHEMA,
                    prefix="participants",
                )
                rows.clear()


__all__ = ["ParquetExporter"]
