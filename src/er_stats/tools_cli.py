"""Utility CLI for maintenance/ops tasks (no DB required).

Commands
- parquet-compact: compact and compress a hive-partitioned Parquet dataset.
- parquet-rebuild: rebuild Parquet datasets with match-level consistency.
- sqlite-prune: delete old SQLite matches and track tombstones.
"""

from __future__ import annotations

import argparse
import datetime as dt
import math
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

from .config import ConfigError, load_ingest_config
from .db import SQLiteStore, parse_start_time


@dataclass
class _MatchChoice:
    row: Dict[str, Any]
    score: Tuple[int, int]
    partition_date: Optional[str]


def _value_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value != ""
    if isinstance(value, float) and math.isnan(value):
        return False
    return True


def _match_score(
    row: Dict[str, Any],
    rep_cols: Iterable[str],
    all_cols: Iterable[str],
) -> Tuple[int, int]:
    rep_score = sum(_value_present(row.get(col)) for col in rep_cols)
    non_null = sum(_value_present(row.get(col)) for col in all_cols)
    return rep_score, non_null


def _date_part(value: Optional[str]) -> Optional[str]:
    iso = parse_start_time(value)
    if not iso:
        return None
    return str(iso)[:10]


def _parse_datetime_or_date(value: str) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        try:
            date_value = dt.date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"Invalid datetime/date format: {value}") from exc
        return dt.datetime.combine(date_value, dt.time(0, 0), tzinfo=dt.timezone.utc)
    if parsed.tzinfo is None:
        raise ValueError(f"Timezone offset is required for datetime '{value}'.")
    return parsed


def _resolve_prune_paths(
    args: argparse.Namespace,
) -> tuple[Optional[Path], Optional[Path], Optional[int]]:
    if args.config is None:
        return args.db, args.parquet_dir, None
    try:
        ingest_config = load_ingest_config(args.config)
    except ConfigError as exc:
        print(f"{exc}", file=sys.stderr)
        return None, None, 2
    ingest_table = ingest_config.get("ingest", {})
    db_value = ingest_table.get("db_path")
    parquet_value = ingest_table.get("parquet_dir")
    config_db = Path(db_value) if isinstance(db_value, str) else None
    config_parquet = Path(parquet_value) if isinstance(parquet_value, str) else None
    if config_db is not None:
        if args.db is not None and args.db != config_db:
            print(
                "--db is ignored because config ingest.db_path is set.",
                file=sys.stderr,
            )
        db_path = config_db
    else:
        db_path = args.db
    if config_parquet is not None:
        if args.parquet_dir is not None and args.parquet_dir != config_parquet:
            print(
                "--parquet-dir is ignored because config ingest.parquet_dir is set.",
                file=sys.stderr,
            )
        parquet_dir = config_parquet
    else:
        parquet_dir = args.parquet_dir
    return db_path, parquet_dir, None


def _iter_rows(dataset: Any, columns: list[str]) -> Iterable[Dict[str, Any]]:
    scanner = dataset.scanner(columns=columns)
    for batch in scanner.to_batches():
        for row in batch.to_pylist():
            yield row


def _strip_date(schema: Any) -> Any:
    if "date" not in schema.names:
        return schema
    idx = schema.get_field_index("date")
    return schema.remove(idx)


def _partition_key(
    row: Dict[str, Any],
    date_value: Optional[str],
) -> Tuple[Any, str, Any, Any]:
    server_name = row.get("server_name")
    if server_name is None:
        server_name = ""
    return (
        row.get("season_id"),
        str(server_name),
        row.get("matching_mode"),
        date_value,
    )


def _apply_match_context(row: Dict[str, Any], match_row: Dict[str, Any]) -> None:
    for field in ("season_id", "matching_mode", "matching_team_mode"):
        if match_row.get(field) is not None:
            row[field] = match_row[field]
    server_name = match_row.get("server_name")
    if isinstance(server_name, str) and server_name != "":
        row["server_name"] = server_name


class _PartitionedWriter:
    def __init__(
        self,
        base_dir: Path,
        schema: Any,
        *,
        max_rows_per_file: int,
        compression: Optional[str],
        pa_module: Any,
        pq_module: Any,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.schema = schema
        self.max_rows_per_file = int(max_rows_per_file)
        self.compression = compression
        self._pa = pa_module
        self._pq = pq_module
        self._buffers: dict[Tuple[Any, str, Any, Any], list[Dict[str, Any]]] = (
            defaultdict(list)
        )
        self._file_counters: dict[Tuple[Any, str, Any, Any], int] = defaultdict(int)

    def _partition_dir(self, key: Tuple[Any, str, Any, Any]) -> Path:
        def as_str(value: Any) -> str:
            return "null" if value is None else str(value)

        season_id, server_name, matching_mode, date_value = key
        parts = [
            f"season_id={as_str(season_id)}",
            f"server_name={as_str(server_name)}",
            f"matching_mode={as_str(matching_mode)}",
            f"date={as_str(date_value)}",
        ]
        path = self.base_dir
        for part in parts:
            path = path / part
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _flush(self, key: Tuple[Any, str, Any, Any]) -> None:
        rows = self._buffers.get(key)
        if not rows:
            return
        self._file_counters[key] += 1
        filename = (
            self._partition_dir(key) / f"part-{self._file_counters[key]:05d}.parquet"
        )
        columns = {name: [row.get(name) for row in rows] for name in self.schema.names}
        table = self._pa.table(columns, schema=self.schema)
        self._pq.write_table(
            table,
            filename,
            compression=self.compression,
            use_dictionary=["server_name"],
        )
        rows.clear()

    def write_row(self, row: Dict[str, Any], key: Tuple[Any, str, Any, Any]) -> None:
        buf = self._buffers[key]
        buf.append(row)
        if len(buf) >= self.max_rows_per_file:
            self._flush(key)

    def close(self) -> None:
        for key in list(self._buffers.keys()):
            self._flush(key)


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Utility commands for ER stats datasets (no DB required)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    compact = subparsers.add_parser(
        "parquet-compact",
        help="Compact and compress an existing Parquet dataset",
    )
    compact.add_argument(
        "--src", type=Path, required=True, help="Source Parquet root (hive partitioned)"
    )
    compact.add_argument(
        "--dst",
        type=Path,
        required=True,
        help="Destination Parquet root (will be created)",
    )
    compact.add_argument(
        "--compression",
        default="zstd",
        help="Parquet compression codec (default: zstd)",
    )
    compact.add_argument(
        "--max-rows-per-file", type=int, default=250000, help="Max rows per output file"
    )

    rebuild = subparsers.add_parser(
        "parquet-rebuild",
        help="Rebuild matches and participants datasets with match-level consistency",
    )
    rebuild.add_argument(
        "--src",
        type=Path,
        required=True,
        help="Source Parquet root (matches/, participants/)",
    )
    rebuild.add_argument(
        "--dst",
        type=Path,
        required=True,
        help="Destination Parquet root (will be created)",
    )
    rebuild.add_argument(
        "--compression",
        default="zstd",
        help="Parquet compression codec (default: zstd)",
    )
    rebuild.add_argument(
        "--max-rows-per-file", type=int, default=250000, help="Max rows per output file"
    )

    prune = subparsers.add_parser(
        "sqlite-prune",
        help="Delete old matches from SQLite and track tombstones",
    )
    prune.add_argument(
        "--config",
        type=Path,
        help="TOML configuration file with ingest.db_path and ingest.parquet_dir",
    )
    prune.add_argument("--db", type=Path, help="SQLite database path")
    prune.add_argument(
        "--parquet-dir",
        type=Path,
        help="Parquet root (informational; no files are modified)",
    )
    prune.add_argument(
        "--before",
        type=str,
        help="Delete matches with start_dtm on or before this ISO-8601 datetime or date (UTC).",
    )
    prune.add_argument(
        "--retention-days",
        type=int,
        help="Keep only the most recent N days of matches (computed in UTC).",
    )
    prune.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Apply deletions (omit for dry-run).",
    )
    prune.add_argument(
        "--vacuum",
        action="store_true",
        default=False,
        help="Run VACUUM after deletion to reclaim disk space.",
    )

    return parser.parse_args(argv)


def run(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    if args.command == "parquet-compact":
        try:
            import pyarrow.dataset as ds
        except Exception as e:
            print(f"pyarrow is required for parquet compaction: {e}", file=sys.stderr)
            return 2

        src = args.src
        dst = args.dst
        dst.mkdir(parents=True, exist_ok=True)

        fmt = ds.ParquetFileFormat()
        opts = fmt.make_write_options(compression=args.compression)
        # Force plain string writes to avoid dictionary-encoded strings across files
        try:
            opts.use_dictionary = False  # type: ignore[attr-defined]
        except Exception:
            pass
        dataset = ds.dataset(str(src), format=fmt, partitioning="hive")
        ds.write_dataset(
            data=dataset,
            base_dir=str(dst),
            format=fmt,
            file_options=opts,
            partitioning=dataset.partitioning,
            max_rows_per_file=int(args.max_rows_per_file),
            max_rows_per_group=int(args.max_rows_per_file),
            existing_data_behavior="overwrite_or_ignore",
        )
        return 0
    if args.command == "parquet-rebuild":
        try:
            import pyarrow as pa
            import pyarrow.dataset as ds
            import pyarrow.parquet as pq
        except Exception as e:
            print(f"pyarrow is required for parquet rebuild: {e}", file=sys.stderr)
            return 2

        src = args.src
        dst = args.dst
        matches_src = src / "matches"
        participants_src = src / "participants"
        if not matches_src.exists() or not participants_src.exists():
            print(
                "parquet-rebuild requires matches/ and participants/ under --src",
                file=sys.stderr,
            )
            return 2
        matches_dst = dst / "matches"
        participants_dst = dst / "participants"
        matches_dst.mkdir(parents=True, exist_ok=True)
        participants_dst.mkdir(parents=True, exist_ok=True)

        no_partitions = ds.partitioning(pa.schema([]))
        matches_dataset = ds.dataset(
            str(matches_src), format="parquet", partitioning=no_partitions
        )
        matches_schema = _strip_date(matches_dataset.schema)
        matches_columns = matches_schema.names
        if "date" in matches_dataset.schema.names:
            matches_columns_with_date = matches_columns + ["date"]
        else:
            matches_columns_with_date = matches_columns

        rep_cols = [
            "season_id",
            "server_name",
            "matching_mode",
            "matching_team_mode",
            "start_dtm",
        ]
        match_choices: dict[int, _MatchChoice] = {}
        for row in _iter_rows(matches_dataset, matches_columns_with_date):
            game_id = row.get("game_id")
            if game_id is None:
                continue
            try:
                game_id_int = int(game_id)
            except (TypeError, ValueError):
                continue
            row_data = {col: row.get(col) for col in matches_columns}
            if row_data.get("server_name") is None:
                row_data["server_name"] = ""
            score = _match_score(row_data, rep_cols, matches_columns)
            existing = match_choices.get(game_id_int)
            if existing is None or score > existing.score:
                match_choices[game_id_int] = _MatchChoice(
                    row=row_data,
                    score=score,
                    partition_date=row.get("date"),
                )

        match_writer = _PartitionedWriter(
            matches_dst,
            matches_schema,
            max_rows_per_file=args.max_rows_per_file,
            compression=args.compression,
            pa_module=pa,
            pq_module=pq,
        )
        for game_id in sorted(match_choices):
            choice = match_choices[game_id]
            row = choice.row
            date_value = _date_part(row.get("start_dtm")) or choice.partition_date
            match_writer.write_row(row, _partition_key(row, date_value))
        match_writer.close()

        participants_dataset = ds.dataset(
            str(participants_src), format="parquet", partitioning=no_partitions
        )
        participants_schema = _strip_date(participants_dataset.schema)
        participants_columns = participants_schema.names
        if "date" in participants_dataset.schema.names:
            participants_columns_with_date = participants_columns + ["date"]
        else:
            participants_columns_with_date = participants_columns

        seen: set[Tuple[int, str]] = set()
        participants_writer = _PartitionedWriter(
            participants_dst,
            participants_schema,
            max_rows_per_file=args.max_rows_per_file,
            compression=args.compression,
            pa_module=pa,
            pq_module=pq,
        )
        for row in _iter_rows(participants_dataset, participants_columns_with_date):
            row_date = row.get("date")
            row_data = {col: row.get(col) for col in participants_columns}
            game_id = row_data.get("game_id")
            if game_id is None:
                continue
            try:
                game_id_int = int(game_id)
            except (TypeError, ValueError):
                continue
            row_data["game_id"] = game_id_int
            if row_data.get("server_name") is None:
                row_data["server_name"] = ""
            match_choice = match_choices.get(game_id_int)
            date_value = row_date
            if match_choice is not None:
                _apply_match_context(row_data, match_choice.row)
                date_value = (
                    _date_part(match_choice.row.get("start_dtm"))
                    or match_choice.partition_date
                )

            uid = row_data.get("uid")
            nickname = row_data.get("nickname")
            if uid:
                dedupe_key = (game_id_int, str(uid))
            elif nickname:
                dedupe_key = (game_id_int, f"nickname:{nickname}")
            else:
                continue
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            participants_writer.write_row(
                row_data, _partition_key(row_data, date_value)
            )
        participants_writer.close()
        return 0
    if args.command == "sqlite-prune":
        db_path, parquet_dir, config_error = _resolve_prune_paths(args)
        if config_error is not None:
            return config_error
        if db_path is None:
            print("--db or ingest.db_path in config is required.", file=sys.stderr)
            return 2
        if not db_path.exists():
            print(f"SQLite DB not found: {db_path}", file=sys.stderr)
            return 2
        if args.before and args.retention_days is not None:
            print("Specify --before or --retention-days, not both.", file=sys.stderr)
            return 2
        if args.before:
            try:
                cutoff_dt = _parse_datetime_or_date(args.before)
            except ValueError as exc:
                print(f"{exc}", file=sys.stderr)
                return 2
            reason = "prune_before"
        elif args.retention_days is not None:
            if args.retention_days <= 0:
                print("--retention-days must be greater than 0.", file=sys.stderr)
                return 2
            now = dt.datetime.now(dt.timezone.utc)
            cutoff_dt = now - dt.timedelta(days=int(args.retention_days))
            reason = "retention_days"
        else:
            print(
                "Either --before or --retention-days is required.",
                file=sys.stderr,
            )
            return 2

        cutoff_iso = cutoff_dt.isoformat()
        print(f"SQLite DB: {db_path}")
        if parquet_dir is not None:
            print(f"Parquet root: {parquet_dir} (not modified)")
        print(f"Prune cutoff: {cutoff_iso}")

        store = SQLiteStore(str(db_path))
        try:
            store.setup_schema()
            candidate_count = store.count_matches_before(cutoff_iso)
            print(f"Game IDs to prune: {candidate_count}")
            if not args.apply:
                print("Dry-run only. Use --apply to delete matches.")
                return 0
            deleted_at = dt.datetime.now(dt.timezone.utc).isoformat()
            deleted = store.prune_matches_before(
                cutoff_iso,
                deleted_at=deleted_at,
                reason=reason,
            )
            existing_prune = store.get_prune_before()
            if existing_prune:
                try:
                    existing_dt = dt.datetime.fromisoformat(existing_prune)
                except ValueError:
                    existing_dt = None
            else:
                existing_dt = None
            if existing_dt and existing_dt > cutoff_dt:
                store.set_prune_before(existing_prune)
            else:
                store.set_prune_before(cutoff_iso)
            if args.vacuum:
                store.connection.execute("VACUUM")
            print(f"Deleted game IDs: {deleted}")
            return 0
        finally:
            store.close()

    raise ValueError(f"Unsupported command: {args.command}")


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
