"""Utility CLI for maintenance/ops tasks (no DB required).

Commands
- parquet-compact: compact and compress a hive-partitioned Parquet dataset.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional


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

    raise ValueError(f"Unsupported command: {args.command}")


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
