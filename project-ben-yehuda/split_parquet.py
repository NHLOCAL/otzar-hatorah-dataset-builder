from __future__ import annotations

import argparse
from pathlib import Path

from pby_dataset.pipeline import split_parquet_file


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "output_parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Split an existing Parquet file into smaller part files.")
    parser.add_argument("--input-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-file", default="pby_dataset.parquet")
    parser.add_argument("--parquet-shards", type=int, default=10)
    parser.add_argument("--parquet-records-per-file", type=int, default=None)
    parser.add_argument("--parquet-target-file-size-mb", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument("--compression", default="zstd")
    parser.add_argument("--keep-existing-output", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = split_parquet_file(
        input_path=args.input_file,
        output_dir=args.output_dir,
        output_file=args.output_file,
        shards=args.parquet_shards,
        records_per_file=args.parquet_records_per_file,
        target_file_size_mb=args.parquet_target_file_size_mb,
        batch_size=args.batch_size,
        compression=args.compression,
        clean_output=not args.keep_existing_output,
    )

    print(f"Created {len(paths)} Parquet files:")
    for path in paths:
        print(f"  - {path}")


if __name__ == "__main__":
    main()
