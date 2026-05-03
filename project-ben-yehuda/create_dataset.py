from __future__ import annotations

import argparse
import os
from pathlib import Path

from pby_dataset.pipeline import PipelineConfig, build_dataset


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SOURCE_DIR = SCRIPT_DIR / "source_data"
DEFAULT_CATALOG_FILE = DEFAULT_SOURCE_DIR / "pseudocatalogue.csv"
DEFAULT_PARQUET_OUTPUT_DIR = SCRIPT_DIR / "output_parquet"
DEFAULT_JSONL_OUTPUT_DIR = SCRIPT_DIR / "output_jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build the Project Ben-Yehuda dataset directly from the source catalog "
            "and text files into Parquet. JSONL shards are optional for compatibility."
        )
    )
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--catalog-file", type=Path, default=DEFAULT_CATALOG_FILE)
    parser.add_argument("--parquet-output-dir", type=Path, default=DEFAULT_PARQUET_OUTPUT_DIR)
    parser.add_argument("--parquet-output-file", default="pby_dataset.parquet")
    parser.add_argument(
        "--write-jsonl",
        action="store_true",
        help="Also write legacy JSONL shards to --jsonl-output-dir.",
    )
    parser.add_argument("--jsonl-output-dir", type=Path, default=DEFAULT_JSONL_OUTPUT_DIR)
    parser.add_argument("--jsonl-records-per-file", type=int, default=2500)
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument(
        "--workers",
        type=int,
        default=min(32, (os.cpu_count() or 4) * 2),
        help="Number of concurrent text-file readers.",
    )
    parser.add_argument(
        "--no-deduplicate",
        action="store_true",
        help="Keep duplicate text records instead of deduplicating by text hash.",
    )
    parser.add_argument(
        "--compression",
        default="zstd",
        help="Parquet compression codec. Use 'snappy' if zstd is unavailable.",
    )
    parser.add_argument(
        "--keep-existing-output",
        action="store_true",
        help="Do not delete the target Parquet file or old generated JSONL shards before writing.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = PipelineConfig(
        source_dir=args.source_dir,
        catalog_file=args.catalog_file,
        parquet_output_dir=args.parquet_output_dir,
        parquet_output_file=args.parquet_output_file,
        jsonl_output_dir=args.jsonl_output_dir if args.write_jsonl else None,
        jsonl_records_per_file=args.jsonl_records_per_file,
        batch_size=args.batch_size,
        workers=args.workers,
        deduplicate_text=not args.no_deduplicate,
        compression=args.compression,
        clean_output=not args.keep_existing_output,
    )

    print("Building Project Ben-Yehuda dataset")
    print(f"Source directory: {config.source_dir}")
    print(f"Catalog file: {config.catalog_file}")
    print(f"Parquet output: {config.parquet_output_dir / config.parquet_output_file}")
    if config.jsonl_output_dir is not None:
        print(f"Legacy JSONL output: {config.jsonl_output_dir}")

    result = build_dataset(config)

    print("\n--- Processing Complete ---")
    print(f"Parquet file: {result.parquet_path}")
    print(f"Processed records: {result.processed_records}")
    print(f"Missing text files: {result.missing_text_files}")
    print(f"Skipped empty texts: {result.skipped_empty_texts}")
    print(f"Duplicate records removed: {result.duplicate_records}")
    if result.jsonl_files:
        print(f"JSONL shard files: {len(result.jsonl_files)}")


if __name__ == "__main__":
    main()
