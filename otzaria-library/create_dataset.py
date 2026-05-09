from __future__ import annotations

import argparse
from pathlib import Path

from otzaria_dataset.pipeline import (
    DEFAULT_OUTPUT_BASENAME,
    DEFAULT_RELEASE_ASSET,
    DEFAULT_REPO,
    PipelineConfig,
    build_dataset,
    download_release_asset,
)


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SOURCE_DIR = SCRIPT_DIR / "source_data"
DEFAULT_ARCHIVE_PATH = DEFAULT_SOURCE_DIR / DEFAULT_RELEASE_ASSET
DEFAULT_MANIFEST_PATH = DEFAULT_SOURCE_DIR / "files_manifest.json"
DEFAULT_METADATA_PATH = DEFAULT_SOURCE_DIR / "metadata.json"
DEFAULT_PARQUET_OUTPUT_DIR = SCRIPT_DIR / "output_parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build the Otzaria Library Hugging Face dataset from a GitHub Release "
            "archive without cloning the full upstream repository."
        )
    )
    parser.add_argument("--archive-path", type=Path, default=DEFAULT_ARCHIVE_PATH)
    parser.add_argument(
        "--extra-archive-path",
        type=Path,
        action="append",
        default=[],
        help="Additional Otzaria release archive to include. Can be passed more than once.",
    )
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--metadata-path", type=Path, default=DEFAULT_METADATA_PATH)
    parser.add_argument("--parquet-output-dir", type=Path, default=DEFAULT_PARQUET_OUTPUT_DIR)
    parser.add_argument("--parquet-output-file", default=f"{DEFAULT_OUTPUT_BASENAME}.parquet")
    parser.add_argument("--parquet-shards", type=int, default=10)
    parser.add_argument("--parquet-records-per-file", type=int, default=None)
    parser.add_argument("--parquet-target-file-size-mb", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--compression", default="zstd")
    parser.add_argument("--github-release", default="latest")
    parser.add_argument("--github-repo", default=DEFAULT_REPO)
    parser.add_argument("--release-asset", default=DEFAULT_RELEASE_ASSET)
    parser.add_argument(
        "--extra-release-asset",
        action="append",
        default=[],
        help="Additional release asset to download into source_data. Can be passed more than once.",
    )
    parser.add_argument("--download-release-asset", action="store_true")
    parser.add_argument("--overwrite-download", action="store_true")
    parser.add_argument("--no-deduplicate", action="store_true")
    parser.add_argument("--keep-existing-output", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    archive_path = args.archive_path
    extra_archive_paths = list(args.extra_archive_path)
    if args.download_release_asset:
        print(
            f"Downloading {args.github_repo}@{args.github_release} asset "
            f"{args.release_asset} into {DEFAULT_SOURCE_DIR}"
        )
        archive_path = download_release_asset(
            repo=args.github_repo,
            release=args.github_release,
            asset_name=args.release_asset,
            output_dir=DEFAULT_SOURCE_DIR,
            overwrite=args.overwrite_download,
        )
        for asset_name in args.extra_release_asset:
            print(
                f"Downloading {args.github_repo}@{args.github_release} asset "
                f"{asset_name} into {DEFAULT_SOURCE_DIR}"
            )
            extra_archive_paths.append(
                download_release_asset(
                    repo=args.github_repo,
                    release=args.github_release,
                    asset_name=asset_name,
                    output_dir=DEFAULT_SOURCE_DIR,
                    overwrite=args.overwrite_download,
                )
            )

    config = PipelineConfig(
        archive_path=archive_path,
        parquet_output_dir=args.parquet_output_dir,
        archive_paths=(archive_path, *extra_archive_paths),
        parquet_output_file=args.parquet_output_file,
        manifest_path=args.manifest_path,
        metadata_path=args.metadata_path,
        github_release=args.github_release,
        parquet_shards=args.parquet_shards,
        parquet_records_per_file=args.parquet_records_per_file,
        parquet_target_file_size_mb=args.parquet_target_file_size_mb,
        batch_size=args.batch_size,
        deduplicate_text=not args.no_deduplicate,
        compression=args.compression,
        clean_output=not args.keep_existing_output,
    )

    print("Building Otzaria Library dataset")
    print("Archives:")
    for path in config.archive_paths or (config.archive_path,):
        print(f"  - {path}")
    print(f"Manifest: {config.manifest_path}")
    print(f"Metadata: {config.metadata_path}")
    print(f"Parquet output directory: {config.parquet_output_dir}")

    result = build_dataset(config)

    print("\n--- Processing Complete ---")
    print(f"Processed records: {result.processed_records}")
    print(f"Parquet files: {len(result.parquet_paths)}")
    for path in result.parquet_paths:
        print(f"  - {path}")
    print(f"Skipped non-text files: {result.skipped_non_txt_files}")
    print(f"Skipped empty texts: {result.skipped_empty_texts}")
    print(f"Duplicate records removed: {result.duplicate_records}")
    print(f"Read errors: {result.read_errors}")


if __name__ == "__main__":
    main()
