from __future__ import annotations

import argparse
from pathlib import Path

import _bootstrap  # noqa: F401
from alonim.dataset_build import markdown_to_parquet


def main() -> None:
    parser = argparse.ArgumentParser(description="Build an Alonim Parquet dataset from converted Markdown files.")
    parser.add_argument("--input-dir", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--output-file", default="alonim_dataset.parquet")
    args = parser.parse_args()

    kwargs = {"output_file": args.output_file}
    if args.input_dir is not None:
        kwargs["input_dir"] = args.input_dir
    if args.output_dir is not None:
        kwargs["output_dir"] = args.output_dir

    result = markdown_to_parquet(**kwargs)
    if result.parquet_path is None:
        print("No Markdown records found. Parquet was not created.")
    else:
        print(f"Wrote {result.records} records to: {result.parquet_path}")


if __name__ == "__main__":
    main()
