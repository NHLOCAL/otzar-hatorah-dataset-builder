from __future__ import annotations

import argparse
from pathlib import Path

import _bootstrap  # noqa: F401
from alonim.dataset_build import markdown_to_jsonl


def main() -> None:
    parser = argparse.ArgumentParser(description="Build JSONL shards from converted Markdown files.")
    parser.add_argument("--input-dir", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--records-per-file", type=int, default=1000)
    args = parser.parse_args()

    kwargs = {"records_per_file": args.records_per_file}
    if args.input_dir is not None:
        kwargs["input_dir"] = args.input_dir
    if args.output_dir is not None:
        kwargs["output_dir"] = args.output_dir

    result = markdown_to_jsonl(**kwargs)
    print(f"Wrote {result.records} records to {len(result.jsonl_paths)} JSONL shard(s).")
    for path in result.jsonl_paths:
        print(f"  - {path}")


if __name__ == "__main__":
    main()
