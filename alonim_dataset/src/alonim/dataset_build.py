from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .config import (
    DEFAULT_JSONL_BASENAME,
    DEFAULT_MARKDOWN_DIR,
    DEFAULT_OUTPUT_JSONL_DIR,
    DEFAULT_OUTPUT_PARQUET_DIR,
    DEFAULT_PARQUET_FILE,
)


@dataclass(frozen=True)
class DatasetBuildResult:
    records: int
    jsonl_paths: tuple[Path, ...]
    parquet_path: Path | None = None


def markdown_to_jsonl(
    input_dir: Path = DEFAULT_MARKDOWN_DIR,
    output_dir: Path = DEFAULT_OUTPUT_JSONL_DIR,
    *,
    basename: str = DEFAULT_JSONL_BASENAME,
    records_per_file: int = 1000,
) -> DatasetBuildResult:
    if records_per_file < 1:
        raise ValueError("records_per_file must be at least 1")

    markdown_files = sorted(input_dir.rglob("*.md"))
    output_dir.mkdir(parents=True, exist_ok=True)

    paths: list[Path] = []
    current_handle = None
    current_count = 0
    part_number = 0
    total_records = 0

    try:
        for markdown_file in markdown_files:
            text = markdown_file.read_text(encoding="utf-8").strip()
            if not text:
                continue

            if current_handle is None or current_count >= records_per_file:
                if current_handle is not None:
                    current_handle.close()
                part_number += 1
                current_count = 0
                path = output_dir / f"{basename}-part-{part_number:05d}.jsonl"
                paths.append(path)
                current_handle = path.open("w", encoding="utf-8", newline="\n")

            record = {
                "text": text,
                "source": markdown_file.relative_to(input_dir).as_posix(),
                "metadata": {
                    "title": markdown_file.stem,
                    "source_format": "markdown",
                },
            }
            current_handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
            current_handle.write("\n")
            current_count += 1
            total_records += 1
    finally:
        if current_handle is not None:
            current_handle.close()

    return DatasetBuildResult(records=total_records, jsonl_paths=tuple(paths))


def jsonl_to_parquet(
    input_dir: Path = DEFAULT_OUTPUT_JSONL_DIR,
    output_dir: Path = DEFAULT_OUTPUT_PARQUET_DIR,
    *,
    output_file: str = DEFAULT_PARQUET_FILE,
) -> DatasetBuildResult:
    records = list(iter_jsonl_records(sorted(input_dir.glob("*.jsonl"))))
    if not records:
        return DatasetBuildResult(records=0, jsonl_paths=tuple(), parquet_path=None)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_file

    import pandas as pd

    dataframe = pd.DataFrame(records)
    if "text" in dataframe.columns:
        dataframe = dataframe.dropna(subset=["text"])
        dataframe = dataframe[dataframe["text"].str.strip() != ""]
        dataframe = dataframe.drop_duplicates(subset=["text"], keep="first")

    dataframe.to_parquet(output_path, index=False)
    return DatasetBuildResult(records=len(dataframe), jsonl_paths=tuple(sorted(input_dir.glob("*.jsonl"))), parquet_path=output_path)


def iter_jsonl_records(paths: Iterable[Path]) -> Iterable[dict]:
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)
