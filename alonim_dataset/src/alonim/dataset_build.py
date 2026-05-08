from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import (
    DEFAULT_MARKDOWN_DIR,
    DEFAULT_OUTPUT_PARQUET_DIR,
    DEFAULT_PARQUET_FILE,
)


@dataclass(frozen=True)
class DatasetBuildResult:
    records: int
    parquet_path: Path | None = None


def markdown_to_parquet(
    input_dir: Path = DEFAULT_MARKDOWN_DIR,
    output_dir: Path = DEFAULT_OUTPUT_PARQUET_DIR,
    *,
    output_file: str = DEFAULT_PARQUET_FILE,
) -> DatasetBuildResult:
    records = list(iter_markdown_records(input_dir))
    if not records:
        return DatasetBuildResult(records=0, parquet_path=None)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_file

    import pandas as pd

    dataframe = pd.DataFrame(records)
    if "text" in dataframe.columns:
        dataframe = dataframe.dropna(subset=["text"])
        dataframe = dataframe[dataframe["text"].str.strip() != ""]
        dataframe = dataframe.drop_duplicates(subset=["text"], keep="first")

    dataframe.to_parquet(output_path, index=False)
    return DatasetBuildResult(records=len(dataframe), parquet_path=output_path)


def iter_markdown_records(input_dir: Path):
    for markdown_file in sorted(input_dir.rglob("*.md")):
        text = markdown_file.read_text(encoding="utf-8").strip()
        if not text:
            continue
        yield {
            "text": text,
            "source": markdown_file.relative_to(input_dir).as_posix(),
            "metadata": {
                "title": markdown_file.stem,
                "source_format": "markdown",
            },
        }
