from __future__ import annotations

import csv
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


SOURCE_NAME = "Project Ben-Yehuda"
DEFAULT_OUTPUT_BASENAME = "pby_dataset"

METADATA_FIELDS = (
    ("pby_id", "ID"),
    ("title", "title"),
    ("authors", "authors"),
    ("translators", "translators"),
    ("author_uris", "author_uris"),
    ("translator_uris", "translator_uris"),
    ("original_language", "original_language"),
    ("genre", "genre"),
    ("source_edition", "source_edition"),
    ("filepath_pby", "path"),
)

PARQUET_SCHEMA = pa.schema(
    [
        pa.field("text", pa.string()),
        pa.field("source", pa.string()),
        pa.field(
            "metadata",
            pa.struct([pa.field(output_name, pa.string()) for output_name, _ in METADATA_FIELDS]),
        ),
    ]
)


@dataclass(frozen=True)
class PipelineConfig:
    source_dir: Path
    catalog_file: Path
    parquet_output_dir: Path
    parquet_output_file: str = f"{DEFAULT_OUTPUT_BASENAME}.parquet"
    jsonl_output_dir: Path | None = None
    jsonl_records_per_file: int = 2500
    batch_size: int = 1024
    workers: int = 8
    deduplicate_text: bool = True
    compression: str = "zstd"
    clean_output: bool = True
    show_progress: bool = True


@dataclass(frozen=True)
class PipelineResult:
    processed_records: int
    missing_text_files: int
    skipped_empty_texts: int
    duplicate_records: int
    parquet_path: Path
    jsonl_files: tuple[Path, ...]


@dataclass(frozen=True)
class _ProcessedRow:
    record: dict | None
    missing_text_file: bool = False
    empty_text: bool = False


class _JsonlShardWriter:
    def __init__(self, output_dir: Path, basename: str, records_per_file: int) -> None:
        if records_per_file < 1:
            raise ValueError("jsonl_records_per_file must be at least 1")

        self.output_dir = output_dir
        self.basename = basename
        self.records_per_file = records_per_file
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._handle = None
        self._part_number = 0
        self._records_in_part = 0
        self.paths: list[Path] = []

    def write(self, record: dict) -> None:
        if self._handle is None or self._records_in_part >= self.records_per_file:
            self._open_next_part()

        self._handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
        self._handle.write("\n")
        self._records_in_part += 1

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def _open_next_part(self) -> None:
        self.close()
        self._part_number += 1
        self._records_in_part = 0
        path = self.output_dir / f"{self.basename}-part-{self._part_number:05d}.jsonl"
        self.paths.append(path)
        self._handle = path.open("w", encoding="utf-8", newline="\n")


def iter_catalog_rows(catalog_file: Path) -> Iterator[dict[str, str]]:
    with catalog_file.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if (row.get("path") or "").strip():
                yield {key: value or "" for key, value in row.items()}


def make_record(metadata: dict[str, str], text: str) -> dict:
    return {
        "text": text.strip(),
        "source": SOURCE_NAME,
        "metadata": {
            output_name: (metadata.get(input_name) or "").strip()
            for output_name, input_name in METADATA_FIELDS
        },
    }


def build_dataset(config: PipelineConfig) -> PipelineResult:
    _validate_config(config)
    _prepare_outputs(config)

    parquet_path = config.parquet_output_dir / config.parquet_output_file
    jsonl_writer = (
        _JsonlShardWriter(config.jsonl_output_dir, DEFAULT_OUTPUT_BASENAME, config.jsonl_records_per_file)
        if config.jsonl_output_dir is not None
        else None
    )

    processed_records = 0
    missing_text_files = 0
    skipped_empty_texts = 0
    duplicate_records = 0
    seen_text_hashes: set[str] = set()
    parquet_writer = pq.ParquetWriter(parquet_path, PARQUET_SCHEMA, compression=config.compression)

    try:
        with ThreadPoolExecutor(max_workers=config.workers) as executor:
            catalog_rows = iter_catalog_rows(config.catalog_file)
            if config.show_progress:
                catalog_rows = tqdm(catalog_rows, desc="Processing catalog rows", unit="record")

            for source_batch in _batched(catalog_rows, config.batch_size):
                records: list[dict] = []
                processed_batch = executor.map(
                    lambda row: _read_text_record(config.source_dir, row),
                    source_batch,
                )

                for processed in processed_batch:
                    if processed.missing_text_file:
                        missing_text_files += 1
                        continue
                    if processed.empty_text:
                        skipped_empty_texts += 1
                        continue
                    if processed.record is None:
                        continue

                    if config.deduplicate_text:
                        text_hash = hashlib.sha256(processed.record["text"].encode("utf-8")).hexdigest()
                        if text_hash in seen_text_hashes:
                            duplicate_records += 1
                            continue
                        seen_text_hashes.add(text_hash)

                    records.append(processed.record)
                    if jsonl_writer is not None:
                        jsonl_writer.write(processed.record)

                if records:
                    table = pa.Table.from_pylist(records, schema=PARQUET_SCHEMA)
                    parquet_writer.write_table(table)
                    processed_records += len(records)
    finally:
        parquet_writer.close()
        if jsonl_writer is not None:
            jsonl_writer.close()

    return PipelineResult(
        processed_records=processed_records,
        missing_text_files=missing_text_files,
        skipped_empty_texts=skipped_empty_texts,
        duplicate_records=duplicate_records,
        parquet_path=parquet_path,
        jsonl_files=tuple(jsonl_writer.paths if jsonl_writer is not None else ()),
    )


def _validate_config(config: PipelineConfig) -> None:
    if not config.source_dir.exists():
        raise FileNotFoundError(f"Source directory was not found: {config.source_dir}")
    if not config.catalog_file.exists():
        raise FileNotFoundError(f"Catalog file was not found: {config.catalog_file}")
    if config.batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    if config.workers < 1:
        raise ValueError("workers must be at least 1")
    if not config.parquet_output_file.endswith(".parquet"):
        raise ValueError("parquet_output_file must end with .parquet")


def _prepare_outputs(config: PipelineConfig) -> None:
    config.parquet_output_dir.mkdir(parents=True, exist_ok=True)

    if not config.clean_output:
        return

    parquet_path = config.parquet_output_dir / config.parquet_output_file
    if parquet_path.exists():
        parquet_path.unlink()

    if config.jsonl_output_dir is not None and config.jsonl_output_dir.exists():
        for path in config.jsonl_output_dir.glob(f"{DEFAULT_OUTPUT_BASENAME}-part-*.jsonl"):
            path.unlink()


def _read_text_record(source_dir: Path, metadata: dict[str, str]) -> _ProcessedRow:
    text_file = _text_file_path(source_dir, metadata["path"])
    if text_file is None or not text_file.exists():
        return _ProcessedRow(record=None, missing_text_file=True)

    text = text_file.read_text(encoding="utf-8")
    if not text.strip():
        return _ProcessedRow(record=None, empty_text=True)

    return _ProcessedRow(record=make_record(metadata, text))


def _text_file_path(source_dir: Path, relative_path: str) -> Path | None:
    normalized = relative_path.strip().replace("\\", "/").lstrip("/")
    path = PurePosixPath(normalized)
    if not normalized or ".." in path.parts:
        return None
    return source_dir.joinpath(*path.parts).with_suffix(".txt")


def _batched(rows: Iterable[dict[str, str]], batch_size: int) -> Iterator[list[dict[str, str]]]:
    batch: list[dict[str, str]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
