from __future__ import annotations

import csv
import hashlib
import math
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
    parquet_shards: int = 10
    parquet_records_per_file: int | None = None
    parquet_target_file_size_mb: int | None = None
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
    parquet_paths: tuple[Path, ...]

    @property
    def parquet_path(self) -> Path:
        return self.parquet_paths[0]


@dataclass(frozen=True)
class _ProcessedRow:
    record: dict | None
    missing_text_file: bool = False
    empty_text: bool = False


class _ParquetShardWriter:
    def __init__(
        self,
        output_dir: Path,
        output_file: str,
        records_per_file: int | None,
        target_file_size_bytes: int | None,
        compression: str,
    ) -> None:
        self.output_dir = output_dir
        self.output_file = output_file
        self.records_per_file = records_per_file
        self.target_file_size_bytes = target_file_size_bytes
        self.compression = compression
        self._writer: pq.ParquetWriter | None = None
        self._records_in_file = 0
        self._part_number = 0
        self.paths: list[Path] = []

    def write_records(self, records: list[dict]) -> None:
        pending = records
        while pending:
            if self.records_per_file is None:
                self._write_table(pending)
                pending = []
                if self._current_file_reached_target_size():
                    self.close()
                continue

            available = self.records_per_file - self._records_in_file
            if available <= 0:
                self.close()
                available = self.records_per_file

            chunk = pending[:available]
            self._write_table(chunk)
            pending = pending[available:]

            if self._records_in_file >= self.records_per_file or self._current_file_reached_target_size():
                self.close()

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
            self._records_in_file = 0

    def _write_table(self, records: list[dict]) -> None:
        if not records:
            return
        if self._writer is None:
            self._open_next_part()
        table = pa.Table.from_pylist(records, schema=PARQUET_SCHEMA)
        self._writer.write_table(table)
        self._records_in_file += len(records)

    def _open_next_part(self) -> None:
        self._part_number += 1
        path = self._next_path()
        self.paths.append(path)
        self._writer = pq.ParquetWriter(path, PARQUET_SCHEMA, compression=self.compression)

    def _next_path(self) -> Path:
        if self.records_per_file is None and self.target_file_size_bytes is None:
            return self.output_dir / self.output_file

        stem = Path(self.output_file).stem
        suffix = Path(self.output_file).suffix
        return self.output_dir / f"{stem}-part-{self._part_number:05d}{suffix}"

    def _current_file_reached_target_size(self) -> bool:
        if self.target_file_size_bytes is None or not self.paths:
            return False
        current_path = self.paths[-1]
        return current_path.exists() and current_path.stat().st_size >= self.target_file_size_bytes


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

    parquet_records_per_file = _resolve_parquet_records_per_file(config)
    parquet_writer = _ParquetShardWriter(
        output_dir=config.parquet_output_dir,
        output_file=config.parquet_output_file,
        records_per_file=parquet_records_per_file,
        target_file_size_bytes=_target_file_size_bytes(config),
        compression=config.compression,
    )
    processed_records = 0
    missing_text_files = 0
    skipped_empty_texts = 0
    duplicate_records = 0
    seen_text_hashes: set[str] = set()

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

                if records:
                    parquet_writer.write_records(records)
                    processed_records += len(records)
    finally:
        parquet_writer.close()

    return PipelineResult(
        processed_records=processed_records,
        missing_text_files=missing_text_files,
        skipped_empty_texts=skipped_empty_texts,
        duplicate_records=duplicate_records,
        parquet_paths=tuple(parquet_writer.paths),
    )


def split_parquet_file(
    input_path: Path,
    output_dir: Path,
    output_file: str = f"{DEFAULT_OUTPUT_BASENAME}.parquet",
    shards: int = 10,
    records_per_file: int | None = None,
    target_file_size_mb: int | None = None,
    batch_size: int = 1024,
    compression: str = "zstd",
    clean_output: bool = True,
) -> tuple[Path, ...]:
    if not input_path.exists():
        raise FileNotFoundError(f"Input Parquet file was not found: {input_path}")
    if not output_file.endswith(".parquet"):
        raise ValueError("output_file must end with .parquet")
    if shards < 1:
        raise ValueError("shards must be at least 1")
    if records_per_file is not None and records_per_file < 1:
        raise ValueError("records_per_file must be at least 1")
    if target_file_size_mb is not None and target_file_size_mb < 1:
        raise ValueError("target_file_size_mb must be at least 1")
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    output_dir.mkdir(parents=True, exist_ok=True)
    if clean_output:
        for path in _existing_parquet_paths(output_dir, output_file):
            if path.resolve() != input_path.resolve():
                path.unlink()

    parquet_file = pq.ParquetFile(input_path)
    resolved_records_per_file = records_per_file
    if resolved_records_per_file is None and shards > 1 and parquet_file.metadata.num_rows > 0:
        resolved_records_per_file = max(1, math.ceil(parquet_file.metadata.num_rows / shards))

    writer = _ParquetShardWriter(
        output_dir=output_dir,
        output_file=output_file,
        records_per_file=resolved_records_per_file,
        target_file_size_bytes=target_file_size_mb * 1024 * 1024 if target_file_size_mb else None,
        compression=compression,
    )

    try:
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            writer.write_records(pa.Table.from_batches([batch]).to_pylist())
    finally:
        writer.close()

    return tuple(writer.paths)


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
    if config.parquet_shards < 1:
        raise ValueError("parquet_shards must be at least 1")
    if config.parquet_records_per_file is not None and config.parquet_records_per_file < 1:
        raise ValueError("parquet_records_per_file must be at least 1")
    if config.parquet_target_file_size_mb is not None and config.parquet_target_file_size_mb < 1:
        raise ValueError("parquet_target_file_size_mb must be at least 1")


def _prepare_outputs(config: PipelineConfig) -> None:
    config.parquet_output_dir.mkdir(parents=True, exist_ok=True)

    if not config.clean_output:
        return

    for path in _existing_parquet_paths(config.parquet_output_dir, config.parquet_output_file):
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


def _resolve_parquet_records_per_file(config: PipelineConfig) -> int | None:
    if config.parquet_records_per_file is not None:
        return config.parquet_records_per_file
    if config.parquet_shards == 1:
        return None

    catalog_rows = sum(1 for _ in iter_catalog_rows(config.catalog_file))
    if catalog_rows == 0:
        return None
    return max(1, math.ceil(catalog_rows / config.parquet_shards))


def _target_file_size_bytes(config: PipelineConfig) -> int | None:
    if config.parquet_target_file_size_mb is None:
        return None
    return config.parquet_target_file_size_mb * 1024 * 1024


def _existing_parquet_paths(output_dir: Path, output_file: str) -> Iterator[Path]:
    exact_path = output_dir / output_file
    if exact_path.exists():
        yield exact_path

    stem = Path(output_file).stem
    suffix = Path(output_file).suffix
    yield from output_dir.glob(f"{stem}-part-*{suffix}")
