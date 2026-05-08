from __future__ import annotations

import hashlib
import json
import math
import shutil
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


SOURCE_NAME = "Otzaria Library"
DEFAULT_OUTPUT_BASENAME = "judaic_texts"
DEFAULT_REPO = "Otzaria/otzaria-library"
DEFAULT_RELEASE_ASSET = "otzaria_latest.zip"

METADATA_FIELDS = (
    "title",
    "author",
    "book_name",
    "category",
    "source_collection",
    "source_path",
    "file_hash",
    "github_release",
    "pub_date",
    "pub_place",
    "comp_date",
    "comp_place",
    "description",
    "license_note",
)

PARQUET_SCHEMA = pa.schema(
    [
        pa.field("text", pa.string()),
        pa.field("source", pa.string()),
        pa.field("metadata", pa.struct([pa.field(name, pa.string()) for name in METADATA_FIELDS])),
    ]
)


@dataclass(frozen=True)
class PipelineConfig:
    archive_path: Path
    parquet_output_dir: Path
    parquet_output_file: str = f"{DEFAULT_OUTPUT_BASENAME}.parquet"
    manifest_path: Path | None = None
    metadata_path: Path | None = None
    github_release: str = ""
    parquet_shards: int = 10
    parquet_records_per_file: int | None = None
    parquet_target_file_size_mb: int | None = None
    batch_size: int = 256
    deduplicate_text: bool = True
    compression: str = "zstd"
    clean_output: bool = True
    show_progress: bool = True


@dataclass(frozen=True)
class PipelineResult:
    processed_records: int
    skipped_empty_texts: int
    skipped_non_txt_files: int
    duplicate_records: int
    read_errors: int
    parquet_paths: tuple[Path, ...]


@dataclass(frozen=True)
class _ZipTextEntry:
    source_path: str
    text: str


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


def download_release_asset(
    repo: str = DEFAULT_REPO,
    release: str = "latest",
    asset_name: str = DEFAULT_RELEASE_ASSET,
    output_dir: Path = Path("otzaria-library/source_data"),
    overwrite: bool = False,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / asset_name
    if output_path.exists() and not overwrite:
        return output_path

    api_url = (
        f"https://api.github.com/repos/{repo}/releases/latest"
        if release == "latest"
        else f"https://api.github.com/repos/{repo}/releases/tags/{release}"
    )
    with urllib.request.urlopen(api_url) as response:
        release_payload = json.loads(response.read().decode("utf-8"))

    asset = next((item for item in release_payload.get("assets", []) if item.get("name") == asset_name), None)
    if asset is None:
        raise ValueError(f"Release asset '{asset_name}' was not found in {repo}@{release}")

    with urllib.request.urlopen(asset["browser_download_url"]) as response, output_path.open("wb") as handle:
        shutil.copyfileobj(response, handle, length=1024 * 1024)
    return output_path


def load_manifest(path: Path | None) -> dict[str, dict[str, str]]:
    if path is None or not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        return {}
    return {str(key).replace("\\", "/"): value for key, value in payload.items() if isinstance(value, dict)}


def load_metadata_index(path: Path | None) -> dict[str, dict[str, str]]:
    if path is None or not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    rows = payload if isinstance(payload, list) else payload.get("books", []) if isinstance(payload, dict) else []
    index: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "").strip()
        if title and title not in index:
            index[title] = {str(key): "" if value is None else str(value) for key, value in row.items()}
    return index


def make_record(
    source_path: str,
    text: str,
    metadata_index: dict[str, dict[str, str]],
    manifest: dict[str, dict[str, str]],
    github_release: str,
) -> dict:
    normalized_path = source_path.replace("\\", "/")
    book_name = Path(PurePosixPath(normalized_path)).stem
    source_metadata = metadata_index.get(book_name, {})
    source_collection = _source_collection(normalized_path)

    return {
        "text": text.strip(),
        "source": SOURCE_NAME,
        "metadata": {
            "title": (source_metadata.get("title") or book_name).strip(),
            "author": (source_metadata.get("author") or "").strip(),
            "book_name": book_name,
            "category": _category_from_path(normalized_path),
            "source_collection": source_collection,
            "source_path": normalized_path,
            "file_hash": (manifest.get(normalized_path, {}).get("hash") or "").strip(),
            "github_release": github_release,
            "pub_date": (source_metadata.get("pubDate") or "").strip(),
            "pub_place": (source_metadata.get("pubPlace") or "").strip(),
            "comp_date": (source_metadata.get("compDate") or "").strip(),
            "comp_place": (source_metadata.get("compPlace") or "").strip(),
            "description": (source_metadata.get("heDesc") or source_metadata.get("heShortDesc") or "").strip(),
            "license_note": _license_note(source_collection),
        },
    }


def build_dataset(config: PipelineConfig) -> PipelineResult:
    _validate_config(config)
    _prepare_outputs(config)

    manifest = load_manifest(config.manifest_path)
    metadata_index = load_metadata_index(config.metadata_path)
    records_per_file = _resolve_parquet_records_per_file(config)
    writer = _ParquetShardWriter(
        output_dir=config.parquet_output_dir,
        output_file=config.parquet_output_file,
        records_per_file=records_per_file,
        target_file_size_bytes=_target_file_size_bytes(config),
        compression=config.compression,
    )

    processed_records = 0
    skipped_empty_texts = 0
    skipped_non_txt_files = 0
    duplicate_records = 0
    read_errors = 0
    seen_text_hashes: set[str] = set()

    try:
        batch: list[dict] = []
        entries = iter_zip_text_entries(config.archive_path)
        if config.show_progress:
            entries = tqdm(entries, desc="Processing Otzaria texts", unit="file")

        for entry in entries:
            if entry.source_path == "":
                skipped_non_txt_files += 1
                continue
            if entry.text == "":
                read_errors += 1
                continue
            if not entry.text.strip():
                skipped_empty_texts += 1
                continue

            record = make_record(
                source_path=entry.source_path,
                text=entry.text,
                metadata_index=metadata_index,
                manifest=manifest,
                github_release=config.github_release,
            )

            if config.deduplicate_text:
                text_hash = hashlib.sha256(record["text"].encode("utf-8")).hexdigest()
                if text_hash in seen_text_hashes:
                    duplicate_records += 1
                    continue
                seen_text_hashes.add(text_hash)

            batch.append(record)
            if len(batch) >= config.batch_size:
                writer.write_records(batch)
                processed_records += len(batch)
                batch = []

        if batch:
            writer.write_records(batch)
            processed_records += len(batch)
    finally:
        writer.close()

    return PipelineResult(
        processed_records=processed_records,
        skipped_empty_texts=skipped_empty_texts,
        skipped_non_txt_files=skipped_non_txt_files,
        duplicate_records=duplicate_records,
        read_errors=read_errors,
        parquet_paths=tuple(writer.paths),
    )


def iter_zip_text_entries(archive_path: Path) -> Iterator[_ZipTextEntry]:
    with zipfile.ZipFile(archive_path) as archive:
        for info in archive.infolist():
            normalized_path = info.filename.replace("\\", "/")
            if info.is_dir() or not normalized_path.lower().endswith(".txt"):
                yield _ZipTextEntry("", "non-txt")
                continue
            try:
                with archive.open(info) as handle:
                    text = handle.read().decode("utf-8-sig")
            except (OSError, UnicodeDecodeError):
                yield _ZipTextEntry(normalized_path, "")
                continue
            yield _ZipTextEntry(normalized_path, text)


def _validate_config(config: PipelineConfig) -> None:
    if not config.archive_path.exists():
        raise FileNotFoundError(f"Archive was not found: {config.archive_path}")
    if config.batch_size < 1:
        raise ValueError("batch_size must be at least 1")
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


def _resolve_parquet_records_per_file(config: PipelineConfig) -> int | None:
    if config.parquet_records_per_file is not None:
        return config.parquet_records_per_file
    if config.parquet_target_file_size_mb is not None or config.parquet_shards == 1:
        return None

    valid_records = _count_candidate_records(config)
    if valid_records == 0:
        return None
    return max(1, math.ceil(valid_records / config.parquet_shards))


def _count_candidate_records(config: PipelineConfig) -> int:
    seen_text_hashes: set[str] = set()
    count = 0
    for entry in iter_zip_text_entries(config.archive_path):
        if entry.source_path == "" or not entry.text.strip():
            continue
        if config.deduplicate_text:
            text_hash = hashlib.sha256(entry.text.strip().encode("utf-8")).hexdigest()
            if text_hash in seen_text_hashes:
                continue
            seen_text_hashes.add(text_hash)
        count += 1
    return count


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


def _source_collection(source_path: str) -> str:
    return source_path.split("/", 1)[0] if "/" in source_path else ""


def _category_from_path(source_path: str) -> str:
    parts = PurePosixPath(source_path).parts
    if len(parts) < 2:
        return ""
    try:
        otzaria_index = parts.index("אוצריא")
    except ValueError:
        return "/".join(parts[1:-1])
    return "/".join(parts[otzaria_index + 1 : -1])


def _license_note(source_collection: str) -> str:
    return (
        "License depends on the source collection and original content owner; "
        f"source collection: {source_collection or 'unknown'}."
    )
