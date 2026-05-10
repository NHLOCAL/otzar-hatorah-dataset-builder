from __future__ import annotations

import sys
from pathlib import Path

from .config import DEFAULT_DOCLING_JSON_DIR, DEFAULT_PDF_DIR


DEFAULT_DOCLING_ARGS = (
    "--no-ocr",
    "--pdf-backend",
    "pypdfium2",
    "--image-export-mode",
    "placeholder",
    "--to",
    "json",
)


def convert_pdf_to_docling_json(
    pdf_path: Path,
    output_dir: Path = DEFAULT_DOCLING_JSON_DIR,
    source_root: Path = DEFAULT_PDF_DIR,
    extra_args: tuple[str, ...] = (),
) -> Path:
    pdf_path = pdf_path.expanduser().resolve()
    if not pdf_path.is_file():
        raise FileNotFoundError(f"Source PDF was not found: {pdf_path}")

    output_path = docling_json_output_path(pdf_path, output_dir, source_root)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    from docling.cli.main import app

    original_argv = sys.argv[:]
    sys.argv = [
        "docling",
        *DEFAULT_DOCLING_ARGS,
        *extra_args,
        str(pdf_path),
        "--output",
        str(output_path.parent),
    ]

    try:
        app()
    finally:
        sys.argv = original_argv

    return output_path


def docling_json_output_path(pdf_path: Path, output_dir: Path, source_root: Path = DEFAULT_PDF_DIR) -> Path:
    pdf_path = pdf_path.expanduser().resolve()
    output_dir = output_dir.expanduser()
    source_root = source_root.expanduser().resolve()

    try:
        relative_path = pdf_path.relative_to(source_root)
    except ValueError:
        relative_path = Path(pdf_path.name)

    return output_dir / relative_path.with_suffix(".json")
