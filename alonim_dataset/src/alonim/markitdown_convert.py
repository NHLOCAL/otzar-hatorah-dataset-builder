from __future__ import annotations

from pathlib import Path


def convert_pdf_to_markdown_with_bidi(pdf_path: Path, output_path: Path | None = None) -> Path:
    pdf_path = pdf_path.expanduser().resolve()
    if not pdf_path.is_file():
        raise FileNotFoundError(f"Source PDF was not found: {pdf_path}")

    if output_path is None:
        output_path = pdf_path.with_suffix(".md")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    from bidi.algorithm import get_display
    from markitdown import MarkItDown

    result = MarkItDown().convert(str(pdf_path))
    fixed_lines = [get_display(line) for line in result.text_content.splitlines()]
    output_path.write_text("\n".join(fixed_lines), encoding="utf-8", newline="\n")
    return output_path
