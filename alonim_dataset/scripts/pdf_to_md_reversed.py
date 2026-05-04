from __future__ import annotations

import argparse
from pathlib import Path

import _bootstrap  # noqa: F401
from alonim.markitdown_convert import convert_pdf_to_markdown_with_bidi


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert a PDF to Markdown with BiDi display normalization.")
    parser.add_argument("pdf", type=Path)
    parser.add_argument("-o", "--output", type=Path, default=None)
    args = parser.parse_args()

    path = convert_pdf_to_markdown_with_bidi(args.pdf, args.output)
    print(f"Markdown written to: {path}")


if __name__ == "__main__":
    main()
