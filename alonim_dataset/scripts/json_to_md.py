from __future__ import annotations

import argparse
from pathlib import Path

import _bootstrap  # noqa: F401
from alonim.markdown_convert import convert_docling_json_to_markdown


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert a Docling JSON document to Markdown.")
    parser.add_argument("json", type=Path)
    parser.add_argument("-o", "--output", type=Path, default=None)
    args = parser.parse_args()

    path = convert_docling_json_to_markdown(args.json, args.output)
    print(f"Markdown written to: {path}")


if __name__ == "__main__":
    main()
