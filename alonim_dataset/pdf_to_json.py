#!/usr/bin/env python3
"""
Wraps the Docling CLI so you pass only the PDF path,
and it auto‑injects:
  --no-ocr --pdf-backend pypdfium2 --image-export-mode placeholder
  --to json --output <same-folder-as-PDF>
"""

import sys
import os
from pathlib import Path
from docling.cli.main import app

def strip_quotes(path: str) -> str:
    return path.strip().strip('"').strip("'")

def main():
    # 1️ Read only the PDF path
    if len(sys.argv) > 1:
        raw = sys.argv[1]
    else:
        raw = input("Enter source PDF path: ")
    src_path = strip_quotes(raw)

    # 2️ Validate the PDF exists
    if not os.path.isfile(src_path):
        print(f"Error: Source file not found at '{src_path}'", file=sys.stderr)
        sys.exit(1)

    # 3️ Determine the output directory
    output_dir = Path(src_path).parent

    # 4️ Auto‑inject all Docling flags, pointing output at the PDF’s folder
    sys.argv = [
        "docling",
        "--no-ocr",
        "--pdf-backend", "pypdfium2",
        "--image-export-mode", "placeholder",
        src_path,
        "--to", "json",
        "--output", str(output_dir),
    ]

    # 5️ Invoke the same Typer app behind `docling` CLI
    try:
        app()
        print(f"Success! JSON written to: {output_dir / (Path(src_path).stem + '.json')}")
    except Exception as e:
        print(f"Error during conversion: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
