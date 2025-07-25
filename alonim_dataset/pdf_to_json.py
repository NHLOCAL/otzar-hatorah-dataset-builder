#!/usr/bin/env python3
"""
Wraps the Docling CLI’s Typer app so you only pass in the PDF path,
and it auto‑injects:
  --no-ocr --pdf-backend pypdfium2 --image-export-mode placeholder
  --to json --output <temp-dir>
Then moves the resulting <basename>.json into <basename>.json alongside the PDF.
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path
from docling.cli.main import app

def strip_quotes(path: str) -> str:
    return path.strip().strip('"').strip("'")

def main():
    # 1️ Read only the PDF path from the user
    if len(sys.argv) > 1:
        raw = sys.argv[1]
    else:
        raw = input("Enter source PDF path: ")
    src_path = strip_quotes(raw)

    # 2️ Validate the PDF exists
    if not os.path.isfile(src_path):
        print(f"Error: Source file not found at '{src_path}'", file=sys.stderr)
        sys.exit(1)

    # 3️ Determine the final JSON path
    base = Path(src_path).with_suffix("")
    dest_path = base.with_suffix(".json")

    # 4️ Make a temporary directory for Docling output
    temp_out = tempfile.mkdtemp(prefix="docling_out_")

    # 5️ Auto‑inject all Docling flags to sys.argv
    sys.argv = [
        "docling",
        "--no-ocr",
        "--pdf-backend", "pypdfium2",
        "--image-export-mode", "placeholder",
        src_path,
        "--to", "json",
        "--output", temp_out,     # must be a directory
    ]

    # 6️ Run the CLI’s Typer app
    try:
        app()
    except Exception as e:
        # Clean up on error
        shutil.rmtree(temp_out, ignore_errors=True)
        print(f"Error during conversion: {e}", file=sys.stderr)
        sys.exit(1)

    # 7️ Locate the generated JSON inside temp_out
    generated = Path(temp_out) / (base.name + ".json")
    if not generated.is_file():
        shutil.rmtree(temp_out, ignore_errors=True)
        print(f"Error: expected output not found at '{generated}'", file=sys.stderr)
        sys.exit(1)

    # 8️ Move it to the desired dest_path (overwriting if necessary)
    try:
        shutil.move(str(generated), str(dest_path))
        print(f"Success! JSON exported to:\n{dest_path}")
    except Exception as e:
        print(f"Error moving JSON file: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        # 9️ Clean up the temporary directory
        shutil.rmtree(temp_out, ignore_errors=True)

if __name__ == "__main__":
    main()
