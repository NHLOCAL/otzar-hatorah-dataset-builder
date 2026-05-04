#!/usr/bin/env python3
"""
pdf_to_md.py

Convert a PDF to Markdown using MarkItDown, then apply
the Unicode BiDi algorithm to render Hebrew (RTL) lines correctly.

Requirements:
    pip install markitdown[all] python-bidi

Usage:
    python pdf_to_md.py path/to/your/file.pdf
    OR
    python pdf_to_md.py
    (then enter the PDF path at the prompt)
"""

import sys
import os
from markitdown import MarkItDown
from bidi.algorithm import get_display  # BiDi reordering

def strip_quotes(path: str) -> str:
    """Strip surrounding quotes."""
    return path.strip().strip('"').strip("'")

def get_source_path() -> str:
    """Get PDF path from argv or prompt."""
    if len(sys.argv) > 1:
        return strip_quotes(sys.argv[1])
    return strip_quotes(input("Enter source PDF path: "))

def main():
    src = get_source_path()
    if not os.path.isfile(src):
        print(f"Error: File not found at '{src}'", file=sys.stderr)
        sys.exit(1)

    dest = os.path.splitext(src)[0] + '.md'
    print(f"Converting '{os.path.basename(src)}' to Markdown…")

    converter = MarkItDown()
    try:
        result = converter.convert(src)
    except Exception as e:
        print(f"Error during conversion: {e}", file=sys.stderr)
        sys.exit(1)

    raw_md = result.text_content

    # Apply BiDi algorithm per line for correct RTL ordering
    fixed_lines = [get_display(line) for line in raw_md.splitlines()]
    fixed_md = "\n".join(fixed_lines)

    try:
        with open(dest, 'w', encoding='utf-8') as f:
            f.write(fixed_md)
        print(f"Success! Markdown exported to:\n{dest}")
    except Exception as e:
        print(f"Error writing Markdown: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
