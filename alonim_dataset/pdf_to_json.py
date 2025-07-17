#!/usr/bin/env python3
"""
Script to extract Hebrew (RTL) text from a PDF and export it to JSON using Docling.

This script:
  - Accepts a PDF path from a command-line argument or user input.
  - Automatically determines the destination JSON path (same name, new extension).
  - Uses Docling's DocumentConverter to parse the PDF.
  - Saves the resulting JSON with UTF-8 encoding.

Requirements:
  pip install docling

Usage:
  python pdf_to_json_converter.py "C:\\path\\to\\your\\file.pdf"
  OR
  python pdf_to_json_converter.py
  (and then enter the path when prompted)
"""
import sys
import json
import os
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions


def strip_quotes(path: str) -> str:
    """Strip surrounding single or double quotes from a string."""
    return path.strip().strip('"').strip("'")


def main():
    src_path = ""
    # Check for command-line argument first
    if len(sys.argv) > 1:
        src_path = strip_quotes(sys.argv[1])
        print(f"Processing file from command-line argument: {src_path}")
    else:
        # If no argument, prompt the user
        src_path = strip_quotes(input("Enter source PDF path: "))

    # Validate that the source file exists
    if not os.path.isfile(src_path):
        print(f"Error: Source file not found at '{src_path}'", file=sys.stderr)
        sys.exit(1)

    # Automatically determine the destination path
    base_name = os.path.splitext(src_path)[0]
    dest_path = base_name + '.json'

    # Configure PDF pipeline options (default; enable OCR if needed)
    pdf_opts = PdfPipelineOptions()
    # Example: to enable OCR on scanned PDFs, uncomment the following line:
    # pdf_opts.enable_ocr = True

    # Create a converter with PDF format options
    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pdf_opts)
        }
    )

    # Convert the PDF
    try:
        print(f"Converting '{os.path.basename(src_path)}'...")
        result = converter.convert(src_path)
    except Exception as e:
        print(f"Error converting PDF: {e}", file=sys.stderr)
        sys.exit(1)

    # Export to dict
    json_data = result.document.export_to_dict()

    # Write to destination file
    try:
        with open(dest_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        print(f"\nSuccess! JSON successfully exported to:\n{dest_path}")
    except Exception as e:
        print(f"Error writing JSON file: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()