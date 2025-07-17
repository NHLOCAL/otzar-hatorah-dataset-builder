#!/usr/bin/env python3
"""
Script to extract Hebrew (RTL) text from a PDF and export it to JSON using Docling.

This script:
  - Prompts the user for a source PDF path and a destination JSON path.
  - Strips surrounding quotes from input paths for Python compatibility.
  - Uses Docling's DocumentConverter to parse the PDF.
  - Exports the document structure to a Python dict and saves as JSON.
  - Saves the resulting JSON with UTF-8 encoding.

Requirements:
  pip install docling

Usage:
  python docling_pdf_to_json.py
"""
import sys
import json
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions


def strip_quotes(path: str) -> str:
    """Strip surrounding single or double quotes from a string."""
    return path.strip().strip('"').strip("'")


def main():
    # Prompt for source and destination paths
    src = strip_quotes(input("Enter source PDF path: "))
    dest = strip_quotes(input("Enter destination JSON path: "))

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
        result = converter.convert(src)
    except Exception as e:
        print(f"Error converting PDF: {e}", file=sys.stderr)
        sys.exit(1)

    # Export to dict (no export_to_json method available)
    json_data = result.document.export_to_dict()

    # Write to destination file
    try:
        with open(dest, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        print(f"JSON successfully exported to: {dest}")
    except Exception as e:
        print(f"Error writing JSON file: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()