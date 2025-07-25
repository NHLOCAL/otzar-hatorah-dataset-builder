#!/usr/bin/env python3
"""
Script to extract Hebrew (RTL) text from a PDF and export it to JSON using Docling,
equivalent to CLI flags:
  --no-ocr --pdf-backend pypdfium2 --image-export-mode placeholder
"""

import sys
import os
from pathlib import Path

# 1️ Correct backend import:
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions

def strip_quotes(path: str) -> str:
    """Strip surrounding single or double quotes from a string."""
    return path.strip().strip('"').strip("'")

def main():
    # 2️ Get the source PDF path
    if len(sys.argv) > 1:
        src_path = strip_quotes(sys.argv[1])
        print(f"Processing file from command-line argument: {src_path}")
    else:
        src_path = strip_quotes(input("Enter source PDF path: "))

    # 3️ Validate the file exists
    if not os.path.isfile(src_path):
        print(f"Error: Source file not found at '{src_path}'", file=sys.stderr)
        sys.exit(1)

    # 4️ Build destination JSON path
    base_name = os.path.splitext(src_path)[0]
    dest_path = base_name + '.json'

    # 5️ Define pipeline options (no OCR, pypdfium2 backend, placeholder images)
    pdf_opts = PdfPipelineOptions(
        do_ocr=False,
        image_export_mode="placeholder",
        backend=PyPdfiumDocumentBackend
    )

    # 6️ Create the converter with our PDF options
    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pdf_opts)
        }
    )

    # 7️ Run conversion
    try:
        print(f"Converting '{Path(src_path).name}' (OCR disabled, backend=pypdfium2, image-export-mode=placeholder)...")
        result = converter.convert(src_path)
    except Exception as e:
        print(f"Error converting PDF: {e}", file=sys.stderr)
        sys.exit(1)

    # 8️ Save as JSON
    try:
        result.document.save_as_json(Path(dest_path))
        print(f"\nSuccess! JSON exported to:\n{dest_path}")
    except Exception as e:
        print(f"Error writing JSON file: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
