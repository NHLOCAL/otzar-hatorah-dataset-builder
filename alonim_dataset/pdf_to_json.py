#!/usr/bin/env python3
"""
Script to extract Hebrew (RTL) text from a PDF and export it to JSON using Docling,
with hard‑coded options (no CLI flags):
  - OCR disabled (--no-ocr)
  - PDF backend = pypdfium2
  - Image export mode = placeholder
"""

import sys
import os
from pathlib import Path

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions, PdfBackend

def strip_quotes(path: str) -> str:
    """Strip surrounding single or double quotes from a string."""
    return path.strip().strip('"').strip("'")

def main():
    # קבלת הנתיב ל־PDF
    if len(sys.argv) > 1:
        src_path = strip_quotes(sys.argv[1])
        print(f"Processing file from command-line argument: {src_path}")
    else:
        src_path = strip_quotes(input("Enter source PDF path: "))

    # בדיקה שהקובץ קיים
    if not os.path.isfile(src_path):
        print(f"Error: Source file not found at '{src_path}'", file=sys.stderr)
        sys.exit(1)

    # קביעת מסלול ה־JSON
    base_name = os.path.splitext(src_path)[0]
    dest_path = base_name + '.json'

    # הגדרת אופציות לפייפליין של PDF
    pdf_opts = PdfPipelineOptions(
        do_ocr=False,                    # equivalent to --no-ocr
        backend=PdfBackend.PYPDFIUM2,    # equivalent to --pdf-backend pypdfium2 :contentReference[oaicite:0]{index=0}
        generate_page_images=False,      # no page bitmaps: placeholder mode :contentReference[oaicite:1]{index=1}
        generate_picture_images=False,   # no embedded images: placeholder mode :contentReference[oaicite:2]{index=2}
        generate_table_images=False      # no table images: placeholder mode :contentReference[oaicite:3]{index=3}
    )

    # יצירת DocumentConverter
    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pdf_opts)
        }
    )

    # המרת PDF
    try:
        print(f"Converting '{Path(src_path).name}' (OCR disabled, backend=pypdfium2, images=placeholder)...")
        result = converter.convert(src_path)
    except Exception as e:
        print(f"Error converting PDF: {e}", file=sys.stderr)
        sys.exit(1)

    # שמירת התוצאה כ־JSON
    try:
        result.document.save_as_json(Path(dest_path))
        print(f"\nSuccess! JSON exported to:\n{dest_path}")
    except Exception as e:
        print(f"Error writing JSON file: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
