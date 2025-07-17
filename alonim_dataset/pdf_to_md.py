# התקנת התלויות:
# pip install docling

import os
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.datamodel.base_models import InputFormat


def normalize_path(path: str) -> str:
    """
    מסיר מרכאות מקיפות (" או ') מהתחלה והסוף, ומחזיר נתיב תקני לפייתון.
    """
    # חיתוך של " ו-' משני קצות המחרוזת
    normalized = path.strip().strip('"').strip("'")
    # התאמת הנתיב לפורמט מערכת ההפעלה
    return os.path.normpath(normalized)


def pdf_to_markdown_reversed_no_ocr(input_pdf_path: str, output_md_path: str):
    source = input_pdf_path

    # 1. הגדרת אפשרויות עיבוד PDF: מכבים את ה‑OCR
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = False  # מבטלים OCR לחלוטין

    # 2. יצירת המרת מסמך עם פורמט PDF מותאם
    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )

    # 3. המרה ושמירת התוצאה
    result = converter.convert(source)
    markdown = result.document.export_to_markdown()

    # 4. (אופציונלי) הפיכת הסדר של השורות
    reversed_md = "\n".join(markdown.splitlines()[::-1])

    # 5. כתיבת הקובץ
    with open(output_md_path, "w", encoding="utf-8") as f:
        f.write(reversed_md)


if __name__ == "__main__":
    # קריאת נתיב הקובץ מהמשתמש, עם או בלי מרכאות
    raw_input_pdf = input("כתובת קובץ ה‑PDF (input): ")
    raw_output_md = input("כתובת קובץ Markdown (output): ")

    input_pdf = normalize_path(raw_input_pdf)
    output_md = normalize_path(raw_output_md)

    pdf_to_markdown_reversed_no_ocr(input_pdf, output_md)
    print(f"✅ ההמרה הושלמה ללא OCR: {output_md}")
