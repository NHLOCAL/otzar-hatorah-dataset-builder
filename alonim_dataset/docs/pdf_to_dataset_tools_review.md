# PDF Dataset Parser

כלי ניסיוני להמרת עלונים וקובצי PDF טקסטואליים לדאטסט מובנה.

המטרה אינה OCR מלא, אלא שחזור מבנה המסמך:
עמודות, כותרות, פסקאות, טבלאות, תיבות צד, סדר קריאה ו־RTL.

## Use Case

הקלט הוא PDF שמכיל טקסט אמיתי, אך המבנה שלו מורכב או לא עקבי.

הפלט הרצוי:

- Markdown נקי
- JSON מובנה
- JSONL לדאטסט
- שמירת metadata כמו עמוד, סוג בלוק, bbox, סדר קריאה ומקור החילוץ

## Recommended Stack

### 1. Docling

המנוע הראשי המומלץ.

מתאים להמרת PDF ל־Markdown/JSON עם שחזור Layout, סדר קריאה, טבלאות ומבנה מסמך.

Install:

```bash
pip install docling
````

Links:

* [https://github.com/docling-project/docling](https://github.com/docling-project/docling)
* [https://docling-project.github.io/docling/](https://docling-project.github.io/docling/)

מתי להשתמש:

* ברירת מחדל לרוב העלונים
* כשצריך JSON עשיר ולא רק טקסט שטוח
* כשצריך בסיס נוח לעיבוד Dataset

### 2. Marker

מנוע חלופי איכותי להמרה ל־Markdown/JSON/HTML.

חזק במיוחד במסמכים מורכבים, טבלאות, headers/footers, תמונות ו־Forms. ניתן לשלב LLM לשיפור תוצאות.

Install:

```bash
pip install marker-pdf
```

Install with full extras:

```bash
pip install "marker-pdf[full]"
```

Links:

* [https://github.com/datalab-to/marker](https://github.com/datalab-to/marker)
* [https://pypi.org/project/marker-pdf/](https://pypi.org/project/marker-pdf/)

שים לב: הקוד תחת GPLv3+, לכן יש לבדוק רישוי לפני שילוב במוצר סגור.

מתי להשתמש:

* כ־fallback ל־Docling
* כשצריך Markdown קריא מאוד
* כשיש טבלאות או מבנה מורכב ש־Docling לא פיענח מספיק טוב

### 3. PyMuPDF4LLM

מנוע מהיר וקל יחסית להמרת PDF ל־Markdown/JSON/Text.

מתאים ל־pipeline מהיר, CPU בלבד, עם תמיכה בעמודות, סדר קריאה וטבלאות בסיסיות.

Install:

```bash
pip install pymupdf4llm
```

Links:

* [https://pymupdf.readthedocs.io/en/latest/pymupdf4llm/](https://pymupdf.readthedocs.io/en/latest/pymupdf4llm/)
* [https://pypi.org/project/pymupdf4llm/](https://pypi.org/project/pymupdf4llm/)

שים לב: מבוסס PyMuPDF, לכן יש לבדוק התאמת רישוי לשימוש מסחרי.

מתי להשתמש:

* כשצריך חילוץ מהיר
* כשאין צורך במודל Layout כבד
* ככלי השוואה מול Docling ו־Marker

### 4. pdfplumber

כלי עזר מדויק לניתוח PDF ברמת תווים, קווים, מלבנים, טבלאות וקואורדינטות.

לא מומלץ כמנוע ראשי, אלא ככלי תיקון ושליטה ידנית.

Install:

```bash
pip install pdfplumber
```

Links:

* [https://github.com/jsvine/pdfplumber](https://github.com/jsvine/pdfplumber)
* [https://pypi.org/project/pdfplumber/](https://pypi.org/project/pdfplumber/)

מתי להשתמש:

* זיהוי תיבות צד
* תיקון סדר שורות
* חילוץ אזורים לפי bbox
* חילוץ טבלאות לפי קווים
* Debug ויזואלי של עמודים בעייתיים

### 5. Camelot

כלי ייעודי לחילוץ טבלאות מתוך PDF.

Install:

```bash
pip install "camelot-py[base]"
```

או עם conda:

```bash
conda install -c conda-forge camelot-py
```

Links:

* [https://camelot-py.readthedocs.io/](https://camelot-py.readthedocs.io/)
* [https://pypi.org/project/camelot-py/](https://pypi.org/project/camelot-py/)

מתי להשתמש:

* רק כשיש טבלאות ברורות
* בעיקר טבלאות עם קווים או מבנה עקבי
* לא מתאים לבד לעלונים חופשיים ומעוצבים

### 6. Unstructured

ספרייה לחלוקת מסמכים לאלמנטים כמו Title, NarrativeText, ListItem וטבלאות.

Install:

```bash
pip install "unstructured[pdf]"
```

או עם uv:

```bash
uv add "unstructured[pdf]"
```

Links:

* [https://github.com/Unstructured-IO/unstructured](https://github.com/Unstructured-IO/unstructured)
* [https://docs.unstructured.io/open-source/introduction/quick-start](https://docs.unstructured.io/open-source/introduction/quick-start)

מתי להשתמש:

* כשצריך חלוקה סמנטית לאלמנטים
* כשבונים pipeline ל־RAG
* ככלי השוואה, לא בהכרח כמנוע ראשי לעברית

### 7. MinerU

Document parser מתקדם להמרת PDF ל־Markdown/JSON, כולל סדר קריאה, טבלאות, הסרת headers/footers ותמיכה ב־CPU/GPU.

Install:

```bash
pip install -U "mineru[core]"
```

Links:

* [https://github.com/opendatalab/MinerU](https://github.com/opendatalab/MinerU)
* [https://opendatalab.github.io/MinerU/](https://opendatalab.github.io/MinerU/)

מתי להשתמש:

* לבדיקה מול Docling ו־Marker
* כשיש מסמכים מורכבים במיוחד
* כשצריך פלט Markdown/JSON עשיר עם Layout

## RTL and Hebrew Notes

רוב הספריות אינן מושלמות בעברית ו־RTL.

לכן יש להוסיף שכבת post-processing:

* תיקון סדר מילים ושורות בעברית
* טיפול במספרים ואנגלית בתוך עברית
* תיקון סוגריים וסימני פיסוק
* איחוד שורות שנשברו בגלל Layout
* הסרת headers ו־footers חוזרים
* שמירת כיוון `rtl` ב־HTML/Markdown לפי הצורך

## Suggested Pipeline

1. Convert PDF with Docling.
2. Export structured JSON, not only Markdown.
3. Normalize Hebrew and RTL.
4. Detect low quality pages or blocks.
5. Retry problematic pages with Marker or PyMuPDF4LLM.
6. Use pdfplumber for manual coordinate-based fixes.
7. Export final dataset as JSONL.

## Suggested Output Format

```json
{
  "doc_id": "alon_001",
  "page": 1,
  "block_id": "p1_b04",
  "type": "paragraph",
  "text": "טקסט עברי מתוקן...",
  "raw_text": "טקסט כפי שחולץ מה-PDF",
  "bbox": [72, 140, 510, 220],
  "direction": "rtl",
  "source_tool": "docling",
  "quality_flags": ["rtl_normalized", "line_merged"]
}
```

## Final Recommendation

ברירת המחדל:

```text
Docling -> RTL Normalizer -> JSONL Dataset
```

Fallback מומלץ:

```text
Marker / PyMuPDF4LLM -> pdfplumber fixes -> RTL Normalizer
```