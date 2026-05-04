# Alonim Dataset

תת-פרויקט לאיסוף, המרה ובניית dataset מובנה מעלוני PDF מאתר BeInenu.

המבנה מפריד בין קוד, קלט גולמי, קבצי ביניים ופלטים סופיים כדי לאפשר
תחזוקה והרחבה נקיות.

## Directory Layout

```text
alonim_dataset/
  source_data/
    pdf/                  # עלונים מקוריים שהורדו
    samples/              # דוגמאות קטנות לבדיקה ידנית
  intermediate/
    docling_json/          # פלט JSON של Docling
    markdown/              # Markdown לאחר עיבוד Layout/RTL
    logs/                  # לוגים ודוחות ריצה
  output_jsonl/            # JSONL shards לדאטהסט
  output_parquet/          # Parquet סופי
  docs/                    # מסמכי מחקר, פרמטרים והנחיות
  scripts/                 # נקודות כניסה להרצה
  src/alonim/              # קוד ניתן לייבוא ובדיקה
  tests/                   # בדיקות יחידה
```

## Recommended Pipeline

```text
Download PDFs -> Docling JSON -> Markdown -> JSONL -> Parquet
```

## Usage

Install dependencies from the repository root or this directory:

```powershell
pip install -r alonim_dataset/requirements.txt
```

Download bulletins:

```powershell
python alonim_dataset/scripts/download_beinenu.py all
```

Convert a PDF to Docling JSON:

```powershell
python alonim_dataset/scripts/pdf_to_json.py alonim_dataset/source_data/pdf/example.pdf
```

Convert Docling JSON to Markdown:

```powershell
python alonim_dataset/scripts/json_to_md.py alonim_dataset/intermediate/docling_json/example.json
```

Build JSONL from Markdown:

```powershell
python alonim_dataset/scripts/build_jsonl.py
```

Convert JSONL to Parquet:

```powershell
python alonim_dataset/scripts/jsonl_to_parquet.py
```

## Legacy Scripts

הסקריפטים המקוריים נשמרו תחת `scripts/*_legacy.py` לצורך השוואה ותאימות
זמנית. קוד חדש צריך להשתמש במודולים תחת `src/alonim` וב־CLI החדשים.

## Git Policy

קובצי PDF, JSON/Markdown ביניים, JSONL ו־Parquet מקומיים אינם מיועדים
להיכנס ל־git כברירת מחדל. אם יש צורך לפרסם snapshot מסוים, יש לעשות זאת
בצורה מכוונת ולתעד את גרסת המקור וה־build.
