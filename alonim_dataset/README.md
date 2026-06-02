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
  output_parquet/          # Parquet סופי
  docs/                    # מסמכי מחקר, פרמטרים והנחיות
  scripts/                 # נקודות כניסה להרצה
  src/alonim/              # קוד ניתן לייבוא ובדיקה
  tests/                   # בדיקות יחידה
```

## Recommended Pipeline

```text
Download PDFs -> Docling JSON -> Markdown -> Parquet
```

## Usage

Install dependencies from the repository root or this directory:

```powershell
pip install -r alonim_dataset/requirements.txt
```

Download Docling model artifacts:

```powershell
alonim_dataset\scripts\download_docling_models.bat
```

Download bulletins:

```powershell
python alonim_dataset/scripts/download_beinenu.py all
```

Convert a PDF to Docling JSON:

```powershell
python alonim_dataset/scripts/pdf_to_json.py alonim_dataset/source_data/pdf/example.pdf
```

Experimental RTL layout pass:

```powershell
python alonim_dataset/scripts/pdf_to_json.py alonim_dataset/source_data/pdf/example.pdf --rtl-mirror-input
```

הדגל הניסיוני מריץ Docling במעבר יחיד עם pipeline מותאם: תמונות העמוד
וקואורדינטות תאי הטקסט מוצגות למודלי ה־layout/table כאילו המסמך LTR,
אך הטקסט עצמו נשאר הטקסט המקורי שחולץ מה־PDF. Docling עדיין רץ עם
`--no-ocr` ו־`--table-mode accurate`. לפני שמירת ה־JSON הקוד מחזיר את
קואורדינטות ה־layout למערכת המקורית.

כאשר הקובץ נמצא תחת `source_data/pdf`, פלט ה־JSON נשמר תחת
`intermediate/docling_json` באותו נתיב יחסי. כך עלונים מסדרות שונות בעלי
אותו שם קובץ אינם דורסים זה את זה.

Convert Docling JSON to Markdown:

```powershell
python alonim_dataset/scripts/json_to_md.py alonim_dataset/intermediate/docling_json/example.json
```

שלב ה־Markdown מפעיל נרמול RTL/עברית ופרופילי Layout לפי סדרת העלון
כאשר היא מזוהה מהנתיב או משם הקובץ. הפרופילים הקיימים מכסים את
`מאמרי הרב מרדכי בלס`, `מתיקות הפרשה`, ו־`שיעורי ליל שישי - ישיבת ברכת יצחק`,
עם ברירת מחדל למסמכים אחרים.

Build Parquet from Markdown:

```powershell
python alonim_dataset/scripts/build_parquet.py
```

## Legacy Scripts

הסקריפטים המקוריים נשמרו תחת `scripts/legacy/*_legacy.py` לצורך השוואה
ותאימות זמנית. קוד חדש צריך להשתמש במודולים תחת `src/alonim` וב־CLI החדשים.

## Git Policy

קובצי PDF, JSON/Markdown ביניים ו־Parquet מקומיים אינם מיועדים
להיכנס ל־git כברירת מחדל. אם יש צורך לפרסם snapshot מסוים, יש לעשות זאת
בצורה מכוונת ולתעד את גרסת המקור וה־build.
