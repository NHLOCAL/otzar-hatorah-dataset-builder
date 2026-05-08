# Otzaria Library Dataset Builder

כלי לבניית גרסת Parquet מסודרת של ספריית
[Otzaria/otzaria-library](https://github.com/Otzaria/otzaria-library) עבור
הדאטהסט הקיים ב-Hugging Face:
[NHLOCAL/judaic-texts-corpus](https://huggingface.co/datasets/NHLOCAL/judaic-texts-corpus).

המסלול כאן לא משכפל את ריפו המקור המלא. במקום זאת הוא מוריד רק את קובץ
ה-Release הרשמי `otzaria_latest.zip`, קורא ממנו קובצי `.txt` ב-streaming,
ומייצר קובצי Parquet מפוצלים תחת `output_parquet`.

## מבנה תיקייה

```text
otzaria-library/
  create_dataset.py              # CLI לבניית הדאטהסט
  otzaria_dataset/
    pipeline.py                  # לוגיקת הורדה, קריאה וכתיבה ל-Parquet
  tests/
    test_pipeline.py             # בדיקות יחידה
  source_data/                   # קלט מקומי, לא מנוהל ב-git
  output_parquet/                # פלט Parquet, לא מנוהל ב-git
```

## קלט

ברירת המחדל מצפה לקבצים הבאים תחת `otzaria-library/source_data`:

- `otzaria_latest.zip`
- `files_manifest.json`
- `metadata.json`

ב-GitHub Actions הקבצים הללו נלקחים מתוך ה-Release של
`Otzaria/otzaria-library`, כך שלא נדרש clone של הריפו הכבד.

## שימוש מקומי

הורדת ה-Release האחרון ובניית Parquet:

```powershell
python otzaria-library/create_dataset.py `
  --download-release-asset `
  --github-release latest `
  --parquet-target-file-size-mb 128
```

אם כבר הורדת את ה-archive ואת קבצי המטא־דאטה:

```powershell
python otzaria-library/create_dataset.py `
  --archive-path otzaria-library/source_data/otzaria_latest.zip `
  --manifest-path otzaria-library/source_data/files_manifest.json `
  --metadata-path otzaria-library/source_data/metadata.json `
  --github-release library-140
```

## פלט

הפלט נכתב כברירת מחדל אל:

```text
otzaria-library/output_parquet/judaic_texts-part-00001.parquet
otzaria-library/output_parquet/judaic_texts-part-00002.parquet
...
```

ה-schema:

- `text`
- `source`
- `metadata` כ-struct עם שדות:
  - `title`
  - `author`
  - `book_name`
  - `category`
  - `source_collection`
  - `source_path`
  - `file_hash`
  - `github_release`
  - `pub_date`
  - `pub_place`
  - `comp_date`
  - `comp_place`
  - `description`
  - `license_note`

## עדכון Hugging Face

ה-workflow `.github/workflows/upload_otzaria_dataset.yml`:

1. מוריד את `otzaria_latest.zip` מה-Release הנבחר של `Otzaria/otzaria-library`.
2. מחלץ ממנו `files_manifest.json` ו-`metadata.json`.
3. בונה קובצי Parquet מפוצלים.
4. מעלה אותם אל `NHLOCAL/judaic-texts-corpus` תחת `data/`.
5. מוחק לפני ההעלאה קובצי `data/*.parquet` ישנים כדי למנוע shards יתומים.

ניתן להריץ ידנית דרך GitHub Actions ולבחור:

- `otzaria_release`: תג Release, למשל `library-140`, או `latest`.
- `parquet_target_file_size_mb`: גודל shard משוער, ברירת מחדל `128`.

## יעילות

- אין clone מלא של upstream.
- אין חילוץ מלא של ה-ZIP לתיקייה זמנית.
- הטקסטים נקראים מתוך ה-ZIP ונכתבים ל-Parquet ב-batches.
- דה-דופליקציה נעשית לפי SHA-256 של הטקסט המנוקה.
- פיצול Parquet יכול להתבצע לפי מספר shards, מספר רשומות לקובץ, או יעד גודל.

## רישוי

ספריית Otzaria כוללת מקורות ברישיונות שונים. לכן הדאטהסט שומר בכל רשומה את
`source_collection`, `source_path`, ו-`license_note`. אין להניח שכל הטקסטים
כפופים לרישיון אחד; יש לבדוק את רישיון המקור לפי התיקייה והמקור המקורי.

## בדיקות

```powershell
python -m unittest discover -s otzaria-library/tests
```
