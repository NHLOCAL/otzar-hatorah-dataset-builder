# Otzaria Library Dataset Builder

כלי לבניית גרסת Parquet מסודרת של ספריית
[Otzaria/otzaria-library](https://github.com/Otzaria/otzaria-library) עבור
הדאטהסט הקיים ב-Hugging Face:
[NHLOCAL/judaic-texts-corpus](https://huggingface.co/datasets/NHLOCAL/judaic-texts-corpus).

המסלול כאן לא משכפל את ריפו המקור המלא. במקום זאת הוא מוריד את
`otzaria_latest.zip` ואת `otzaria_dicta_latest.zip` מה-Release האחרון,
ומשלים אותם בקובצי `.txt` שקיימים בארכיון `otzaria_latest.zip` של
`library-141` אך חסרים ב-Release האחרון. ההפרש מחושב באמצעות
`files_manifest.json` של שתי הגרסאות, ללא תלות בשם תיקיית המקור. הקבצים
נקראים ב-streaming ונכתבים לקובצי Parquet מפוצלים תחת `output_parquet`.

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
- `otzaria_dicta_latest.zip` אם רוצים לכלול גם את ספרי Dicta
- `otzaria_library_141.zip` אם רוצים להשלים ספרים שהוסרו אחרי גרסה 141
- `files_manifest_141.json` שחולץ מהארכיון של גרסה 141
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
  --extra-release-asset otzaria_dicta_latest.zip `
  --parquet-target-file-size-mb 128
```

אם כבר הורדת את ה-archive ואת קבצי המטא־דאטה:

```powershell
python otzaria-library/create_dataset.py `
  --archive-path otzaria-library/source_data/otzaria_latest.zip `
  --extra-archive-path otzaria-library/source_data/otzaria_dicta_latest.zip `
  --supplement-archive-path otzaria-library/source_data/otzaria_library_141.zip `
  --supplement-manifest-path otzaria-library/source_data/files_manifest_141.json `
  --manifest-path otzaria-library/source_data/files_manifest.json `
  --metadata-path otzaria-library/source_data/metadata.json `
  --github-release library-143
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

1. מוריד את `otzaria_latest.zip` ואת `otzaria_dicta_latest.zip` מה-Release הנבחר של `Otzaria/otzaria-library`.
2. מוריד גם את `otzaria_latest.zip` של `library-141` בשם מקומי נפרד.
3. מחלץ מה-Release הנבחר את `files_manifest.json` ו-`metadata.json`.
4. משווה בין המניפסטים וממיר את נתיבי המקור לנתיבי ה-ZIP השטוחים מסוג `אוצריא/...`.
5. בונה קובצי Parquet מהגרסה הנבחרת ומ-Dicta, ומוסיף מגרסה 141 רק ספרים שנתיבם אינו קיים בגרסה הנבחרת.
6. מעלה אותם אל `NHLOCAL/judaic-texts-corpus` תחת `data/`.
7. מוחק לפני ההעלאה קובצי `data/*.parquet` ישנים כדי למנוע shards יתומים.

הארכיונים של ה-Release הנבחר מעובדים לפני הארכיון ההיסטורי. לכן, כאשר אותו
ספר מופיע בשתי הגרסאות, נשמרת הגרסה האחרונה בלבד. אין להסתמך על שמות כמו
`extraBooks` או `sefariaToOtzaria`: תהליך יצירת ה-Release משטח את תיקיות
המקור, והארכיון מכיל נתיבים שמתחילים ב-`אוצריא`.

ניתן לבדוק אילו ספרים יתווספו ללא הורדת ה-ZIP על ידי הורדת
`files_manifest.json` משני תגי ה-GitHub והשוואת נתיבי ה-`.txt` לאחר הסרת
החלק שלפני רכיב `אוצריא`.

ההעלאה מתבצעת דרך הסקריפט המשותף `scripts/upload_directory_to_hf.py`. הסקריפט
אינו נמצא בתיקייה זו משום שהוא כלי תשתיתי כללי המשמש כמה מקורות דאטה. מחיקת
הקבצים הישנים מבוצעת לפני ההעלאה באמצעות `HfApi.delete_files`, עם pattern
יחסי לשורש ה-Hugging Face repo:

```text
data/*.parquet
```

אין להשתמש כאן ב-pattern יחסי ל-`path_in_repo`, מפני שהעלאה לתוך `data/`
כבר מגדירה את יעד ההעלאה בלבד. הפרדה זו מונעת מצב שבו pattern כמו
`data/*.parquet` מפורש בטעות כ-`data/data/*.parquet`.

ניתן להריץ ידנית דרך GitHub Actions ולבחור:

- `otzaria_release`: תג Release, למשל `library-143`, או `latest`.
- `parquet_target_file_size_mb`: גודל shard משוער, ברירת מחדל `128`.

## יעילות

- אין clone מלא של upstream.
- אין חילוץ מלא של ה-ZIP לתיקייה זמנית.
- הטקסטים נקראים מתוך ה-ZIP ונכתבים ל-Parquet ב-batches.
- דה-דופליקציה נעשית תחילה לפי הנתיב הלוגי של הספר, עם עדיפות לארכיון
  שהוגדר ראשון, ולאחר מכן לפי SHA-256 של הטקסט המנוקה.
- פיצול Parquet יכול להתבצע לפי מספר shards, מספר רשומות לקובץ, או יעד גודל.

## רישוי

ספריית Otzaria כוללת מקורות ברישיונות שונים. לכן הדאטהסט שומר בכל רשומה את
`source_collection`, `source_path`, ו-`license_note`. אין להניח שכל הטקסטים
כפופים לרישיון אחד; יש לבדוק את רישיון המקור לפי התיקייה והמקור המקורי.

## בדיקות

```powershell
python -m unittest discover -s otzaria-library/tests
```
