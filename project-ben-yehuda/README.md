# Project Ben-Yehuda Dataset Builder

כלי לבניית דאטהסט מובנה של Project Ben-Yehuda בפורמט Parquet מתוך
`source_data/pseudocatalogue.csv` וקובצי הטקסט התואמים.

## מבנה תיקייה

```text
project-ben-yehuda/
  create_dataset.py          # CLI לבניית הדאטהסט
  split_parquet.py           # CLI לפיצול Parquet קיים
  build_dataset.bat          # הרצת ברירת מחדל ב-Windows
  pby_dataset/
    pipeline.py              # לוגיקת העיבוד והכתיבה
  tests/
    test_pipeline.py         # בדיקות יחידה
  source_data/               # קלט מקומי, לא מנוהל ב-git
  output_parquet/            # פלט Parquet, לא מנוהל ב-git
```

## קלט צפוי

התיקייה `source_data` צריכה להכיל:

- `pseudocatalogue.csv`
- קובצי `.txt` לפי עמודת `path` בקטלוג. לדוגמה, הערך `/p23/m10`
  ייקרא מהנתיב `source_data/p23/m10.txt`.

## שימוש מומלץ

```powershell
python project-ben-yehuda/create_dataset.py
```

ברירת המחדל כותבת 10 קובצי Parquet:

```text
project-ben-yehuda/output_parquet/pby_dataset-part-00001.parquet
project-ben-yehuda/output_parquet/pby_dataset-part-00002.parquet
...
project-ben-yehuda/output_parquet/pby_dataset-part-00010.parquet
```

הפלט כולל את העמודות:

- `text`
- `source`
- `metadata` כ-struct עם שדות כמו `pby_id`, `title`, `authors`,
  `genre`, ו-`filepath_pby`.

## אפשרויות יעילות

```powershell
python project-ben-yehuda/create_dataset.py `
  --batch-size 2048 `
  --workers 16 `
  --compression zstd
```

- `--batch-size` קובע כמה רשומות נכתבות בכל batch ל-Parquet.
- `--workers` קובע כמה קובצי טקסט נקראים במקביל.
- `--compression` שולט בדחיסת Parquet. ברירת המחדל היא `zstd`.
- `--parquet-shards` קובע לכמה קובצי Parquet לשאוף. ברירת המחדל היא `10`.
- `--parquet-records-per-file` קובע מספר רשומות מקסימלי לכל קובץ ועוקף את `--parquet-shards`.
- `--parquet-target-file-size-mb` מבצע רוטציה אחרי שקובץ מגיע בערך לגודל שהוגדר.
- כברירת מחדל יש הסרת כפילויות לפי hash של הטקסט כדי להימנע משמירת טקסטים זהים.

דוגמה לחלוקה לפי מספר רשומות:

```powershell
python project-ben-yehuda/create_dataset.py --parquet-records-per-file 3000
```

דוגמה לחלוקה לפי יעד גודל:

```powershell
python project-ben-yehuda/create_dataset.py --parquet-target-file-size-mb 50
```

## פיצול Parquet קיים

אם כבר נוצר קובץ Parquet יחיד, אפשר לפצל אותו בלי לבנות מחדש מהמקור:

```powershell
python project-ben-yehuda/split_parquet.py `
  --input-file project-ben-yehuda/output_parquet/pby_dataset.parquet `
  --parquet-shards 10
```

## גרסאות ופרסום ב-Hugging Face

גרסת הדאטהסט הנוכחית:

```text
pby-2026.03
```

מדיניות מספור מומלצת:

```text
pby-YYYY.MM[.patch]
```

- `YYYY.MM` הוא חודש גרסת המקור של Project Ben-Yehuda, לא חודש ההעלאה.
- `patch` משמש רק לתיקוני אריזה או metadata ללא שינוי מהותי בקורפוס.
- דוגמאות:
  - `pby-2026.03` - גרסת מקור מרץ 2026.
  - `pby-2026.03.1` - תיקון packaging לאותה גרסת מקור.
  - `pby-2026.06` - עדכון קורפוס חדש מיוני 2026.

בכל פרסום ל-Hugging Face יש לעדכן:

- Dataset card / README ב-HF:
  - גרסה: `pby-2026.03`
  - תאריך מקור: `2026-03`
  - תאריך build מקומי
  - מספר רשומות
  - מספר קבצי Parquet
  - schema: `text`, `source`, `metadata`
  - הערה משפטית: התוכן כפוף לתנאי Project Ben-Yehuda, הקוד בריפו תחת הרישיון של הריפו.
- Files and versions:
  - למחוק קבצי Parquet ישנים שאינם חלק מהגרסה.
  - להעלות רק את `pby_dataset-part-*.parquet` של אותה גרסה.
- Tag או release ב-HF:
  - ליצור tag בשם `pby-2026.03` אחרי שהקבצים וה-card עודכנו.
- אימות לאחר העלאה:
  - לבדוק שה-Dataset Viewer מציג את מספר הרשומות הצפוי.
  - לבדוק שאין shards ישנים או כפולים.
  - לבדוק שה-preview נפתח ושעמודת `metadata` תקינה.

עבור גרסת `pby-2026.03`, הפלט המקומי הצפוי הוא 10 קבצי Parquet:

```text
pby_dataset-part-00001.parquet
...
pby_dataset-part-00010.parquet
```

## בדיקות

```powershell
python -m unittest discover -s project-ben-yehuda/tests
```

## הערות CI

`source_data` אינו מנוהל ב-git. ה-workflow של GitHub Actions משתמש בקבצי
Parquet קיימים אם הם זמינים בתיקיית הפלט, או בונה אותם ישירות מקובצי המקור
כאשר `source_data/pseudocatalogue.csv` זמין בסביבת הריצה.
