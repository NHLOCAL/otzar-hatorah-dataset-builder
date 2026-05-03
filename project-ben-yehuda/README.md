# Project Ben-Yehuda Dataset Builder

כלי לבניית דאטהסט מובנה של Project Ben-Yehuda בפורמט Parquet מתוך
`source_data/pseudocatalogue.csv` וקובצי הטקסט התואמים.

## מבנה תיקייה

```text
project-ben-yehuda/
  create_dataset.py          # CLI לבניית הדאטהסט
  pby_dataset/
    pipeline.py              # לוגיקת העיבוד והכתיבה
  tests/
    test_pipeline.py         # בדיקות יחידה
  source_data/               # קלט מקומי, לא מנוהל ב-git
  output_parquet/            # פלט Parquet, לא מנוהל ב-git
  output_jsonl/              # פלט JSONL אופציונלי לתאימות לאחור
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

ברירת המחדל כותבת קובץ אחד:

```text
project-ben-yehuda/output_parquet/pby_dataset.parquet
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
- כברירת מחדל יש הסרת כפילויות לפי hash של הטקסט כדי להימנע משמירת טקסטים זהים.

## JSONL לתאימות לאחור

המסלול המהיר אינו דורש JSONL. אם בכל זאת צריך להפיק shards ישנים:

```powershell
python project-ben-yehuda/create_dataset.py --write-jsonl
```

אפשר לשנות את גודל ה-shards:

```powershell
python project-ben-yehuda/create_dataset.py --write-jsonl --jsonl-records-per-file 5000
```

## בדיקות

```powershell
python -m unittest discover -s project-ben-yehuda/tests
```

## הערות CI

`source_data` אינו מנוהל ב-git. ה-workflow של GitHub Actions מנסה לבנות
Parquet ישירות מקובצי המקור אם הם זמינים בסביבת הריצה. אם הם אינם זמינים,
הוא נופל למסלול התאימות הישן שממיר את קובצי `output_jsonl` הקיימים.
