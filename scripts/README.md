# Shared Dataset Scripts

תיקייה זו מכילה כלי תשתית משותפים לכל מקורות הדאטה בפרויקט.

## `upload_directory_to_hf.py`

סקריפט כללי להעלאת תיקיית פלט אל Hugging Face Dataset repo.

דוגמה:

```powershell
python scripts/upload_directory_to_hf.py `
  --repo-id NHLOCAL/judaic-texts-corpus `
  --local-dir otzaria-library/output_parquet `
  --path-in-repo data `
  --delete-pattern "data/*.parquet" `
  --commit-message "Update Otzaria dataset"
```

## מחיקת קבצים ישנים לפני העלאה

הארגומנט `--delete-pattern` הוא pattern יחסי לשורש ה-Hugging Face repo, לא
יחסי ל-`--path-in-repo`.

לכן, כאשר מעלים לתיקייה `data`, יש להשתמש כך:

```text
--path-in-repo data
--delete-pattern "data/*.parquet"
```

המחיקה מבוצעת בקריאה מפורשת ל-`HfApi.delete_files` לפני ההעלאה, ולא דרך
`delete_patterns` של `upload_folder`. זה חשוב מפני שב-`upload_folder`, כאשר
מוגדר `path_in_repo`, תבניות המחיקה מפורשות יחסית לאותה תיקייה. למשל
`path_in_repo="data"` יחד עם `delete_patterns="data/*.parquet"` עלול לכוון
בטעות אל `data/data/*.parquet`.

הפיצול לשתי פעולות, מחיקה ואז העלאה, מוודא שלא נשארים shards ישנים כאשר מספר
קובצי ה-Parquet משתנה בין גרסאות.
