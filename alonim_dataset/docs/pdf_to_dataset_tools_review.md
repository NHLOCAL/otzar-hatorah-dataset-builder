# PDF Dataset Parser

כלי ניסיוני להמרת עלונים וקובצי PDF טקסטואליים לדאטסט מובנה.

המטרה אינה OCR מלא, אלא שחזור מבנה המסמך:
עמודות, כותרות, פסקאות, טבלאות, תיבות צד, סדר קריאה ו־RTL.

## Use Case

הקלט הוא PDF שמכיל טקסט אמיתי, אך המבנה שלו מורכב או לא עקבי.

הפלט הרצוי:

- Markdown נקי
- JSON מובנה
- Parquet לדאטסט
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

## Actual Conversion Trials

נבדקו בפועל כמה עלונים מתוך `alonim_dataset/source_data/pdf` כדי להשוות בין
המקור לבין תוצאות ההמרה ולזהות כשלים שחוזרים לפי סדרת עלונים.

הדוגמאות שנבדקו:

* `תשפ_ג/בראשית/ויקרא שמם אדם/מאמרי הרב מרדכי בלס.pdf`
* `תשפ_ג/ויקרא/פרשת ויקרא/מתיקות הפרשה - הרב אריה לוין.pdf`
* `תשפ_ג/בראשית/פרשת בראשית, הגדרת יום ולילה/שיעורי ליל שישי - ישיבת ברכת יצחק.pdf`

### Docling

Docling נבדק עם הפרמטרים המומלצים:

```powershell
docling --to json --from pdf --no-ocr --pdf-backend pypdfium2 --image-export-mode placeholder <נתיב קובץ>
```

תוצאות בפועל:

* החילוץ הטקסטואלי בעברית היה הטוב ביותר מבין הכלים שנבדקו בפועל.
* ברוב גוף הטקסט Docling החזיר עברית בסדר לוגי תקין, בניגוד לכלים שהחזירו טקסט הפוך או סדר קריאה חזותי בלבד.
* פלט JSON של Docling נתן `bbox`, מספרי עמודים, labels ויחסי children שמאפשרים לבנות שכבת post-processing אמינה.
* ב־`מאמרי הרב מרדכי בלס` זוהה מבנה דו־עמודי קבוע יחסית. יש לקרוא עמודה ימנית לפני שמאלית, תוך שמירה על כותרות/בלוקים מרכזיים לפני העמודות.
* ב־`מתיקות הפרשה` חלק מטקסט המותג והכותרת הופיע כ־children של `picture`. אם מדלגים על children של תמונה, מאבדים טקסט אמיתי מהכותרת.
* ב־`מתיקות הפרשה` טקסט מנוקד בפסוקים יצא לעיתים עם רווחים מלאכותיים בתוך מילים, למשל תבניות בסגנון `דַּ בֵּר אֶ ל`. נדרש נרמול ייעודי שמאחה רווחים אחרי סימני ניקוד עבריים.
* ב־`שיעורי ליל שישי - ישיבת ברכת יצחק` רוב המסמך מתנהג כעמודה אחת רחבה. פיצול אגרסיבי לעמודות עלול להזיק, ולכן נדרש profile שומר סדר אנכי.
* שמירת פלט לפי `pdf_path.stem` בלבד אינה מספיקה: שמות כמו `מאמרי הרב מרדכי בלס.pdf`, `מתיקות הפרשה - הרב אריה לוין.pdf`, ו־`שיעורי ליל שישי - ישיבת ברכת יצחק.pdf` חוזרים בתיקיות רבות. בפועל נמצאו עשרות מופעים חוזרים, ולכן פלטי JSON/Markdown חייבים לשמר את הנתיב היחסי מתחת ל־`source_data/pdf`.

מסקנה: Docling הוא מנוע ההמרה הראשי המתאים ביותר כרגע, אבל רק יחד עם שכבת post-processing לפי סדרה: שמירת נתיב יחסי, פרופילי layout, הכללת children של `picture`, ונרמול עברית/RTL.

### pdfplumber

נבדק כ־debug extractor ישיר על עמוד ראשון מהדוגמאות.

תוצאות בפועל:

* הטקסט העברי חזר בסדר חזותי הפוך, למשל שורות עבריות נקראו מימין לשמאל אך נשמרו כטקסט הפוך.
* ב־`מאמרי הרב מרדכי בלס` וב־`שיעורי ליל שישי` ניתן לראות את תוכן המקור, אבל הוא דורש היפוך/שחזור משמעותי לפני שימוש כדאטהסט.
* היתרון העיקרי הוא גישה לקואורדינטות ולניתוח עמוד ידני, לא יצירת Markdown סופי.

מסקנה: pdfplumber אינו מתאים כמנוע ראשי לעלונים בעברית, אבל הוא כלי שימושי לאבחון `bbox`, בדיקת סדר שורות, ותיקון נקודתי של אזורים בעייתיים.

### MarkItDown

נבדק דרך `alonim_dataset/scripts/pdf_to_md_reversed.py`, כלומר המרה עם MarkItDown ולאחר מכן `python-bidi`.

תוצאות בפועל:

* ב־`מאמרי הרב מרדכי בלס` התקבל טקסט קריא בחלקו, אך סדר הקריאה היה מעורבב: סוף/אמצע העמודות הופיעו לפני פתיחת המאמר, ופסקאות הוצמדו זו לזו.
* ב־`מתיקות הפרשה` התקבלו קטעים רבים בסדר קריאה לא יציב, עם ערבוב בין כותרת, גוף, וקטעי המשך.
* הפעלת `python-bidi` על פלט שטוח שיפרה תצוגה מסוימת, אך לא פתרה את בעיית layout והעמודות.

מסקנה: MarkItDown אינו מתאים כרגע כמנוע ראשי לעלוני BeInenu. ניתן לשמור אותו ככלי השוואה מהיר או fallback נקודתי, אך לא לבנות עליו pipeline איכותי לדאטהסט עברי.

### Tools Not Yet Validated In This Run

Marker, PyMuPDF4LLM, Camelot, Unstructured ו־MinerU נשארים מועמדים להשוואות עתידיות, אך לא הורצו בפועל במסגרת בדיקת הדוגמאות הנוכחית. אין להסיק מהמסמך שהם טובים או גרועים יותר לעלונים עבריים עד שתתבצע הרצה מדגמית זהה ותתועד תוצאה בפועל.

## Suggested Pipeline

1. Convert PDF with Docling.
2. Save JSON under `intermediate/docling_json` with the same relative path as the source PDF under `source_data/pdf`.
3. Export structured JSON, not only Markdown.
4. Detect bulletin profile by recurring series/rabbi/file path.
5. Reconstruct reading order from Docling JSON using profile-aware layout rules.
6. Include text children nested under `picture` nodes when they contain real text.
7. Normalize Hebrew and RTL: parentheses, punctuation, niqqud spacing, split quote fragments, and paragraph joining.
8. Detect low quality pages or blocks.
9. Retry problematic pages with future validated fallbacks only when there is concrete evidence that they improve the output.
10. Use pdfplumber for manual coordinate-based debugging and fixes.
11. Export final dataset as Parquet.

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
Docling JSON -> Profile-aware Layout Reconstruction -> Hebrew/RTL Normalizer -> Markdown -> Parquet Dataset
```

Fallback אפשרי לאחר בדיקה נקודתית:

```text
Validated alternate parser -> pdfplumber coordinate fixes -> Hebrew/RTL Normalizer
```

נכון לבדיקה הנוכחית, אין fallback שאושר כטוב יותר מ־Docling עבור הדוגמאות
שנבדקו. הפתרון המקצועי הוא לא להחליף מנוע מיד, אלא לחזק את שכבת העיבוד
שמעל Docling לפי סדרות העלונים החוזרות.
