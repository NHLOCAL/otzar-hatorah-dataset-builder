# מדריך פרמטרים ל־Docling CLI

להלן הסבר מלא ומפורט לכל פרמטר שורת פקודה בכלי `docling`, כולל ערכי ברירת מחדל.

---

## 🎯 פרמטרי קלט (Arguments)

| פרמטר           | תיאור |
|-----------------|--------|
| `input_sources` | נתיב (או נתיבים) לקבצי PDF שברצונך להמיר. ניתן להשתמש גם בנתיב תיקיה או ב־URL ישיר. **חובה.**<br>🔹 ברירת מחדל: אין – נדרש לספק |

---

## ⚙️ אפשרויות (Options)

### קלט ופלט

| פרמטר      | תיאור | ברירת מחדל |
|------------|--------|-------------|
| `--from`   | פורמט הקלט: `pdf`, `docx`, `pptx`, `html`, `image`, `csv`, `xlsx`, `json_docling` וכו'. | כל הפורמטים |
| `--to`     | פורמט הפלט: `md`, `json`, `html`, `html_split_page`, `text`, `doctags`. | `md` |
| `--output` | נתיב תיקיית הפלט לקבצים המומרצים. | `.` |

### OCR (זיהוי טקסט מתמונות)

| פרמטר          | תיאור | ברירת מחדל |
|----------------|--------|-------------|
| `--ocr` / `--no-ocr` | האם לבצע OCR במסמכים סרוקים. | מופעל |
| `--force-ocr`   | מחליף טקסט קיים בטקסט מ־OCR. | כבוי |
| `--ocr-engine`  | מנוע ה־OCR: `easyocr`, `tesseract`, `ocrmac`, `rapidocr`, `tesserocr`. | `easyocr` |
| `--ocr-lang`    | שפות ה־OCR: לדוג׳ `"eng,heb"`. | אין |

### תמונות

| פרמטר                 | תיאור | ברירת מחדל |
|-----------------------|--------|-------------|
| `--image-export-mode` | מצב ייצוא תמונות: `placeholder`, `embedded`, `referenced`. | `embedded` |

### פייפליין ומודלים

| פרמטר         | תיאור | ברירת מחדל |
|---------------|--------|-------------|
| `--pipeline`  | סוג פייפליין: `standard`, `vlm`, `asr`. | `standard` |
| `--vlm-model` | מודל Vision: `smoldocling`, `granite_vision`, `granite_vision_ollama`. | `smoldocling` |
| `--asr-model` | מודל ASR: `whisper_tiny`, `small`, `medium`, `large`, `turbo`. | `whisper_tiny` |

### טבלאות וקוד

| פרמטר                  | תיאור | ברירת מחדל |
|------------------------|--------|-------------|
| `--pdf-backend`        | מנוע PDF: `pypdfium2`, `dlparse_v1`, `dlparse_v2`, `dlparse_v4`. | `dlparse_v2` |
| `--table-mode`         | מצב עיבוד טבלאות: `fast`, `accurate`. | `accurate` |
| `--enrich-code`        | שיפור קוד בתוכן. | כבוי |
| `--enrich-formula`     | שיפור נוסחאות. | כבוי |
| `--enrich-picture-classification` | סיווג תמונות. | כבוי |
| `--enrich-picture-description` | תיאור תמונות. | כבוי |

### מודלים חיצוניים

| פרמטר                    | תיאור | ברירת מחדל |
|--------------------------|--------|-------------|
| `--artifacts-path`       | נתיב למודלים לשימוש אופליין. | אין |
| `--enable-remote-services` | הפעלת מודלים בענן. | כבוי |
| `--allow-external-plugins` | טעינת פלאגינים חיצוניים. | כבוי |
| `--show-external-plugins` | הצגת פלאגינים מותקנים. | כבוי |

### ביצועים ודיבאג

| פרמטר                     | תיאור | ברירת מחדל |
|---------------------------|--------|-------------|
| `--abort-on-error`        | עצירה בשגיאה ראשונה. | כבוי |
| `--document-timeout`      | טיימאאוט למסמך. | אין |
| `--num-threads`           | מספר תהליכים. | `4` |
| `--device`                | סוג מעבד: `auto`, `cpu`, `cuda`, `mps`. | `auto` |
| `--verbose`, `-v`, `-vv`  | לוגינג: info/debug. | `0` |
| `--show-layout`           | הצגת גבולות תיבות. | כבוי |

### דיבאג ויזואלי

| פרמטר                          | תיאור | ברירת מחדל |
|--------------------------------|--------|-------------|
| `--debug-visualize-cells`      | הצגת חלוקת תאים. | כבוי |
| `--debug-visualize-ocr`        | הצגת תיבות OCR. | כבוי |
| `--debug-visualize-layout`     | הצגת חלוקת מבנה. | כבוי |
| `--debug-visualize-tables`     | הצגת טבלאות. | כבוי |

### כללי

| פרמטר       | תיאור |
|-------------|--------|
| `--version` | הצגת גרסת Docling |
| `--logo`    | הצגת לוגו |
| `--help`    | עזרה |