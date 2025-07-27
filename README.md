<p align="center">
  <!-- Hugging Face Datasets -->
  <a href="https://huggingface.co/datasets/nhlocal/otzar-hatorah">
    <img alt="Otzar HaTorah on Hugging Face" src="https://img.shields.io/badge/HuggingFace-Otzar%20HaTorah-yellow?logo=huggingface&style=flat">
  </a>
  <a href="https://huggingface.co/datasets/nhlocal/project-ben-yehuda">
    <img alt="Project Ben-Yehuda on Hugging Face" src="https://img.shields.io/badge/HuggingFace-Project%20Ben--Yehuda-yellow?logo=huggingface&style=flat">
  </a>

  <!-- GitHub Actions: CI status -->
  <a href="https://github.com/NHLOCAL/The-Digital-Genizah/actions/workflows/upload_dataset.yml">
    <img alt="Build: Otzar HaTorah" src="https://github.com/NHLOCAL/The-Digital-Genizah/actions/workflows/upload_dataset.yml/badge.svg">
  </a>
  <a href="https://github.com/NHLOCAL/The-Digital-Genizah/actions/workflows/upload_pby_dataset.yml">
    <img alt="Build: Project Ben-Yehuda" src="https://github.com/NHLOCAL/The-Digital-Genizah/actions/workflows/upload_pby_dataset.yml/badge.svg">
  </a>

  <!-- License -->
  <a href="https://github.com/NHLOCAL/The-Digital-Genizah/blob/main/LICENSE">
    <img alt="License: Apache 2.0" src="https://img.shields.io/github/license/NHLOCAL/The-Digital-Genizah?color=blue">
  </a>

  <!-- Contributions Welcome -->
  <a href="https://github.com/NHLOCAL/The-Digital-Genizah/issues">
    <img alt="Contributions Welcome" src="https://img.shields.io/badge/contributions-welcome-brightgreen.svg">
  </a>
</p>

# הגניזה הדיגיטלית (The Digital Genizah)

מאגר קוד ותהליכי עבודה ליצירת תשתית של מערכי נתונים (datasets) בעברית, עם דגש על טקסטים תורניים, והעלאתם באופן אוטומטי למרכז הנתונים של [Hugging Face](https://huggingface.co/datasets).

---

### English Version Below

## <img src="https://flagcdn.com/w40/il.png" width="20"> גירסה עברית

### אודות הפרויקט

מטרת מאגר "הגניזה הדיגיטלית" היא להרחיב ולהעשיר את משאבי השפה העברית הזמינים לאימון מודלי שפה מודרניים (LLMs). הפרויקט יוצר תשתית לאיסוף, עיבוד ופרסום של טקסטים ממקורות מגוונים, בדגש מיוחד על:
-   **העולם התורני:** ספרות רבנית, ספרי הלכה, פרשנות וטקסטים לימודיים.
-   **מושגים תלמודיים:** הרחבת יכולתם של מודלים להבין ולהשתמש בטרמינולוגיה וסגנון שיח ייחודיים.
-   **העולם החרדי:** טקסטים המשקפים את השפה והתרבות העכשווית, כגון עלוני שבת וספרות מודרנית.
-   **עברית כללית:** העשרת מאגרי השפה בעברית תקנית וספרותית.

התוצר הסופי הוא מערכי נתונים מובנים, נקיים וזמינים לקהילת הבינה המלאכותית, במטרה לקדם את יכולות העיבוד וההבנה של השפה העברית על כל רבדיה.

### ⚠️ הבהרה משפטית חשובה

הקוד במאגר זה מופץ תחת רישיון **Apache 2.0**. עם זאת, רישיון זה חל **אך ורק על קבצי הקוד והסקריפטים** שנוצרו עבור פרויקט זה.

התוכן הטקסטואלי עצמו, הנאסף ממקורות חיצוניים, **אינו כפוף לרישיון זה**. כל מקור מידע (ספר, מאמר, עלון וכו') מגיע עם תנאי שימוש, זכויות יוצרים, ורישיונות משלו.

**האחריות לבדיקת רישיונות השימוש, זכויות היוצרים, והתנאים המשפטיים של כל מקור תוכן חלה באופן בלעדי על המשתמש.**

מערכי הנתונים הנוצרים על ידי הכלים במאגר זה מוגשים **'כמות שהם' (As Is)**, ללא כל אחריות, מפורשת או משתמעת, לגבי חוקיות השימוש בתוכן. על המשתמש לוודא כי אופן השימוש שלו בנתונים תואם את הרישיון המקורי של התוכן.

### מקורות המידע

המאגר מיועד לעבד ולהנגיש טקסטים ממגוון רחב של מקורות. נכון לעכשיו, המאגר מתמקד במקורות הבאים:
-   **פרויקט בן-יהודה:** יצירות קלאסיות מהספרות העברית, מעובדות ומועלות כדאטהסט נפרד.
-   **גמ"ח אוצר התורה:** אוסף טקסטים בסגנון למדני-ישיבתי עדכני, המייצג שפה תורנית חיה.
-   **ספריית "אוצריא" המלאה:** פרויקט משלים ונפרד הממיר את כלל ספריית התוכנה "אוצריא" ומעלה אותה ל-Hugging Face. ניתן למצוא את המאגר הייעודי לכך כאן: [NHLOCAL/otzaria-library](https://github.com/NHLOCAL/otzaria-library).
-   **מקורות נוספים:** בעתיד יתווספו מקורות כמו עלוני שבת ומאגרים נוספים שניתן יהיה להמיר ולהתאים.

### תהליך העבודה

תהליך העבודה במאגר הוא אוטומטי ברובו ומבוסס על השלבים הבאים:
1.  **איסוף ועיבוד ראשוני:** סקריפטים ייעודיים (כמו `create_dataset_markdown.py` ו-`create_dataset.py`) סורקים תיקיות המכילות קבצים גולמיים (`DOCX`, `PDF`, `TXT`), מחלצים את הטקסט, ומייצרים קבצי `JSONL` סטנדרטיים ומפוצלים.
2.  **ניקוי וסטנדרטיזציה:** הסקריפט `jsonl_to_parquet.py` קורא את קבצי ה-JSONL, מבצע פעולות ניקוי מתקדמות (כגון תיקון טקסט הפוך, הסרת כפילויות, ואנונימיזציה), וממיר את הנתונים לפורמט `Parquet` היעיל לאחסון וניתוח.
3.  **העלאה ל-Hugging Face:** תהליכי `GitHub Actions` (`upload_dataset.yml`, `upload_pby_dataset.yml`) מופעלים אוטומטית בעת עדכון קבצי המקור, מריצים את שלבי העיבוד, ומעלים את התוצר הסופי למאגר הנתונים המתאים ב-Hugging Face.

### תרומה לפרויקט
אנו מקדמים בברכה כל תרומה לפרויקט! ניתן לתרום בדרכים הבאות:
-   הוספת סקריפטים לעיבוד מקורות מידע חדשים.
-   שיפור לוגיקת הניקוי והנרמול של הטקסטים.
-   הצעה והוספה של מקורות מידע עבריים נוספים.
-   דיווח על באגים ופתיחת [Issues](https://github.com/NHLOCAL/The-Digital-Genizah/issues).

---

## <img src="https://flagcdn.com/w40/us.png" width="20">  English Version

### About The Project

This repository contains code and workflows to create a foundational infrastructure for Hebrew datasets, with a special focus on Judaic texts, and to automatically upload them to the [Hugging Face Hub](https://huggingface.co/datasets).

The primary goal of "The Digital Genizah" is to expand and enrich the Hebrew language resources available for training modern AI models (LLMs). The project provides an infrastructure for collecting, processing, and publishing texts from diverse sources, with a particular emphasis on:
-   **Judaic Scholarship (Torani World):** Rabbinic literature, Halakhic (legal) texts, commentaries, and educational materials.
-   **Talmudic Concepts:** Enhancing the ability of models to understand and utilize the unique terminology and discourse style of Talmudic study.
-   **The Haredi (Ultra-Orthodox) World:** Texts reflecting contemporary language and culture, such as Shabbat pamphlets and modern literature.
-   **General Hebrew:** Enriching language corpora with standard and literary Hebrew.

The final output is a collection of structured, clean datasets made available to the AI community to advance the processing and comprehension capabilities of the Hebrew language in all its forms.

### ⚠️ Important Legal Disclaimer

The code in this repository is distributed under the **Apache 2.0 License**. However, this license applies **only to the code and script files** created for this project.

The textual content itself, which is collected from external sources, is **not subject to this license**. Each source of information (book, article, pamphlet, etc.) comes with its own terms of use, copyrights, and licenses.

**The responsibility for verifying the usage licenses, copyrights, and legal terms of each content source rests solely with the user.**

The datasets generated by the tools in this repository are provided **'AS IS'**, without any warranty, express or implied, regarding the legality of using the content. The user must ensure that their use of the data complies with the original license of the content.

### Data Sources

This repository is designed to process and provide access to texts from a wide range of sources. Currently, the project focuses on:
-   **Project Ben-Yehuda:** Classic works of Hebrew literature, processed and uploaded as a separate dataset.
-   **Gmach Otzar HaTorah:** A collection of texts in a modern, yeshiva-style discourse, representing a living, scholarly Hebrew.
-   **The Complete "Otzaria" Library:** A separate, complementary project that converts the entire library of the "Otzaria" software and uploads it to Hugging Face. The dedicated repository for this effort can be found here: [NHLOCAL/otzaria-library](https://github.com/NHLOCAL/otzaria-library).
-   **Additional Sources:** Future plans include adding sources like Shabbat pamphlets and other corpora that can be converted and adapted.

### The Workflow

The workflow is largely automated and based on the following steps:
1.  **Ingestion and Initial Processing:** Dedicated scripts (like `create_dataset_markdown.py` and `create_dataset.py`) scan directories of raw files (`DOCX`, `PDF`, `TXT`), extract text, and generate standardized, sharded `JSONL` files.
2.  **Cleaning and Standardization:** The `jsonl_to_parquet.py` script reads the JSONL files, performs advanced cleaning operations (such as fixing reversed text, deduplication, and anonymization), and converts the data into the efficient `Parquet` format.
3.  **Upload to Hugging Face:** `GitHub Actions` workflows (`upload_dataset.yml`, `upload_pby_dataset.yml`) are automatically triggered when source files are updated. They execute the processing pipeline and upload the final artifacts to the appropriate dataset repository on Hugging Face.

### Contributing
We welcome all contributions! You can help by:
-   Adding new scripts to process new data sources.
-   Improving the text cleaning and normalization logic.
-   Suggesting and adding new Hebrew data sources.
-   Reporting bugs and opening [Issues](https://github.com/NHLOCAL/The-Digital-Genizah/issues).