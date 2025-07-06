import os
import json
import pandas as pd
import pyarrow # נדרש ע"י pandas לכתיבה וקריאה של Parquet
import re # נוסף עבור זיהוי תבניות (אימייל, טלפון)

def anonymize_record(record):
    """
    מבצעת אנונימיזציה לרשומה בודדת ע"י הסרת אימיילים ומספרי טלפון.
    הפונקציה עוברת על כל הערכים במילון, ואם ערך הוא מחרוזת, היא מחפשת בו
    תבניות של אימייל וטלפון ישראלי ומחליפה אותן.

    Args:
        record (dict): רשומת הנתונים כמילון.

    Returns:
        dict: הרשומה לאחר אנונימיזציה.
    """
    # הגדרת ביטויים רגולריים (Regex) לזיהוי מידע רגיש
    # Regex לזיהוי אימייל
    EMAIL_REGEX = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    # Regex לזיהוי מספרי טלפון ישראליים (סלולרי וקווי, עם/בלי מקף, עם/בלי קידומת +972)
    PHONE_REGEX = r'\b(?:(?:\+972-?)|0)(?:[23489]|5[0-9]|7[0-9])-?\d{7}\b'

    # הגדרת מצייני המיקום
    EMAIL_PLACEHOLDER = "[EMAIL_REMOVED]"
    PHONE_PLACEHOLDER = "[PHONE_REMOVED]"

    anonymized_record = {}
    for key, value in record.items():
        if isinstance(value, str):
            # החלפת כתובות אימייל
            sanitized_value = re.sub(EMAIL_REGEX, EMAIL_PLACEHOLDER, value)
            # החלפת מספרי טלפון (על הערך שכבר עבר ניקוי אימיילים)
            sanitized_value = re.sub(PHONE_REGEX, PHONE_PLACEHOLDER, sanitized_value)
            anonymized_record[key] = sanitized_value
        else:
            # אם הערך אינו מחרוזת, השאר אותו כפי שהוא
            anonymized_record[key] = value

    return anonymized_record

def convert_jsonl_to_parquet(input_dir, output_dir, output_filename="combined_dataset.parquet"):
    """
    ממיר מספר קבצי JSONL מתיקיית קלט לקובץ Parquet יחיד בתיקיית פלט,
    תוך כדי ביצוע אנונימיזציה לנתונים (הסרת אימיילים וטלפונים).

    Args:
        input_dir (str): הנתיב לתיקיית הקלט המכילה את קבצי ה-JSONL.
        output_dir (str): הנתיב לתיקיית הפלט שבה יישמר קובץ ה-Parquet.
        output_filename (str): שם קובץ ה-Parquet שייווצר (ברירת מחדל: combined_dataset.parquet).
    """

    # יצירת תיקיית הפלט אם אינה קיימת
    os.makedirs(output_dir, exist_ok=True)
    print(f"תיקיית פלט נוצרה/קיימת: {output_dir}")

    all_records = []
    jsonl_files_found = 0

    # איסוף כל קבצי ה-JSONL מתיקיית הקלט
    for filename in os.listdir(input_dir):
        if filename.endswith(".jsonl"):
            filepath = os.path.join(input_dir, filename)
            jsonl_files_found += 1
            print(f"מעבד ומבצע אנונימיזציה לקובץ: {filepath}") # עודכן טקסט

            with open(filepath, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f):
                    try:
                        record = json.loads(line)
                        # --- שלב האנונימיזציה ---
                        anonymized_record = anonymize_record(record)
                        all_records.append(anonymized_record)
                    except json.JSONDecodeError as e:
                        print(f"אזהרה: שגיאת פענוח JSON בשורה {line_num+1} בקובץ {filename}: {e}")
                        print(f"   השורה הבעייתית: {line.strip()}")
                    except Exception as e:
                        print(f"אזהרה: שגיאה בלתי צפויה בשורה {line_num+1} בקובץ {filename}: {e}")
                        print(f"   השורה הבעייתית: {line.strip()}")

    if jsonl_files_found == 0:
        print(f"לא נמצאו קבצי JSONL בתיקיה: {input_dir}. לא נוצר קובץ Parquet.")
        return

    if not all_records:
        print(f"לא נמצאו רשומות JSON חוקיות באף אחד מקבצי הקלט בתיקיה: {input_dir}. לא נוצר קובץ Parquet.")
        return

    # יצירת DataFrame מכל הרשומות שנאספו
    df = pd.DataFrame(all_records)
    print(f"נאספו ועברו אנונימיזציה {len(all_records)} רשומות מ-{jsonl_files_found} קבצי JSONL.")

    # בניית נתיב קובץ הפלט
    output_parquet_path = os.path.join(output_dir, output_filename)

    # שמירת ה-DataFrame לפורמט Parquet
    # index=False מונע שמירת האינדקס של pandas כעמודה בקובץ Parquet.
    df.to_parquet(output_parquet_path, index=False)
    print(f"\nההמרה והאנונימיזציה הושלמו בהצלחה!")
    print(f"קובץ Parquet נשמר בנתיב: {output_parquet_path}")

# --- הגדרות ---
INPUT_DIRECTORY = "output_dataset"
OUTPUT_DIRECTORY = "converted_parquet"
OUTPUT_PARQUET_FILE = "all_data_combined.parquet" # השם של קובץ הפרקט שייווצר

# --- הפעלת הפונקציה ---
if __name__ == "__main__":

    # הפעלת תהליך ההמרה
    convert_jsonl_to_parquet(INPUT_DIRECTORY, OUTPUT_DIRECTORY, OUTPUT_PARQUET_FILE)

    print("\n-----------------------------------------------------")
    print("שימו לב:")
    print("הנתונים עברו תהליך אנונימיזציה להסרת אימיילים ומספרי טלפון.")
    print(f"הקבצים המקוריים בתיקיה '{INPUT_DIRECTORY}' נשמרו כפי שהם.")
    print(f"קובץ הפרקט האנונימי נוצר בתיקיה '{OUTPUT_DIRECTORY}'.")
    print("-----------------------------------------------------")

    # דוגמה לקריאת קובץ הפרקט שנוצר (לא חובה, רק לבדיקה)
    try:
        df_read = pd.read_parquet(os.path.join(OUTPUT_DIRECTORY, OUTPUT_PARQUET_FILE))
        print("\nדוגמה לתוכן קובץ הפרקט שנוצר (לאחר אנונימיזציה):")
        print(df_read)
        print(f"\nמספר רשומות בקובץ הפרקט: {len(df_read)}")
        print(f"עמודות בקובץ הפרקט: {df_read.columns.tolist()}")
    except FileNotFoundError:
        print(f"\nשגיאה: קובץ הפרקט {os.path.join(OUTPUT_DIRECTORY, OUTPUT_PARQUET_FILE)} לא נמצא לאחר ההמרה.")
    except Exception as e:
        print(f"\nשגיאה בקריאת קובץ הפרקט: {e}")