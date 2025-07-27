import os
import pandas as pd
import json
from tqdm import tqdm
import pathlib

# --- הגדרות ---
SCRIPT_DIR = pathlib.Path(__file__).parent
SOURCE_DATA_DIR = SCRIPT_DIR / "source_data"
CATALOG_FILE = SOURCE_DATA_DIR / "pseudocatalogue.csv"
TEXT_FILES_BASE_DIR = SOURCE_DATA_DIR

OUTPUT_DIR = SCRIPT_DIR / "output_jsonl"
# --- שינוי: הגדרת שם בסיס לקבצי הפלט ---
OUTPUT_BASENAME = "pby_dataset"

# --- חדש: קבוע לשליטה על גודל כל קובץ פלט (מספר רשומות) ---
# שנה את הערך הזה לפי הצורך. 5,000 רשומות לקובץ זה איזון טוב.
RECORDS_PER_CHUNK = 2500

def create_pby_dataset():
    """
    Reads the Project Ben-Yehuda catalog, merges it with text files,
    and creates a sharded JSONL dataset, splitting it into multiple files.
    """
    if not SOURCE_DATA_DIR.exists() or not CATALOG_FILE.exists():
        print(f"Error: Source data not found.")
        print(f"Please place 'pseudocatalogue.csv' and the text directories inside '{SOURCE_DATA_DIR}'")
        return

    print("Loading catalog file...")
    try:
        df = pd.read_csv(CATALOG_FILE, keep_default_na=False)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    OUTPUT_DIR.mkdir(exist_ok=True)
    
    print(f"Processing {len(df)} records and creating sharded dataset...")

    # --- שינוי: לוגיקת פיצול הקבצים ---
    processed_count = 0
    not_found_count = 0
    
    current_chunk_records = 0
    part_num = 1
    output_file_handle = None

    try:
        for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing books"):
            # פתיחת קובץ חדש אם צריך (בתחילת הריצה או כשהקודם התמלא)
            if output_file_handle is None or current_chunk_records >= RECORDS_PER_CHUNK:
                if output_file_handle:
                    output_file_handle.close()
                
                # הפורמט :05d מבטיח שמות כמו 00001, 00002... לצורך מיון נכון
                part_filename = OUTPUT_DIR / f"{OUTPUT_BASENAME}-part-{part_num:05d}.jsonl"
                print(f"\nCreating new chunk file: {part_filename}")
                output_file_handle = open(part_filename, 'w', encoding='utf-8')
                current_chunk_records = 0
                part_num += 1

            metadata = row.to_dict()
            relative_path = metadata.get('path', '').strip()
            if not relative_path:
                continue

            text_file_path_str = relative_path.lstrip('/') + ".txt"
            text_file_path = TEXT_FILES_BASE_DIR / text_file_path_str
            
            try:
                with open(text_file_path, 'r', encoding='utf-8') as f:
                    text_content = f.read()
            except FileNotFoundError:
                not_found_count += 1
                continue

            record = {
                "text": text_content.strip(),
                "source": "Project Ben-Yehuda",
                "metadata": {
                    "pby_id": metadata.get('ID'),
                    "title": metadata.get('title'),
                    "authors": metadata.get('authors'),
                    "translators": metadata.get('translators'),
                    "author_uris": metadata.get('author_uris'),
                    "translator_uris": metadata.get('translator_uris'),
                    "original_language": metadata.get('original_language'),
                    "genre": metadata.get('genre'),
                    "source_edition": metadata.get('source_edition'),
                    "filepath_pby": relative_path
                }
            }

            output_file_handle.write(json.dumps(record, ensure_ascii=False) + '\n')
            processed_count += 1
            current_chunk_records += 1
    
    finally:
        # --- חשוב: סגירת הקובץ האחרון שנשאר פתוח ---
        if output_file_handle:
            output_file_handle.close()

    print("\n--- Processing Complete ---")
    print(f"Successfully processed and wrote {processed_count} records into {part_num - 1} files.")
    if not_found_count > 0:
        print(f"Warning: Skipped {not_found_count} records because their text files were not found.")
    print(f"Dataset created in: {OUTPUT_DIR}")

if __name__ == '__main__':
    create_pby_dataset()