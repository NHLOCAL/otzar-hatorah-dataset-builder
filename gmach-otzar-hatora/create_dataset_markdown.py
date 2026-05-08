import os
import re
import json
import pathlib
import logging
import subprocess
import tempfile
from tqdm import tqdm
import multiprocessing
import pandas as pd

# --- תלויות חדשות ---
try:
    import docx
    from pypdf import PdfReader
except ImportError:
    print("Error: Missing required libraries. Please run: pip install python-docx pypdf")
    exit()

# --- שינוי: הוספנו את הייבוא הראשי גם כאן, זה נוהג טוב ---
# אך הייבוא בתוך הפונקציה הוא זה שפותר את הבעיה.
from markitdown import MarkItDown

# הגדרת לוגינג בסיסי להסתרת פלט עודף
logging.basicConfig(level=logging.ERROR)

# --- קבועים: יש לעדכן את הנתיבים לסביבה שלך ---
SCRIPT_DIR = pathlib.Path(__file__).parent
ROOT_DIRECTORY = SCRIPT_DIR / "Otzar_Hatorah_Books"  # דוגמה לנתיב יחסי
SOFFICE_PATH = r"C:\Program Files\LibreOffice\program\soffice"  # יש לוודא שנתיב זה נכון

# --- הגדרות פלט לפיצול קבצי Parquet ---
OUTPUT_DIR = SCRIPT_DIR / "output_parquet"
OUTPUT_BASENAME = "otzar_hatorah_dataset"
CHUNK_SIZE = 250  # מספר הרשומות בכל קובץ פלט
MANIFEST_PATH = OUTPUT_DIR / "processed_sources.json"

# --- עדכון: נוספו תבניות להתעלמות ---
PATTERNS_TO_DELETE = ["~$", "desktop.ini", "~"] # מתעלם מקבצי וורד זמניים וקבצים שמתחילים בטילדה
IGNORED_EXTENSIONS = [".rar", ".zip", ".xps", ".ini", "", ".tmp", ".db"]

# --- Constants for Hebrew text processing ---
FINAL_LETTERS = frozenset('םןץףך')
NON_FINAL_EQUIVALENTS = frozenset('כמנפצ')
REVERSED_CANARY_WORDS = frozenset([':atad', 'egami', 'ptth', 'lmth', 'gnp/egami'])
HEBREW_REVERSED_CANARIES = frozenset(['אוה', 'רועיש', 'הזש', 'רוסא', 'יבר', 'ןכא'])
HEBREW_CORRECT_CANARIES = frozenset(['הוא', 'שיעור', 'שזה', 'אסור', 'רבי', 'אכן'])


def load_progress_manifest(manifest_path: pathlib.Path) -> tuple[int, set[str]]:
    """
    קורא manifest קטן שמחליף את סריקת קבצי הביניים.
    ה-manifest שומר אילו קבצי מקור כבר נכתבו בהצלחה ל-Parquet.
    """
    if not manifest_path.exists():
        return 0, set()

    with manifest_path.open('r', encoding='utf-8') as handle:
        payload = json.load(handle)

    return int(payload.get("last_part", 0)), set(payload.get("processed_sources", []))


def save_progress_manifest(manifest_path: pathlib.Path, last_part: int, processed_sources: set[str]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = manifest_path.with_suffix(".tmp")
    payload = {
        "last_part": last_part,
        "processed_sources": sorted(processed_sources),
    }
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    tmp_path.replace(manifest_path)


def write_parquet_part(records: list[dict], output_dir: pathlib.Path, basename: str, part_num: int) -> pathlib.Path:
    part_filename = output_dir / f"{basename}-part-{part_num:05d}.parquet"
    dataframe = pd.DataFrame(records)
    dataframe.to_parquet(part_filename, index=False)
    return part_filename


def is_garbled_text(text: str) -> bool:
    if len(text) < 100:
        return False

    quote_density = text.count('"') / len(text)
    if quote_density > 0.04:
        return True

    words = [word for word in re.split(r'[^א-ת]+', text) if word]
    if len(words) > 50:
        average_word_length = sum(len(w) for w in words) / len(words)
        if average_word_length < 2.7:
            return True

    return False


def _pre_process_text(text: str) -> str:
    return re.sub(r'(?<=[א-ת])\n(?=[א-ת])', '', text)


def fix_hebrew_encoding(text: str) -> str:
    try:
        return text.encode('latin-1').decode('windows-1255')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def detect_and_fix_reversed_hebrew(text: str) -> tuple[str, bool]:
    sample = text[:10000]

    for canary in HEBREW_CORRECT_CANARIES:
        if re.search(r'\b' + re.escape(canary) + r'\b', sample):
            return text, False

    for canary in REVERSED_CANARY_WORDS:
        if canary in sample:
            return text[::-1], True

    for canary in HEBREW_REVERSED_CANARIES:
        if re.search(r'\b' + re.escape(canary) + r'\b', sample):
            return text[::-1], True

    words_to_sample = [word for word in re.split(r'[^א-ת]+', sample) if word]
    if not words_to_sample:
        return text, False

    reversed_evidence_score = 0
    for word in words_to_sample:
        if len(word) > 1:
            if word[0] in FINAL_LETTERS:
                reversed_evidence_score += 1
            if word[-1] in NON_FINAL_EQUIVALENTS:
                reversed_evidence_score += 1

    if reversed_evidence_score >= 3:
        return text[::-1], True
    return text, False


def process_text_field(text: str, cid_threshold: int = 10) -> tuple[str | None, bool]:
    was_reversed = False
    if not isinstance(text, str):
        return text, was_reversed

    if is_garbled_text(text):
        return None, was_reversed

    if text.count('(cid:') > cid_threshold:
        return None, was_reversed

    text = re.sub(r'\(cid:\d+\)', '', text)
    processed_text = _pre_process_text(text)
    processed_text = fix_hebrew_encoding(processed_text)
    processed_text, was_reversed = detect_and_fix_reversed_hebrew(processed_text)
    return processed_text, was_reversed


def anonymize_record(record: dict) -> dict:
    email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    phone_regex = r'\b(?:(?:\+972-?)|0)(?:[23489]|5[0-9]|7[0-9])-?\d{7}\b'

    anonymized_record = {}
    for key, value in record.items():
        if isinstance(value, str):
            sanitized_value = re.sub(email_regex, "[EMAIL_REMOVED]", value)
            sanitized_value = re.sub(phone_regex, "[PHONE_REMOVED]", sanitized_value)
            anonymized_record[key] = sanitized_value
        else:
            anonymized_record[key] = value
    return anonymized_record


def extract_file_metadata(file_path: pathlib.Path) -> dict:
    """מחלצת מטא-דאטה מקבצים נתמכים (docx, pdf)."""
    metadata = {}
    ext = file_path.suffix.lower()
    try:
        if ext == '.docx':
            doc = docx.Document(file_path)
            props = doc.core_properties
            metadata = {
                'author': props.author,
                'created': props.created.isoformat() if props.created else None,
                'modified': props.modified.isoformat() if props.modified else None,
                'last_modified_by': props.last_modified_by,
                'subject': props.subject,
                'title_meta': props.title,
                'version': props.version
            }
        elif ext == '.pdf':
            with open(file_path, 'rb') as f:
                reader = PdfReader(f)
                info = reader.metadata
                if info:
                    metadata = {
                        'author': info.author,
                        'creator': info.creator,
                        'producer': info.producer,
                        'subject': info.subject,
                        'title_meta': info.title
                    }
    except Exception:
        pass
    return {k: v for k, v in metadata.items() if v}


def process_single_file(file_path_str: str) -> dict or None:
    """
    *** פונקציית העובד המרכזית ***
    מקבלת נתיב לקובץ, מעבדת אותו ומחזירה רשומת JSON או None במקרה של כשל.
    """
    from markitdown import MarkItDown
    
    md_converter = MarkItDown(enable_plugins=True)
    root_path = ROOT_DIRECTORY

    file_obj = pathlib.Path(file_path_str)
    temp_dir_mgr = None
    try:
        name = file_obj.name
        ext = file_obj.suffix.lower()
        
        # --- עדכון: לוגיקת התעלמות משופרת ---
        if not file_obj.is_file() or any(name.startswith(p) for p in PATTERNS_TO_DELETE) or ext in IGNORED_EXTENSIONS:
            return None

        path_to_process = file_obj
        if ext == '.doc':
            temp_dir_mgr = tempfile.TemporaryDirectory()
            temp_dir_path = pathlib.Path(temp_dir_mgr.name)
            user_profile_path_uri = (temp_dir_path / "profile").as_uri()
            cmd = [
                SOFFICE_PATH,
                f"-env:UserInstallation={user_profile_path_uri}",
                '--headless',
                '--convert-to', 'docx',
                '--outdir', str(temp_dir_path),
                str(file_obj)
            ]
            # --- עדכון: הגדלת הזמן הקצוב להמרה ---
            # הזמן הוגדל ל-480 שניות (8 דקות) כדי לתת ל-LibreOffice מספיק זמן
            # להמיר קבצים גדולים או מורכבים במיוחד ולמנוע כישלונות מיותרים.
            subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=480)
            converted_path = temp_dir_path / (file_obj.stem + '.docx')
            if not converted_path.exists():
                raise FileNotFoundError(f"LibreOffice conversion failed for {file_obj.name}")
            path_to_process = converted_path

        # --- עדכון: טיפול בשגיאת רקורסיה בקבצי PDF ---
        try:
            result = md_converter.convert(str(path_to_process))
        except RecursionError:
            tqdm.write(f"Skipping '{file_obj.name}' due to a RecursionError during PDF processing. The file structure may be too complex.")
            return None
            
        text = result.text_content or ''
        if not text.strip():
            return None

        rel_path_posix = file_obj.relative_to(root_path).as_posix()
        final_metadata = {
            "title": file_obj.stem,
            "categories": list(file_obj.relative_to(root_path).parts[:-1])
        }

        explicit_meta = extract_file_metadata(path_to_process)
        if explicit_meta:
            final_metadata.update(explicit_meta)

        processed_text, _was_reversed = process_text_field(text.strip())
        if processed_text is None:
            return None

        return anonymize_record({
            "text": processed_text.strip(),
            "source": rel_path_posix,
            "metadata": final_metadata
        })
    
    except subprocess.CalledProcessError as e:
        tqdm.write(f"LibreOffice conversion error for '{file_obj.name}':\n"
                   f"STDOUT: {e.stdout.strip()}\n"
                   f"STDERR: {e.stderr.strip()}")
        return None
    except Exception as e:
        tqdm.write(f"Error processing '{file_obj.name}': {e}")
        return None
    finally:
        if temp_dir_mgr:
            temp_dir_mgr.cleanup()


def main():
    """הפונקציה הראשית שמנהלת את תהליך העיבוד המקבילי"""
    
    root_path = pathlib.Path(ROOT_DIRECTORY)
    if not root_path.exists():
        print(f"Fatal: Root directory does not exist: {ROOT_DIRECTORY}")
        return
        
    # ודא שתיקיית הפלט קיימת
    OUTPUT_DIR.mkdir(exist_ok=True)

    print("--- Loading progress manifest ---")
    last_part, already_processed = load_progress_manifest(MANIFEST_PATH)
    print(f"Found {len(already_processed)} previously processed source files.")
    if last_part > 0:
        print(f"Resuming after part number: {last_part}")

    print("\n--- Scanning source directory for new files ---")
    all_files_paths = [f for f in root_path.rglob('*') if f.is_file()]
    files_to_process = [
        str(f) for f in all_files_paths
        if f.relative_to(root_path).as_posix() not in already_processed
    ]

    if not files_to_process:
        print("\nNo new files to process. All up to date.")
    else:
        print(f"Found {len(files_to_process)} new files to process.")

        processed_count, error_count = 0, 0
        
        # --- עדכון: התאמת מספר התהליכים למפרט המחשב ---
        # למחשב עם 4 ליבות, שימוש ב-2 תהליכים במקביל הוא איזון טוב
        # שמונע עומס יתר על הדיסק הקשיח ומשאיר משאבים למערכת ההפעלה.
        num_workers = 2
        
        print(f"\n--- Starting parallel processing with {num_workers} workers ---")

        current_part_num = last_part
        records_in_current_chunk = []
        
        try:
            with multiprocessing.Pool(processes=num_workers) as pool:
                with tqdm(total=len(files_to_process), desc="Processing files") as progress_bar:
                    for result in pool.imap_unordered(process_single_file, files_to_process):
                        if result:
                            records_in_current_chunk.append(result)
                            if len(records_in_current_chunk) >= CHUNK_SIZE:
                                current_part_num += 1
                                part_path = write_parquet_part(records_in_current_chunk, OUTPUT_DIR, OUTPUT_BASENAME, current_part_num)
                                tqdm.write(f"Wrote Parquet part: {part_path}")
                                already_processed.update(record["source"] for record in records_in_current_chunk)
                                save_progress_manifest(MANIFEST_PATH, current_part_num, already_processed)
                                processed_count += len(records_in_current_chunk)
                                records_in_current_chunk = []
                        else:
                            error_count += 1
                        progress_bar.update(1)
        finally:
            if records_in_current_chunk:
                current_part_num += 1
                part_path = write_parquet_part(records_in_current_chunk, OUTPUT_DIR, OUTPUT_BASENAME, current_part_num)
                tqdm.write(f"Wrote Parquet part: {part_path}")
                already_processed.update(record["source"] for record in records_in_current_chunk)
                save_progress_manifest(MANIFEST_PATH, current_part_num, already_processed)
                processed_count += len(records_in_current_chunk)

        print(f"\n--- Summary: {processed_count} new files processed, {error_count} errors ---")
    
    print(f"\n--- Verifying entire Parquet dataset from '{OUTPUT_DIR}' ---")
    try:
        parquet_files = sorted(OUTPUT_DIR.glob(f"{OUTPUT_BASENAME}-part-*.parquet"))

        if parquet_files:
            total_records = 0
            for parquet_file in parquet_files:
                total_records += len(pd.read_parquet(parquet_file))
            print(f"Verification successful. Parquet files: {len(parquet_files)}")
            print(f"Total records: {total_records}")
        else:
            print("No output files found to verify.")
    except Exception as e:
        print(f"Dataset load verification failed: {e}")

    print("\n--- Done ---")


if __name__ == '__main__':
    # הגדרת שיטת ה-start של multiprocessing, חשוב לחלונות
    if os.name == 'nt':
        multiprocessing.set_start_method('spawn', force=True)
    main()
