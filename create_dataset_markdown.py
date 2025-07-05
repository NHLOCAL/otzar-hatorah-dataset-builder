import os
import json
import pathlib
import logging
import subprocess
import tempfile
from tqdm import tqdm
from datasets import load_dataset
import multiprocessing

# --- תלות חדשה ---
try:
    import docx
    from pypdf import PdfReader
except ImportError:
    print("Error: Missing required libraries. Please run: pip install python-docx pypdf")
    exit()

# הגדרת לוגינג בסיסי להסתרת פלט עודף
logging.basicConfig(level=logging.ERROR)

# --- קבועים: יש לעדכן את הנתיבים לסביבה שלך ---
SCRIPT_DIR = pathlib.Path(__file__).parent
ROOT_DIRECTORY = SCRIPT_DIR / "Otzar_Hatorah_Books"  # דוגמה לנתיב יחסי
SOFFICE_PATH = r"C:\Program Files\LibreOffice\program\soffice" # יש לוודא שנתיב זה נכון

OUTPUT_FILE = "otzar_hatorah_dataset.jsonl"
PATTERNS_TO_DELETE = ["~$", "desktop.ini"]
IGNORED_EXTENSIONS = [".rar", ".zip", ".xps", ".ini", ""]


def get_processed_files(output_path: str) -> set:
    """קוראת את קובץ הפלט ויוצרת סט של קבצים שכבר עובדו."""
    processed = set()
    if not os.path.exists(output_path):
        return processed
    with open(output_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
                if 'source' in record:
                    processed.add(record['source'])
            except json.JSONDecodeError:
                continue
    return processed


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
        # שגיאות כאן אינן קריטיות, נתעלם בשקט
        pass
    return {k: v for k, v in metadata.items() if v}


def process_single_file(file_path_str: str) -> dict or None:
    """
    *** פונקציית העובד המרכזית ***
    מקבלת נתיב לקובץ, מעבדת אותו ומחזירה רשומת JSON או None במקרה של כשל.
    """
    # אתחול הממיר בתוך התהליך כדי למנוע בעיות של שיתוף אובייקטים
    md_converter = MarkItDown(enable_plugins=True)
    root_path = ROOT_DIRECTORY # משתמשים בקבוע הגלובלי

    file_obj = pathlib.Path(file_path_str)
    temp_dir_mgr = None
    try:
        # סינון בסיסי
        name = file_obj.name
        ext = file_obj.suffix.lower()
        if not file_obj.is_file() or any(name.startswith(p) for p in PATTERNS_TO_DELETE) or ext in IGNORED_EXTENSIONS:
            return None

        path_to_process = file_obj
        if ext == '.doc':
            # כל תהליך מקבל ספרייה זמנית משלו
            temp_dir_mgr = tempfile.TemporaryDirectory()
            temp_dir_path = pathlib.Path(temp_dir_mgr.name)

            # יצירת נתיב פרופיל ייחודי ל-LibreOffice כדי למנוע התנגשויות
            user_profile_path_uri = (temp_dir_path / "profile").as_uri()
            cmd = [
                SOFFICE_PATH,
                f"-env:UserInstallation={user_profile_path_uri}", # <-- הפתרון לבעיית המקביליות
                '--headless',
                '--convert-to', 'docx',
                '--outdir', str(temp_dir_path),
                str(file_obj)
            ]
            
            # הגדלת timeout והרצת הפקודה
            subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=120)
            
            converted_path = temp_dir_path / (file_obj.stem + '.docx')
            if not converted_path.exists():
                raise FileNotFoundError(f"LibreOffice conversion failed for {file_obj.name}")
            path_to_process = converted_path

        result = md_converter.convert(str(path_to_process))
        text = result.text_content or ''
        if not text.strip():
            return None # קובץ ריק

        rel_path_posix = file_obj.relative_to(root_path).as_posix()
        final_metadata = {
            "title": file_obj.stem,
            "categories": list(file_obj.relative_to(root_path).parts[:-1])
        }

        explicit_meta = extract_file_metadata(path_to_process)
        if explicit_meta:
            final_metadata.update(explicit_meta)

        return {
            "text": text.strip(),
            "source": rel_path_posix,
            "metadata": final_metadata
        }
    
    except subprocess.CalledProcessError as e:
        # טיפול שגיאות מפורט: מדפיס את הפלט המדויק מ-LibreOffice
        tqdm.write(f"LibreOffice conversion error for '{file_obj.name}':\n"
                   f"STDOUT: {e.stdout.strip()}\n"
                   f"STDERR: {e.stderr.strip()}")
        return None
    except Exception as e:
        # טיפול בשגיאות אחרות
        tqdm.write(f"Error processing '{file_obj.name}': {e}")
        return None
    finally:
        # ניקוי הספרייה הזמנית
        if temp_dir_mgr:
            temp_dir_mgr.cleanup()


def main():
    """הפונקציה הראשית שמנהלת את תהליך העיבוד המקבילי"""
    
    root_path = pathlib.Path(ROOT_DIRECTORY)
    if not root_path.exists():
        print(f"Fatal: Root directory does not exist: {ROOT_DIRECTORY}")
        return

    # 1. הכנת רשימת העבודה
    print("Scanning for files to process...")
    already_processed = get_processed_files(OUTPUT_FILE)
    
    # יצירת רשימת הקבצים שיש לעבד
    all_files_paths = [f for f in root_path.rglob('*') if f.is_file()]
    files_to_process = [
        str(f) for f in all_files_paths
        if f.relative_to(root_path).as_posix() not in already_processed
    ]

    if not files_to_process:
        print("No new files to process. All up to date.")
    else:
        print(f"Found {len(already_processed)} processed files. Resuming with {len(files_to_process)} new files.")

        # 2. עיבוד מקבילי
        processed_count, error_count = 0, 0
        # השתמש ברוב הליבות, אך השאר אחת פנויה למערכת ההפעלה
        num_workers = max(1, multiprocessing.cpu_count() - 1)
        print(f"\n--- Starting parallel processing with {num_workers} workers ---")

        with open(OUTPUT_FILE, 'a', encoding='utf-8') as out_f:
            with multiprocessing.Pool(processes=num_workers) as pool:
                # שימוש ב-imap_unordered לקבלת תוצאות ברגע שהן מוכנות
                with tqdm(total=len(files_to_process), desc="Processing files") as progress_bar:
                    for result in pool.imap_unordered(process_single_file, files_to_process):
                        if result:
                            out_f.write(json.dumps(result, ensure_ascii=False) + '\n')
                            processed_count += 1
                        else:
                            error_count += 1
                        progress_bar.update(1)

        print(f"\n--- Summary: {processed_count} new files processed, {error_count} errors ---")
    
    # 3. אימות הדאטהסט
    print(f"\n--- Verifying: {OUTPUT_FILE} ---")
    try:
        if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
            ds = load_dataset('json', data_files=OUTPUT_FILE)
            print("Verification successful. Dataset info:")
            print(ds)
            if len(ds['train']) > 0:
                print("\nExample of last processed record:")
                print(ds['train'][-1])
        else:
            print("No output generated or file is empty.")
    except Exception as e:
        print(f"Dataset load failed: {e}")

    print("\n--- Done ---")


if __name__ == '__main__':
    # הכרחי להריץ את הקוד הראשי בתוך בלוק זה כשמשתמשים ב-multiprocessing
    main()