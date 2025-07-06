import os
import re
import json
import glob
import pathlib
import logging
import subprocess
import tempfile
from tqdm import tqdm
from datasets import load_dataset
import multiprocessing

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

# --- שינוי: הגדרות פלט חדשות לפיצול קבצים ---
OUTPUT_DIR = SCRIPT_DIR / "output_dataset"
OUTPUT_BASENAME = "otzar_hatorah_dataset"
CHUNK_SIZE = 500  # מספר הרשומות בכל קובץ פלט

# --- עדכון: נוספו תבניות להתעלמות ---
PATTERNS_TO_DELETE = ["~$", "desktop.ini", "~"] # מתעלם מקבצי וורד זמניים וקבצים שמתחילים בטילדה
IGNORED_EXTENSIONS = [".rar", ".zip", ".xps", ".ini", "", ".tmp"]


def find_last_part_and_processed_files(output_dir: pathlib.Path, basename: str) -> (int, set):
    """
    סורק את תיקיית הפלט, מוצא את מספר החלק (part) האחרון שנוצר,
    ואוסף סט של כל קבצי המקור שכבר עובדו בכל החלקים.
    """
    processed_sources = set()
    last_part_num = 0
    
    # תבנית לחיפוש קבצי פלט, למשל '.../dataset-part-00001.jsonl'
    glob_pattern = str(output_dir / f"{basename}-part-*.jsonl")
    
    # תבנית רגולרית לחילוץ המספר מהשם
    part_num_re = re.compile(rf"{re.escape(basename)}-part-(\d+)\.jsonl")

    output_files = sorted(glob.glob(glob_pattern))

    if not output_files:
        return 0, set()

    print(f"Found {len(output_files)} existing output parts. Scanning for processed files...")
    
    for filepath in tqdm(output_files, desc="Scanning existing parts"):
        # מציאת מספר החלק הגבוה ביותר
        match = part_num_re.search(os.path.basename(filepath))
        if match:
            part_num = int(match.group(1))
            if part_num > last_part_num:
                last_part_num = part_num

        # איסוף קבצים שכבר עובדו
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    if 'source' in record:
                        processed_sources.add(record['source'])
                except json.JSONDecodeError:
                    continue
                    
    return last_part_num, processed_sources


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
            subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=120)
            converted_path = temp_dir_path / (file_obj.stem + '.docx')
            if not converted_path.exists():
                raise FileNotFoundError(f"LibreOffice conversion failed for {file_obj.name}")
            path_to_process = converted_path

        result = md_converter.convert(str(path_to_process))
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

        return {
            "text": text.strip(),
            "source": rel_path_posix,
            "metadata": final_metadata
        }
    
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

    print("--- Scanning for existing files and progress ---")
    last_part, already_processed = find_last_part_and_processed_files(OUTPUT_DIR, OUTPUT_BASENAME)
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
        num_workers = max(1, multiprocessing.cpu_count() - 1)
        print(f"\n--- Starting parallel processing with {num_workers} workers ---")

        current_part_num = last_part
        count_in_current_chunk = 0
        output_file_handle = None
        
        try:
            with multiprocessing.Pool(processes=num_workers) as pool:
                with tqdm(total=len(files_to_process), desc="Processing files") as progress_bar:
                    for result in pool.imap_unordered(process_single_file, files_to_process):
                        if result:
                            # --- לוגיקת פיצול הקבצים ---
                            if output_file_handle is None or count_in_current_chunk >= CHUNK_SIZE:
                                if output_file_handle:
                                    output_file_handle.close()
                                
                                current_part_num += 1
                                count_in_current_chunk = 0
                                # הפורמט :05d מבטיח מספרים כמו 00001, 00002...
                                part_filename = OUTPUT_DIR / f"{OUTPUT_BASENAME}-part-{current_part_num:05d}.jsonl"
                                tqdm.write(f"Creating new output file: {part_filename}")
                                output_file_handle = open(part_filename, 'w', encoding='utf-8')
                            
                            output_file_handle.write(json.dumps(result, ensure_ascii=False) + '\n')
                            count_in_current_chunk += 1
                            processed_count += 1
                        else:
                            error_count += 1
                        progress_bar.update(1)
        finally:
            if output_file_handle:
                output_file_handle.close() # חשוב לסגור את הקובץ האחרון

        print(f"\n--- Summary: {processed_count} new files processed, {error_count} errors ---")
    
    print(f"\n--- Verifying entire dataset from '{OUTPUT_DIR}' ---")
    try:
        # --- עדכון: טעינה באמצעות תבנית גלוב, כפי ש-HF עושה ---
        data_files_pattern = str(OUTPUT_DIR / f"{OUTPUT_BASENAME}-part-*.jsonl")
        
        if glob.glob(data_files_pattern):
            ds = load_dataset('json', data_files=data_files_pattern)
            print("Verification successful. Dataset info:")
            print(ds)
            
            total_records = sum(len(split) for split in ds.values())
            if total_records > 0:
                print("\nExample of last record in the 'train' split:")
                print(ds['train'][-1])
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