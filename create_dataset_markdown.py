import os
import json
import pathlib
import logging
import subprocess
import tempfile
from tqdm import tqdm
from markitdown import MarkItDown
from datasets import load_dataset

# --- NEW DEPENDENCIES ---
# Make sure to install the required libraries for metadata extraction:
# pip install python-docx PyPDF2
try:
    import docx
    from PyPDF2 import PdfReader
except ImportError:
    print("Error: Missing required libraries. Please run: pip install python-docx PyPDF2")
    exit()

# Configure basic logging to suppress excessive output
logging.basicConfig(level=logging.ERROR)

# --- Constants: update these paths for your environment ---
ROOT_DIRECTORY = r"C:\Users\me\Documents\אוצר התורה\גמח אוצר התורה החדש טבת תשפה"
SOFFICE_PATH = r"C:\Program Files\LibreOffice\program\soffice"

OUTPUT_FILE = "otzar_hatorah_dataset.jsonl"
PATTERNS_TO_DELETE = ["~$", "desktop.ini"]
IGNORED_EXTENSIONS = [".rar", ".zip", ".xps", ".ini", ""]

# Initialize MarkItDown converter. Plugins are still enabled for fallback.
md_converter = MarkItDown(enable_plugins=True)


def extract_file_metadata(file_path: pathlib.Path) -> dict:
    """
    Extracts metadata from supported file types (.docx, .pdf).
    Args:
        file_path: The path to the file (as a pathlib.Path object).
    Returns:
        A dictionary containing the extracted metadata.
    """
    metadata = {}
    ext = file_path.suffix.lower()
    try:
        if ext == '.docx':
            doc = docx.Document(file_path)
            props = doc.core_properties
            # Extract all available core properties
            metadata = {
                'author': props.author,
                'created': props.created.isoformat() if props.created else None,
                'modified': props.modified.isoformat() if props.modified else None,
                'last_modified_by': props.last_modified_by,
                'subject': props.subject,
                'title_meta': props.title,  # Use a different key to avoid conflict
                'version': props.version
            }
        elif ext == '.pdf':
            with open(file_path, 'rb') as f:
                reader = PdfReader(f)
                info = reader.metadata
                if info:
                    # Extract common PDF metadata fields
                    metadata = {
                        'author': info.author,
                        'creator': info.creator,
                        'producer': info.producer,
                        'subject': info.subject,
                        'title_meta': info.title
                    }
    except Exception as e:
        # Silently fail if metadata extraction doesn't work for a file
        tqdm.write(f"Could not extract metadata from {file_path.name}: {e}")
        pass
    # Filter out any keys that have None or empty string values
    return {k: v for k, v in metadata.items() if v}


def clean_target_directory(root_dir: str):
    """
    (Optional) Remove junk files before processing.
    """
    print("\n--- Starting Pre-cleanup Phase ---")
    deleted_count = 0
    try:
        root_path = pathlib.Path(root_dir)
        if not root_path.exists():
            print(f"Warning: Directory not found: {root_dir}")
            return
        for file_path in tqdm(root_path.rglob('*.*'), desc="Scanning for junk files"):
            name = file_path.name
            if any(name.startswith(p) for p in PATTERNS_TO_DELETE):
                try:
                    os.remove(file_path)
                    tqdm.write(f"Deleted: {file_path}")
                    deleted_count += 1
                except OSError as e:
                    tqdm.write(f"Error deleting {file_path}: {e}")
    except Exception as e:
        print(f"Cleanup error: {e}")
    print(f"Cleanup finished. Deleted {deleted_count} junk files.")


def process_and_stream_to_jsonl(root_dir: str, output_path: str):
    """
    Walk through files, convert to Markdown, extract metadata, and stream to JSONL.
    """
    processed, errors = 0, 0
    root_path = pathlib.Path(root_dir)
    if not root_path.exists():
        print(f"Fatal: Root directory does not exist: {root_dir}")
        return processed, errors

    all_files = list(root_path.rglob('*'))
    print(f"\n--- Processing {len(all_files)} files ---")

    with open(output_path, 'w', encoding='utf-8') as out_f:
        for file_obj in tqdm(all_files, desc="Processing files"):
            temp_dir_mgr = None
            try:
                name = file_obj.name
                ext = file_obj.suffix.lower()
                if any(name.startswith(p) for p in PATTERNS_TO_DELETE) or ext in IGNORED_EXTENSIONS:
                    continue

                path_to_process = file_obj
                if ext == '.doc':
                    temp_dir_mgr = tempfile.TemporaryDirectory()
                    cmd = [SOFFICE_PATH, '--headless', '--convert-to', 'docx', '--outdir', temp_dir_mgr.name, str(file_obj)]
                    subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=60)
                    converted_path = pathlib.Path(temp_dir_mgr.name) / (file_obj.stem + '.docx')
                    if not converted_path.exists():
                        raise FileNotFoundError(f"Conversion failed for {file_obj.name}")
                    path_to_process = converted_path

                # Convert main content to Markdown text
                result = md_converter.convert(str(path_to_process))
                text = result.text_content or ''
                if not text.strip():
                    continue

                # --- METADATA GATHERING ---
                # 1. Start with metadata from file path (title from filename, categories from folders)
                rel_path = file_obj.relative_to(root_path)
                final_metadata = {
                    "title": file_obj.stem,
                    "categories": list(rel_path.parts[:-1])
                }

                # 2. Add metadata extracted directly from the file content (docx, pdf, etc.)
                # This is now the primary source for internal metadata.
                explicit_meta = extract_file_metadata(path_to_process)
                if explicit_meta:
                    final_metadata.update(explicit_meta)
                
                # --- RECORD ASSEMBLY ---
                record = {
                    "text": text.strip(),
                    "source": str(rel_path.as_posix()),  # Use POSIX path for consistency
                    "metadata": final_metadata
                }

                out_f.write(json.dumps(record, ensure_ascii=False) + '\n')
                processed += 1

            except subprocess.CalledProcessError as e:
                errors += 1
                tqdm.write(f"LibreOffice conversion error for {file_obj.name}: {e.stderr}")
            except subprocess.TimeoutExpired:
                errors += 1
                tqdm.write(f"Timeout converting {file_obj.name}")
            except Exception as e:
                errors += 1
                tqdm.write(f"Error processing {file_obj.name}: {e}")
            finally:
                if temp_dir_mgr:
                    temp_dir_mgr.cleanup()

    print(f"\n--- Summary: {processed} processed, {errors} errors ---")
    return processed, errors


if __name__ == '__main__':
    # 1) Pre-cleanup
    # clean_target_directory(ROOT_DIRECTORY)
    # 2) Conversion & streaming
    processed_count, error_count = process_and_stream_to_jsonl(ROOT_DIRECTORY, OUTPUT_FILE)
    # 3) Verify dataset
    print(f"\n--- Verifying: {OUTPUT_FILE} ---")
    try:
        if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
            ds = load_dataset('json', data_files=OUTPUT_FILE)
            print("Verification successful. Dataset info:")
            print(ds)
            # Print the first record to show an example with metadata
            if len(ds['train']) > 0:
                print("\nExample of first record:")
                print(ds['train'][0])
        else:
            print("No output generated or file is empty.")
    except Exception as e:
        print(f"Dataset load failed: {e}")

    print("--- Done ---")

