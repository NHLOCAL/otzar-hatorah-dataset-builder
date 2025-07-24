import os
import json
import pandas as pd
import pyarrow  # Required by pandas for Parquet I/O
import re
import argparse

# --- Constants for Hebrew text processing ---
# Using frozenset for efficient membership testing ('in')
FINAL_LETTERS = frozenset('םןץףך')
NON_FINAL_EQUIVALENTS = frozenset('כמנפצ')

# Common technical English words that, when reversed, are a very strong sign of reversed text.
REVERSED_CANARY_WORDS = frozenset([':atad', 'egami', 'ptth', 'lmth', 'gnp/egami'])
# Hebrew words that, when reversed, are a knockout sign of reversed text.
HEBREW_REVERSED_CANARIES = frozenset(['אוה', 'רועיש', 'תא', 'הזש', 'רוסא', 'יבר', 'ןכא'])
# Hebrew words that, if found, prove the text is CORRECT and should NOT be reversed.
HEBREW_CORRECT_CANARIES = frozenset(['הוא', 'שיעור', 'את', 'שזה', 'אסור', 'רבי', 'אכן'])




def is_garbled_text(text: str) -> bool:
    """
    Detects severely garbled text based on corruption patterns in the entire document.
    """
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
    """
    Performs preliminary cleaning on a text string.
    - Joins "broken" lines of single Hebrew characters into continuous words.
    """
    processed_text = re.sub(r'(?<=[א-ת])\n(?=[א-ת])', '', text)
    return processed_text


def fix_hebrew_encoding(text: str) -> str:
    """Attempts to fix text that was decoded with the wrong encoding."""
    try:
        return text.encode('latin-1').decode('windows-1255')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def detect_and_fix_reversed_hebrew(text: str) -> tuple[str, bool]:
    """
    Detects and corrects reversed Hebrew using a multi-layered heuristic.

    The logic order is crucial:
    1. Anti-Reversal Check: Looks for specific correct words (e.g., "שיעור").
       If found, the text is confirmed as correct and is NOT reversed.
    2. Knockout Reversal Check: Looks for specific reversed words (e.g., "רועיש").
       If found, the text is immediately reversed.
    3. Scoring-based Heuristic: If no knockout rules apply, falls back to the
       original check for final/non-final letters.

    Returns:
        A tuple containing the processed string and a boolean indicating if a reversal occurred.
    """
    sample = text[:10000]

    # 1. Anti-Reversal Check (Highest Priority): If we find a correct canary word,
    # the text is definitely not reversed. Stop and return as is.
    for canary in HEBREW_CORRECT_CANARIES:
        # Use word boundaries (\b) for whole-word matching.
        if re.search(r'\b' + re.escape(canary) + r'\b', sample):
            return text, False

    # 2. Knockout Reversal Checks (Second Priority):
    # A) English technical terms
    for canary in REVERSED_CANARY_WORDS:
        if canary in sample:
            return text[::-1], True
    # B) Hebrew reversed words
    for canary in HEBREW_REVERSED_CANARIES:
        if re.search(r'\b' + re.escape(canary) + r'\b', sample):
            return text[::-1], True

    # 3. Fallback to Scoring-Based Heuristic (if no knockout rules applied)
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

    # Final decision based on the score.
    is_likely_reversed = reversed_evidence_score >= 3
    if is_likely_reversed:
        return text[::-1], True
    else:
        return text, False


def process_text_field(text: str, cid_threshold: int = 10) -> tuple[str | None, bool]:
    """
    Applies a full cleaning and normalization pipeline to a text field.

    Returns:
        A tuple containing the cleaned string (or None if too corrupted)
        and a boolean indicating if the text was reversed.
    """
    was_reversed = False
    if not isinstance(text, str):
        return text, was_reversed

    # Step 1: Drop records that are severely garbled and beyond repair.
    if is_garbled_text(text):
        return None, was_reversed

    # Step 2: Drop records that are too corrupted with (cid:) tags.
    if text.count('(cid:') > cid_threshold:
        return None, was_reversed

    # Remove (cid:xx) tags before further processing.
    text = re.sub(r'\(cid:\d+\)', '', text)

    # Step 3: Apply sequential cleaning functions. The order is important.
    processed_text = _pre_process_text(text)
    processed_text = fix_hebrew_encoding(processed_text)
    processed_text, was_reversed = detect_and_fix_reversed_hebrew(processed_text)

    return processed_text, was_reversed


def anonymize_record(record: dict) -> dict:
    """Anonymizes a record by replacing emails and phone numbers."""
    EMAIL_REGEX = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    PHONE_REGEX = r'\b(?:(?:\+972-?)|0)(?:[23489]|5[0-9]|7[0-9])-?\d{7}\b'
    
    anonymized_record = {}
    for key, value in record.items():
        if isinstance(value, str):
            sanitized_value = re.sub(EMAIL_REGEX, "[EMAIL_REMOVED]", value)
            sanitized_value = re.sub(PHONE_REGEX, "[PHONE_REMOVED]", sanitized_value)
            anonymized_record[key] = sanitized_value
        else:
            anonymized_record[key] = value
    return anonymized_record


def convert_jsonl_to_parquet(input_dir, output_dir, output_filename):
    """
    Converts JSONL files to a single, cleaned Parquet file.
    """
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory is ready: {output_dir}")

    all_records = []
    skipped_count = 0
    reversed_count = 0  # Initialize counter for reversed texts
    
    jsonl_files = [f for f in os.listdir(input_dir) if f.endswith(".jsonl")]

    if not jsonl_files:
        print(f"No .jsonl files found in '{input_dir}'. Aborting.")
        return

    for filename in jsonl_files:
        filepath = os.path.join(input_dir, filename)
        print(f"Processing file: {filepath}")

        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    record = json.loads(line)
                    text_content = record.get('text')

                    processed_text, was_reversed = process_text_field(text_content)

                    if was_reversed:
                        reversed_count += 1

                    if processed_text is None:
                        skipped_count += 1
                        continue

                    record['text'] = processed_text
                    anonymized_record = anonymize_record(record)
                    all_records.append(anonymized_record)

                except json.JSONDecodeError:
                    print(f"  Warning: Skipping malformed JSON on line {line_num}.")
                except Exception as e:
                    print(f"  Warning: An unexpected error occurred on line {line_num}: {e}")

    if not all_records:
        print(f"No valid records were collected from the input files. No Parquet file will be created.")
        return
    
    print(f"\nCollected {len(all_records)} valid records.")
    if skipped_count > 0:
        print(f"Skipped {skipped_count} records due to excessive corruption.")
    if reversed_count > 0:
        print(f"Corrected and reversed {reversed_count} text records.")

    df = pd.DataFrame(all_records)

    # Deduplicate based on the cleaned 'text' field.
    if 'text' in df.columns:
        initial_count = len(df)
        df.dropna(subset=['text'], inplace=True)
        df = df[df['text'].str.strip() != '']
        if initial_count > 1:
            df.drop_duplicates(subset=['text'], keep='first', inplace=True)
            final_count = len(df)
            print(f"Removed {initial_count - final_count} duplicate records based on text content.")
            print(f"Final dataset contains {final_count} unique records.")
    
    output_parquet_path = os.path.join(output_dir, output_filename)
    df.to_parquet(output_parquet_path, index=False)
    
    print(f"\nProcess completed successfully!")
    print(f"Cleaned Parquet file saved to: {output_parquet_path}")


if __name__ == "__main__":
    # הגדרת היכולת לקבל פרמטרים חיצוניים עם ערכי ברירת מחדל
    parser = argparse.ArgumentParser(
        description="""Converts JSONL files to a single, cleaned Parquet file.
                       Runs with default paths if no arguments are provided."""
    )
    
    # הגדרת פרמטרים אופציונליים. אם לא יסופקו, ישומשו ערכי ה-default.
    parser.add_argument(
        "--input-dir", 
        default="output_dataset",  # ברירת המחדל המקורית
        help="Path to the directory containing the source JSONL files. Defaults to 'output_dataset'."
    )
    parser.add_argument(
        "--output-dir", 
        default="converted_parquet", # ברירת המחדל המקורית
        help="Path to the directory where the output Parquet file will be saved. Defaults to 'converted_parquet'."
    )
    parser.add_argument(
        "--output-file", 
        default="all_data_combined.parquet", 
        help="The name of the final Parquet file. Defaults to 'all_data_combined.parquet'."
    )
    
    # קריאת הפרמטרים מהפקודה. אם אין, ישומשו ערכי ברירת המחדל.
    args = parser.parse_args()

    print(f"Using Input Directory: '{args.input_dir}'")
    print(f"Using Output Directory: '{args.output_dir}'")
    
    # קריאה לפונקציה הראשית עם הפרמטרים (בין אם הגיעו מהפקודה או מברירת המחדל)
    convert_jsonl_to_parquet(args.input_dir, args.output_dir, args.output_file)

    print("\n-----------------------------------------------------")
    print("Verification:")
    
    # ולידציה של קובץ הפלט
    try:
        output_path = os.path.join(args.output_dir, args.output_file)
        if not os.path.exists(output_path):
             raise FileNotFoundError(f"Output file was not created at '{output_path}'")
        
        df_read = pd.read_parquet(output_path)
        print(f"Successfully read the output file '{output_path}'.")
        print(f"Final record count: {len(df_read)}")
        print(f"Columns: {df_read.columns.tolist()}")
        if not df_read.empty:
            print("\nSample of the first 5 records:")
            print(df_read.head())
    except FileNotFoundError as e:
        print(f"\nVerification Error: {e}")
    except Exception as e:
        print(f"\nVerification Error: Could not read the Parquet file: {e}")