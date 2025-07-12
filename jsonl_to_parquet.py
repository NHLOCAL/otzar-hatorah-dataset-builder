import os
import json
import pandas as pd
import pyarrow  # Required by pandas for Parquet I/O
import re


def _pre_process_text(text: str) -> str:
    """
    Performs preliminary cleaning on a text string.

    - Removes (cid:xx) tags, which are common artifacts from PDF conversions.
    - Joins "broken" lines of single Hebrew characters into continuous words.
    """
    # Fix "vertically-spelled" text by joining Hebrew letters across newlines.
    # e.g., 'ש\nל\nו\nם' becomes 'שלום'
    processed_text = re.sub(r'(?<=[א-ת])\n(?=[א-ת])', '', text)
    return processed_text


def fix_hebrew_encoding(text: str) -> str:
    """Attempts to fix text that was decoded with the wrong encoding."""
    try:
        # This handles the common case of windows-1255 text being misread as latin-1.
        return text.encode('latin-1').decode('windows-1255')
    except (UnicodeEncodeError, UnicodeDecodeError):
        # The text was likely already in a correct format (e.g., UTF-8).
        return text


def detect_and_fix_reversed_hebrew(text: str) -> str:
    """Detects and corrects reversed (visual) Hebrew text."""
    # Split text by any non-Hebrew character to get potential words.
    words = [word for word in re.split(r'[^א-ת]+', text) if word]
    if not words:
        return text

    final_letters = 'םןץףך'
    reversed_word_count = sum(
        1 for word in words if len(word) > 1 and word.startswith(final_letters)
    )

    # A dynamic threshold helps avoid false positives on short texts.
    # It requires at least 2 reversed words, or a high ratio for longer texts.
    ratio = reversed_word_count / len(words)
    is_likely_reversed = (reversed_word_count >= 2 and ratio > 0.05) or ratio > 0.2

    return text[::-1] if is_likely_reversed else text


def process_text_field(text: str, cid_threshold: int = 10) -> str | None:
    """
    Applies a full cleaning and normalization pipeline to a text field.

    Args:
        text (str): The input text to process.
        cid_threshold (int): The maximum allowed number of '(cid:)' tags.
            If exceeded, the function returns None to signal the record
            should be dropped.

    Returns:
        A cleaned and corrected string, or None if the text is too corrupted.
    """
    if not isinstance(text, str):
        return text

    # Step 1: Drop records that are too corrupted to be useful.
    if text.count('(cid:') > cid_threshold:
        return None

    # Remove (cid:xx) tags before further processing.
    text = re.sub(r'\(cid:\d+\)', '', text)

    # Step 2: Apply sequential cleaning functions.
    processed_text = _pre_process_text(text)
    processed_text = fix_hebrew_encoding(processed_text)
    processed_text = detect_and_fix_reversed_hebrew(processed_text)

    return processed_text


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

    The process includes text cleaning, fixing encoding/reversal issues,
    anonymization, and deduplication based on the final text content.
    """
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory is ready: {output_dir}")

    all_records = []
    skipped_count = 0
    
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

                    processed_text = process_text_field(text_content)

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

    df = pd.DataFrame(all_records)

    # Deduplicate based on the cleaned 'text' field.
    if 'text' in df.columns:
        initial_count = len(df)
        df.dropna(subset=['text'], inplace=True)
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
    INPUT_DIRECTORY = "output_dataset"
    OUTPUT_DIRECTORY = "converted_parquet"
    OUTPUT_PARQUET_FILE = "all_data_combined.parquet"

    convert_jsonl_to_parquet(INPUT_DIRECTORY, OUTPUT_DIRECTORY, OUTPUT_PARQUET_FILE)

    print("\n-----------------------------------------------------")
    print("Verification:")
    
    # Attempt to read the created Parquet file for a quick check.
    try:
        output_path = os.path.join(OUTPUT_DIRECTORY, OUTPUT_PARQUET_FILE)
        df_read = pd.read_parquet(output_path)
        print(f"Successfully read the output file '{output_path}'.")
        print(f"Final record count: {len(df_read)}")
        print(f"Columns: {df_read.columns.tolist()}")
        print("\nSample of the first 5 records:")
        print(df_read.head())
    except FileNotFoundError:
        print(f"\nError: Could not find the output file at '{output_path}'.")
    except Exception as e:
        print(f"\nError reading the Parquet file: {e}")