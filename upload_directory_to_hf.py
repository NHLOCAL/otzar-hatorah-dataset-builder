import os
import argparse
import pathlib
import json
from datetime import datetime
from huggingface_hub import HfApi

def extract_file_metadata(file_path: pathlib.Path, root_dir: pathlib.Path) -> dict:
    """מחלצת מטא-דאטה מקבצים נתמכים ויוצרת שדות בפורמט המתאים עבור HF Datasets."""
    metadata = {}
    # בסיסי: שמות ותוויות
    metadata['title'] = file_path.stem
    # קטגוריות: שמות תיקיות יחסים ביחס לשורש
    rel_parts = file_path.relative_to(root_dir).parent.parts
    metadata['categories'] = list(rel_parts) if rel_parts else []

    ext = file_path.suffix.lower()
    try:
        if ext == '.docx':
            import docx
            doc = docx.Document(file_path)
            props = doc.core_properties
            # use datetime objects כדי Datasets יזהה timestamps
            if props.created:
                metadata['created'] = props.created
            if props.modified:
                metadata['modified'] = props.modified
            if props.author:
                metadata['author'] = props.author
            if props.last_modified_by:
                metadata['last_modified_by'] = props.last_modified_by
            if props.subject:
                metadata['subject'] = props.subject
            if props.title:
                # כבר יש title, אבל בסיס fallback
                metadata.setdefault('title_meta', props.title)
        elif ext == '.pdf':
            from PyPDF2 import PdfReader
            with open(file_path, 'rb') as f:
                reader = PdfReader(f)
                info = reader.metadata
                if info:
                    if info.author:
                        metadata['author'] = info.author
                    if info.creator:
                        metadata['creator'] = info.creator
                    if info.producer:
                        metadata['producer'] = info.producer
                    if info.subject:
                        metadata['subject'] = info.subject
                    if info.title:
                        metadata.setdefault('title_meta', info.title)
        # אם אין תאריכים במטא של הקובץ, משתמשים במערכת הקבצים
        stat = file_path.stat()
        if 'created' not in metadata:
            metadata['created'] = datetime.fromtimestamp(stat.st_ctime)
        if 'modified' not in metadata:
            metadata['modified'] = datetime.fromtimestamp(stat.st_mtime)
    except Exception:
        # במקרי קצה נתעלם משגיאות במטא
        pass

    # מסניף ערכים ריקים
    return {k: v for k, v in metadata.items() if v is not None}


def main():
    parser = argparse.ArgumentParser(
        description="Upload a dataset directory to the Hugging Face Hub with JSONL metadata."
    )
    parser.add_argument(
        "--repo-id", type=str, required=True,
        help="The ID of the repository on the Hub (e.g., 'username/my-dataset')."
    )
    parser.add_argument(
        "--local-dir", type=str, required=True,
        help="The local path to the directory to upload."
    )
    parser.add_argument(
        "--path-in-repo", type=str, default="data",
        help="The target directory path in the repo. Defaults to 'data'."
    )

    args = parser.parse_args()
    hf_token = os.environ.get("HUGGINGFACE_TOKEN")
    if not hf_token:
        raise ValueError(
            "HUGGINGFACE_TOKEN environment variable is not set."
        )

    root_dir = pathlib.Path(args.local_dir)
    if not root_dir.is_dir():
        raise ValueError(f"'{args.local_dir}' is not a directory.")

    # יוצרים קובץ JSONL של מטא-דאטה
    meta_path = root_dir / "metadata.jsonl"
    with open(meta_path, 'w', encoding='utf-8') as fout:
        for file_path in root_dir.rglob('*'):
            if file_path.is_file() and file_path.suffix.lower() in {'.docx', '.pdf'}:
                meta = extract_file_metadata(file_path, root_dir)
                fout.write(json.dumps(meta, default=lambda o: o.isoformat()) + '\n')

    # מעלה הכול ל-HF
    print(f"Authenticating with Hugging Face Hub...")
    api = HfApi(token=hf_token)
    print(f"Uploading directory '{args.local_dir}' to '{args.repo_id}' under '{args.path_in_repo}'...")
    api.upload_folder(
        folder_path=str(root_dir),
        path_in_repo=args.path_in_repo,
        repo_id=args.repo_id,
        repo_type="dataset",
        commit_message="Update dataset with converted metadata"
    )

    print("Upload complete.")
    print(f"View your dataset: https://huggingface.co/datasets/{args.repo_id}/tree/main/{args.path_in_repo}")

if __name__ == "__main__":
    main()
