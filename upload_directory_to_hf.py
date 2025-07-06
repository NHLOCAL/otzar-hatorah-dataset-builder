import os
import argparse
import pathlib
import json
from datetime import datetime
from datasets import load_dataset, Features, Value, Sequence, Timestamp
from huggingface_hub import HfApi

def extract_file_metadata(file_path: pathlib.Path, root_dir: pathlib.Path) -> dict:
    metadata = {"title": file_path.stem}
    rel_parts = file_path.relative_to(root_dir).parent.parts
    metadata['categories'] = list(rel_parts) if rel_parts else []
    ext = file_path.suffix.lower()
    try:
        if ext == '.docx':
            import docx
            props = docx.Document(file_path).core_properties
            if props.author: metadata['author'] = props.author
            if props.created: metadata['created'] = props.created
            if props.modified: metadata['modified'] = props.modified
            if props.last_modified_by: metadata['last_modified_by'] = props.last_modified_by
            if props.subject: metadata['subject'] = props.subject
            if props.title: metadata.setdefault('title_meta', props.title)
        elif ext == '.pdf':
            from PyPDF2 import PdfReader
            info = PdfReader(file_path).metadata
            if info.author: metadata['author'] = info.author
            if info.creator: metadata['creator'] = info.creator
            if info.producer: metadata['producer'] = info.producer
            if info.subject: metadata['subject'] = info.subject
            if info.title: metadata.setdefault('title_meta', info.title)
        stat = file_path.stat()
        metadata.setdefault('created', datetime.fromtimestamp(stat.st_ctime))
        metadata.setdefault('modified', datetime.fromtimestamp(stat.st_mtime))
    except Exception:
        pass
    return {k: v for k, v in metadata.items() if v is not None}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-id", required=True)
    parser.add_argument("--local-dir", required=True)
    args = parser.parse_args()
    hf_token = os.environ.get("HUGGINGFACE_TOKEN")
    root = pathlib.Path(args.local_dir)
    # 1. Generate metadata.jsonl
    lines = []
    for f in root.rglob('*'):
        if f.is_file() and f.suffix.lower() in ('.docx', '.pdf'):
            meta = extract_file_metadata(f, root)
            lines.append(meta)
    meta_file = root / 'metadata.jsonl'
    with open(meta_file, 'w') as fout:
        for m in lines:
            fout.write(json.dumps(m, default=lambda o: o.isoformat()) + '\n')
    # 2. Load with explicit features and push to hub
    features = Features({
        'title': Value('string'),
        'categories': Sequence(Value('string')),
        'author': Value('string'),
        'created': Timestamp('s'),
        'modified': Timestamp('s'),
        'last_modified_by': Value('string'),
        'title_meta': Value('string'),
        'subject': Value('string'),
        'creator': Value('string'),
        'producer': Value('string'),
    })
    ds = load_dataset(
        'json', data_files=str(meta_file), split='train', features=features
    )
    ds.push_to_hub(args.repo_id, token=hf_token)

if __name__ == '__main__':
    main()