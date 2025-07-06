import os
import argparse
import tempfile
from huggingface_hub import HfApi
from datasets import load_dataset, Features, Value, Sequence, Timestamp

def main():
    """
    Uploads an entire directory of pre-generated JSONL shards to a Hugging Face dataset repository,
    converting each shard to the explicit schema required by 🤗 Datasets before upload.
    """
    parser = argparse.ArgumentParser(
        description="Upload a dataset directory to the Hugging Face Hub with explicit schema conversion."
    )
    parser.add_argument(
        "--repo-id",
        type=str,
        required=True,
        help="The ID of the repository on the Hub (e.g., 'username/my-dataset')."
    )
    parser.add_argument(
        "--local-dir",
        type=str,
        required=True,
        help="The local path to the directory containing JSONL shards."
    )
    parser.add_argument(
        "--path-in-repo",
        type=str,
        default="data",
        help="The target directory path in the repo. Defaults to 'data'."
    )
    args = parser.parse_args()

    hf_token = os.environ.get("HUGGINGFACE_TOKEN")
    if not hf_token:
        raise ValueError(
            "HUGGINGFACE_TOKEN environment variable is not set."
        )

    if not os.path.isdir(args.local_dir):
        raise ValueError(f"The provided local path '{args.local_dir}' is not a directory.")

    # Define explicit schema for JSONL fields
    features = Features({
        "title":            Value("string"),
        "categories":       Sequence(Value("string")),
        "author":           Value("string"),
        "created":          Timestamp("s"),
        "modified":         Timestamp("s"),
        "last_modified_by": Value("string"),
        "title_meta":       Value("string"),
        "subject":          Value("string"),
        "creator":          Value("string"),
        "producer":         Value("string"),
    })

    # Prepare a temporary directory for converted shards
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Converting JSONL shards in {args.local_dir} to explicit schema...")
        shards = [f for f in os.listdir(args.local_dir) if f.lower().endswith('.jsonl')]
        for shard in shards:
            input_path = os.path.join(args.local_dir, shard)
            output_path = os.path.join(tmpdir, shard)

            # Load with explicit features
            ds = load_dataset(
                'json',
                data_files={'train': input_path},
                split='train',
                features=features,
                field='__root__',  # treat each line as one record
            )
            # Save back to JSONL with the correct schema
            ds.to_json(output_path, orient='records', lines=True)
            print(f"  Converted {shard} -> {output_path}")

        # All shards converted, now upload the converted folder
        print("Authenticating with Hugging Face Hub...")
        api = HfApi(token=hf_token)

        print(f"Uploading converted data from '{tmpdir}' to '{args.repo_id}' under '{args.path_in_repo}'...")
        api.upload_folder(
            folder_path=tmpdir,
            path_in_repo=args.path_in_repo,
            repo_id=args.repo_id,
            repo_type="dataset",
            commit_message="Update dataset with explicit schema conversion"
        )

    print("Upload completed successfully!")
    print(
        f"Check your dataset at: https://huggingface.co/datasets/{args.repo_id}/tree/main/{args.path_in_repo}"
    )

if __name__ == "__main__":
    main()
