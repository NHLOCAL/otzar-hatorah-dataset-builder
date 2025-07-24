import os
import argparse
from huggingface_hub import HfApi

def main():
    """
    Uploads an entire directory to a Hugging Face dataset repository.
    This is ideal for datasets split into multiple files (shards).
    This script is designed to be run by GitHub Actions.
    """
    parser = argparse.ArgumentParser(description="Upload a dataset directory to the Hugging Face Hub.")
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
        help="The local path to the directory to upload."
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
        raise ValueError("HUGGINGFACE_TOKEN environment variable is not set. Please set it in your GitHub repository secrets.")
        
    if not os.path.isdir(args.local_dir):
        raise ValueError(f"The provided local path '{args.local_dir}' is not a directory.")

    print(f"Authenticating with Hugging Face Hub...")
    api = HfApi(token=hf_token)

    print(f"Uploading directory '{args.local_dir}' to '{args.repo_id}' repository under '{args.path_in_repo}'...")

    # Use upload_folder to upload all contents of the directory
    api.upload_folder(
        folder_path=args.local_dir,
        path_in_repo=args.path_in_repo,
        repo_id=args.repo_id,
        repo_type="dataset",
        commit_message="Update dataset from GitHub Actions"
    )

    print("Directory uploaded successfully!")
    print(f"Check your dataset at: https://huggingface.co/datasets/{args.repo_id}/tree/main/{args.path_in_repo}")


if __name__ == "__main__":
    main()