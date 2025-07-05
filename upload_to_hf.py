# upload_to_hf.py

import os
from huggingface_hub import HfApi, HfFolder
import argparse

def main():
    """
    Uploads a single file to a Hugging Face dataset repository.
    This script is designed to be run by GitHub Actions.
    """
    parser = argparse.ArgumentParser(description="Upload a dataset file to the Hugging Face Hub.")
    parser.add_argument("--repo-id", type=str, required=True, help="The ID of the repository on the Hub (e.g., 'username/my-dataset').")
    parser.add_argument("--local-path", type=str, required=True, help="The local path to the file to upload.")
    parser.add_argument("--path-in-repo", type=str, help="The path where the file should be stored in the repo. Defaults to the local filename.")
    
    args = parser.parse_args()

    hf_token = os.environ.get("HUGGINGFACE_TOKEN")
    if not hf_token:
        raise ValueError("HUGGINGFACE_TOKEN environment variable is not set. Please set it in your GitHub repository secrets.")

    print(f"Authenticating with Hugging Face Hub...")
    api = HfApi(token=hf_token)

    # The path where the file will be stored in the dataset repo.
    # We put it inside a 'data' folder for good practice.
    path_in_repo = args.path_in_repo or f"data/{os.path.basename(args.local_path)}"

    print(f"Uploading '{args.local_path}' to '{args.repo_id}' at path '{path_in_repo}'...")

    api.upload_file(
        path_or_fileobj=args.local_path,
        path_in_repo=path_in_repo,
        repo_id=args.repo_id,
        repo_type="dataset",
        commit_message=f"Upload dataset file {os.path.basename(args.local_path)}"
    )

    print("File uploaded successfully!")

if __name__ == "__main__":
    main()