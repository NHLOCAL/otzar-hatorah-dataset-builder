import argparse
import os
from pathlib import Path

from huggingface_hub import HfApi


def upload_directory(
    repo_id: str,
    local_dir: Path,
    path_in_repo: str = "data",
    commit_message: str = "Update dataset from GitHub Actions",
    delete_patterns: list[str] | None = None,
    api: HfApi | None = None,
    token: str | None = None,
) -> None:
    hf_token = token or os.environ.get("HUGGINGFACE_TOKEN")
    if not hf_token:
        raise ValueError(
            "HUGGINGFACE_TOKEN environment variable is not set. "
            "Please set it in your GitHub repository secrets."
        )

    if not local_dir.is_dir():
        raise ValueError(f"The provided local path '{local_dir}' is not a directory.")

    resolved_api = api or HfApi(token=hf_token)
    if delete_patterns:
        resolved_api.delete_files(
            repo_id=repo_id,
            delete_patterns=delete_patterns,
            repo_type="dataset",
            commit_message=f"Delete old files before upload: {commit_message}",
        )

    resolved_api.upload_folder(
        folder_path=str(local_dir),
        path_in_repo=path_in_repo,
        repo_id=repo_id,
        repo_type="dataset",
        commit_message=commit_message,
    )


def main():
    """Uploads a local dataset directory to a Hugging Face dataset repository."""
    parser = argparse.ArgumentParser(description="Upload a dataset directory to the Hugging Face Hub.")
    parser.add_argument(
        "--repo-id",
        type=str,
        required=True,
        help="The ID of the repository on the Hub (e.g., 'username/my-dataset').",
    )
    parser.add_argument(
        "--local-dir",
        type=str,
        required=True,
        help="The local path to the directory to upload.",
    )
    parser.add_argument(
        "--path-in-repo",
        type=str,
        default="data",
        help="The target directory path in the repo. Defaults to 'data'.",
    )
    parser.add_argument(
        "--commit-message",
        type=str,
        default="Update dataset from GitHub Actions",
        help="Commit message to use on the Hugging Face Hub.",
    )
    parser.add_argument(
        "--delete-pattern",
        action="append",
        default=[],
        help=(
            "Repository-root-relative pattern to delete before upload. "
            "May be provided more than once, for example --delete-pattern 'data/*.parquet'."
        ),
    )

    args = parser.parse_args()

    print("Authenticating with Hugging Face Hub...")
    if args.delete_pattern:
        print(f"Deleting existing remote files matching: {args.delete_pattern}")
    print(f"Uploading directory '{args.local_dir}' to '{args.repo_id}' repository under '{args.path_in_repo}'...")

    upload_directory(
        repo_id=args.repo_id,
        local_dir=Path(args.local_dir),
        path_in_repo=args.path_in_repo,
        commit_message=args.commit_message,
        delete_patterns=args.delete_pattern,
    )

    print("Directory uploaded successfully!")
    print(f"Check your dataset at: https://huggingface.co/datasets/{args.repo_id}/tree/main/{args.path_in_repo}")


if __name__ == "__main__":
    main()
