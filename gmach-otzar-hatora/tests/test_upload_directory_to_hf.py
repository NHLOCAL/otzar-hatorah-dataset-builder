import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from upload_directory_to_hf import upload_directory


class FakeHfApi:
    def __init__(self):
        self.calls = []

    def upload_folder(self, **kwargs):
        self.calls.append(kwargs)


class UploadDirectoryTests(unittest.TestCase):
    def test_upload_directory_passes_delete_patterns_and_commit_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            api = FakeHfApi()

            upload_directory(
                repo_id="NHLOCAL/judaic-texts-corpus",
                local_dir=Path(tmp),
                path_in_repo="data",
                commit_message="Update Otzaria dataset",
                delete_patterns=["data/*.parquet"],
                api=api,
                token="token",
            )

        self.assertEqual(api.calls[0]["repo_id"], "NHLOCAL/judaic-texts-corpus")
        self.assertEqual(api.calls[0]["repo_type"], "dataset")
        self.assertEqual(api.calls[0]["path_in_repo"], "data")
        self.assertEqual(api.calls[0]["commit_message"], "Update Otzaria dataset")
        self.assertEqual(api.calls[0]["delete_patterns"], ["data/*.parquet"])

    def test_upload_directory_requires_token(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "HUGGINGFACE_TOKEN"):
                upload_directory(
                    repo_id="NHLOCAL/judaic-texts-corpus",
                    local_dir=Path(tmp),
                    path_in_repo="data",
                    commit_message="Update",
                    delete_patterns=[],
                )


if __name__ == "__main__":
    unittest.main()
