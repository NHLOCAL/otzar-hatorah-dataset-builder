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

    def delete_files(self, **kwargs):
        self.calls.append(("delete_files", kwargs))

    def upload_folder(self, **kwargs):
        self.calls.append(("upload_folder", kwargs))


class UploadDirectoryTests(unittest.TestCase):
    def test_upload_directory_deletes_repo_root_patterns_before_upload(self):
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

        self.assertEqual([name for name, _ in api.calls], ["delete_files", "upload_folder"])
        self.assertEqual(api.calls[0][1]["delete_patterns"], ["data/*.parquet"])
        self.assertEqual(api.calls[0][1]["repo_id"], "NHLOCAL/judaic-texts-corpus")
        self.assertEqual(api.calls[0][1]["repo_type"], "dataset")

        upload_kwargs = api.calls[1][1]
        self.assertEqual(upload_kwargs["repo_id"], "NHLOCAL/judaic-texts-corpus")
        self.assertEqual(upload_kwargs["repo_type"], "dataset")
        self.assertEqual(upload_kwargs["path_in_repo"], "data")
        self.assertEqual(upload_kwargs["commit_message"], "Update Otzaria dataset")
        self.assertNotIn("delete_patterns", upload_kwargs)

    def test_upload_directory_skips_delete_call_without_patterns(self):
        with tempfile.TemporaryDirectory() as tmp:
            api = FakeHfApi()

            upload_directory(
                repo_id="nhlocal/otzar-hatorah",
                local_dir=Path(tmp),
                path_in_repo="data",
                commit_message="Update",
                delete_patterns=[],
                api=api,
                token="token",
            )

        self.assertEqual([name for name, _ in api.calls], ["upload_folder"])

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
