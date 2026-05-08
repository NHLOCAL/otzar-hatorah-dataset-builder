from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

import pandas as pd


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "create_dataset_markdown.py"
    spec = importlib.util.spec_from_file_location("gmach_create_dataset_markdown", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class GmachDatasetBuildTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_module()

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_progress_manifest_round_trips_processed_sources(self) -> None:
        manifest_path = self.root / "output_parquet" / "processed_sources.json"

        self.module.save_progress_manifest(manifest_path, 7, {"a.docx", "b.pdf"})
        last_part, processed_sources = self.module.load_progress_manifest(manifest_path)

        self.assertEqual(last_part, 7)
        self.assertEqual(processed_sources, {"a.docx", "b.pdf"})

    def test_write_parquet_part_creates_dataset_part_without_jsonl(self) -> None:
        output_dir = self.root / "output_parquet"
        output_dir.mkdir()

        path = self.module.write_parquet_part(
            [
                {
                    "text": "טקסט",
                    "source": "book.docx",
                    "metadata": {"title": "book"},
                }
            ],
            output_dir,
            "otzar_hatorah_dataset",
            1,
        )

        self.assertEqual(path.name, "otzar_hatorah_dataset-part-00001.parquet")
        self.assertFalse(any(self.root.rglob("*.jsonl")))
        dataframe = pd.read_parquet(path)
        self.assertEqual(dataframe.loc[0, "text"], "טקסט")


if __name__ == "__main__":
    unittest.main()
