from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from alonim.dataset_build import markdown_to_parquet


class AlonimDatasetBuildTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_markdown_to_parquet_writes_deduplicated_parquet_without_jsonl(self) -> None:
        markdown_dir = self.root / "markdown"
        markdown_dir.mkdir()
        (markdown_dir / "a_first.md").write_text("טקסט ראשון\n", encoding="utf-8")
        (markdown_dir / "b_duplicate.md").write_text("טקסט ראשון\n", encoding="utf-8")
        (markdown_dir / "empty.md").write_text("   ", encoding="utf-8")

        output_dir = self.root / "parquet"
        result = markdown_to_parquet(
            input_dir=markdown_dir,
            output_dir=output_dir,
            output_file="alonim_dataset.parquet",
        )

        self.assertEqual(result.records, 1)
        self.assertEqual(result.parquet_path, output_dir / "alonim_dataset.parquet")
        self.assertFalse(any(self.root.rglob("*.jsonl")))

        table = pq.read_table(result.parquet_path)
        self.assertEqual(table.num_rows, 1)
        self.assertEqual(table.column_names, ["text", "source", "metadata"])
        self.assertEqual(table.to_pylist()[0]["metadata"]["title"], "a_first")


if __name__ == "__main__":
    unittest.main()
