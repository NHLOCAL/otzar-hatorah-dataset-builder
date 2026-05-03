import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pby_dataset.pipeline import PipelineConfig, build_dataset, iter_catalog_rows, make_record, split_parquet_file


class BenYehudaPipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.source_dir = self.root / "source_data"
        self.source_dir.mkdir()
        self.catalog_file = self.source_dir / "pseudocatalogue.csv"

    def tearDown(self):
        self.tmp.cleanup()

    def write_catalog(self, rows):
        fieldnames = [
            "ID",
            "path",
            "title",
            "authors",
            "translators",
            "author_uris",
            "translator_uris",
            "original_language",
            "genre",
            "source_edition",
        ]
        with self.catalog_file.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def write_text(self, relative_path, text):
        target = self.source_dir / f"{relative_path.strip('/')}.txt"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def test_make_record_uses_nested_metadata_and_strips_text(self):
        row = {
            "ID": "10",
            "path": "/p23/m10",
            "title": "חצי-נחמה",
            "authors": "אחד העם",
            "translators": "",
            "author_uris": "https://wikidata.org/wiki/Q380425",
            "translator_uris": "",
            "original_language": "",
            "genre": "article",
            "source_edition": "",
        }

        record = make_record(row, "  טקסט לדוגמה  \n")

        self.assertEqual(record["text"], "טקסט לדוגמה")
        self.assertEqual(record["source"], "Project Ben-Yehuda")
        self.assertEqual(record["metadata"]["pby_id"], "10")
        self.assertEqual(record["metadata"]["filepath_pby"], "/p23/m10")

    def test_iter_catalog_rows_skips_blank_paths_without_loading_dataframe(self):
        self.write_catalog(
            [
                {"ID": "1", "path": "/p1/m1", "title": "א"},
                {"ID": "2", "path": "", "title": "ב"},
            ]
        )

        rows = list(iter_catalog_rows(self.catalog_file))

        self.assertEqual([row["ID"] for row in rows], ["1"])

    def test_build_dataset_writes_parquet_and_optional_jsonl(self):
        self.write_catalog(
            [
                {"ID": "1", "path": "/p1/m1", "title": "א", "authors": "מחבר"},
                {"ID": "2", "path": "/p1/m2", "title": "ב", "authors": "מחבר"},
                {"ID": "3", "path": "/missing/m3", "title": "חסר"},
            ]
        )
        self.write_text("/p1/m1", "טקסט ראשון\n")
        self.write_text("/p1/m2", "טקסט שני")

        output_dir = self.root / "output_parquet"
        jsonl_dir = self.root / "output_jsonl"
        result = build_dataset(
            PipelineConfig(
                source_dir=self.source_dir,
                catalog_file=self.catalog_file,
                parquet_output_dir=output_dir,
                parquet_output_file="pby_dataset.parquet",
                jsonl_output_dir=jsonl_dir,
                parquet_shards=1,
                batch_size=1,
                workers=2,
                show_progress=False,
            )
        )

        self.assertEqual(result.processed_records, 2)
        self.assertEqual(result.missing_text_files, 1)
        table = pq.read_table(output_dir / "pby_dataset.parquet")
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.column_names, ["text", "source", "metadata"])

        jsonl_files = sorted(jsonl_dir.glob("*.jsonl"))
        self.assertEqual(len(jsonl_files), 1)
        first_jsonl_record = json.loads(jsonl_files[0].read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(first_jsonl_record["metadata"]["title"], "א")

    def test_build_dataset_splits_parquet_into_requested_shards(self):
        self.write_catalog(
            [
                {"ID": str(index), "path": f"/p1/m{index}", "title": f"כותרת {index}"}
                for index in range(1, 6)
            ]
        )
        for index in range(1, 6):
            self.write_text(f"/p1/m{index}", f"טקסט {index}")

        output_dir = self.root / "output_parquet"
        result = build_dataset(
            PipelineConfig(
                source_dir=self.source_dir,
                catalog_file=self.catalog_file,
                parquet_output_dir=output_dir,
                parquet_output_file="pby_dataset.parquet",
                parquet_shards=3,
                batch_size=2,
                workers=2,
                show_progress=False,
            )
        )

        parquet_files = sorted(output_dir.glob("*.parquet"))

        self.assertEqual([path.name for path in parquet_files], [
            "pby_dataset-part-00001.parquet",
            "pby_dataset-part-00002.parquet",
            "pby_dataset-part-00003.parquet",
        ])
        self.assertEqual(result.parquet_paths, tuple(parquet_files))
        self.assertEqual(sum(pq.read_table(path).num_rows for path in parquet_files), 5)
        self.assertLessEqual(max(pq.read_table(path).num_rows for path in parquet_files), 2)

    def test_split_parquet_file_rewrites_existing_file_into_shards(self):
        self.write_catalog(
            [
                {"ID": str(index), "path": f"/p2/m{index}", "title": f"כותרת {index}"}
                for index in range(1, 5)
            ]
        )
        for index in range(1, 5):
            self.write_text(f"/p2/m{index}", f"טקסט {index}")

        source_output_dir = self.root / "source_parquet"
        build_dataset(
            PipelineConfig(
                source_dir=self.source_dir,
                catalog_file=self.catalog_file,
                parquet_output_dir=source_output_dir,
                parquet_output_file="single.parquet",
                parquet_shards=1,
                show_progress=False,
            )
        )

        target_output_dir = self.root / "split_parquet"
        split_paths = split_parquet_file(
            input_path=source_output_dir / "single.parquet",
            output_dir=target_output_dir,
            output_file="pby_dataset.parquet",
            shards=2,
            batch_size=2,
        )

        self.assertEqual([path.name for path in split_paths], [
            "pby_dataset-part-00001.parquet",
            "pby_dataset-part-00002.parquet",
        ])
        self.assertEqual(sum(pq.read_table(path).num_rows for path in split_paths), 4)


if __name__ == "__main__":
    unittest.main()
