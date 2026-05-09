import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from otzaria_dataset.pipeline import (
    PipelineConfig,
    build_dataset,
    load_metadata_index,
    make_record,
)


class OtzariaPipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.archive_path = self.root / "otzaria_latest.zip"
        self.dicta_archive_path = self.root / "otzaria_dicta_latest.zip"
        self.manifest_path = self.root / "files_manifest.json"
        self.metadata_path = self.root / "metadata.json"

    def tearDown(self):
        self.tmp.cleanup()

    def write_archive(self, files, path=None):
        archive_path = path or self.archive_path
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path, content in files.items():
                archive.writestr(path, content)

    def write_json(self, path, data):
        path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    def test_load_metadata_index_matches_titles_without_loading_source_files(self):
        self.write_json(
            self.metadata_path,
            [
                {
                    "title": "ברכות",
                    "author": "מחבר א",
                    "pubDate": "ה' שפח",
                    "heDesc": "תיאור קצר",
                },
                {"title": "שבת", "author": "מחבר ב"},
            ],
        )

        index = load_metadata_index(self.metadata_path)

        self.assertEqual(index["ברכות"]["author"], "מחבר א")
        self.assertEqual(index["שבת"]["title"], "שבת")

    def test_make_record_derives_book_category_hash_and_license_note(self):
        metadata_index = {
            "ברכות": {
                "title": "ברכות",
                "author": "מחבר",
                "pubDate": "ה' שפח",
                "heDesc": "תיאור",
            }
        }
        manifest = {
            "sefariaToOtzaria/sefaria_export/ספרים/אוצריא/תלמוד/ברכות.txt": {
                "hash": "abc123"
            }
        }

        record = make_record(
            source_path="sefariaToOtzaria/sefaria_export/ספרים/אוצריא/תלמוד/ברכות.txt",
            text="\nטקסט הספר\n",
            metadata_index=metadata_index,
            manifest=manifest,
            github_release="library-140",
        )

        self.assertEqual(record["text"], "טקסט הספר")
        self.assertEqual(record["source"], "Otzaria Library")
        self.assertEqual(record["metadata"]["title"], "ברכות")
        self.assertEqual(record["metadata"]["author"], "מחבר")
        self.assertEqual(record["metadata"]["book_name"], "ברכות")
        self.assertEqual(record["metadata"]["category"], "תלמוד")
        self.assertEqual(record["metadata"]["source_collection"], "sefariaToOtzaria")
        self.assertEqual(record["metadata"]["file_hash"], "abc123")
        self.assertEqual(record["metadata"]["github_release"], "library-140")
        self.assertIn("source collection", record["metadata"]["license_note"])

    def test_build_dataset_streams_zip_writes_parquet_and_skips_invalid_rows(self):
        self.write_archive(
            {
                "sefariaToOtzaria/sefaria_export/ספרים/אוצריא/תלמוד/ברכות.txt": "טקסט ראשון",
                "MoreBooks/ספרים/אוצריא/הלכה/ספר שני.txt": "טקסט שני",
                "MoreBooks/ספרים/אוצריא/הלכה/ריק.txt": "   ",
                "MoreBooks/ספרים/אוצריא/הלכה/כפול.txt": "טקסט שני",
                "metadata.json": "{}",
                "image.png": "not text",
            }
        )
        self.write_json(
            self.manifest_path,
            {
                "sefariaToOtzaria/sefaria_export/ספרים/אוצריא/תלמוד/ברכות.txt": {
                    "hash": "hash-1"
                },
                "MoreBooks/ספרים/אוצריא/הלכה/ספר שני.txt": {"hash": "hash-2"},
            },
        )
        self.write_json(
            self.metadata_path,
            [
                {"title": "ברכות", "author": "מחבר א"},
                {"title": "ספר שני", "author": "מחבר ב"},
            ],
        )

        output_dir = self.root / "output_parquet"
        result = build_dataset(
            PipelineConfig(
                archive_path=self.archive_path,
                parquet_output_dir=output_dir,
                parquet_output_file="judaic_texts.parquet",
                manifest_path=self.manifest_path,
                metadata_path=self.metadata_path,
                github_release="library-140",
                parquet_shards=2,
                batch_size=1,
                show_progress=False,
            )
        )

        self.assertEqual(result.processed_records, 2)
        self.assertEqual(result.skipped_empty_texts, 1)
        self.assertEqual(result.duplicate_records, 1)
        self.assertEqual(result.skipped_non_txt_files, 2)
        self.assertEqual([path.name for path in result.parquet_paths], [
            "judaic_texts-part-00001.parquet",
            "judaic_texts-part-00002.parquet",
        ])

        table = pq.read_table(result.parquet_paths[0])
        self.assertEqual(table.column_names, ["text", "source", "metadata"])
        all_rows = []
        for path in result.parquet_paths:
            all_rows.extend(pq.read_table(path).to_pylist())
        self.assertEqual([row["metadata"]["author"] for row in all_rows], ["מחבר א", "מחבר ב"])

    def test_build_dataset_combines_multiple_archives_and_deduplicates_across_them(self):
        self.write_archive(
            {
                "main/ספרים/אוצריא/תלמוד/ברכות.txt": "טקסט ראשון",
                "main/ספרים/אוצריא/הלכה/כפול.txt": "טקסט כפול",
            }
        )
        self.write_archive(
            {
                "dicta/ספרים/אוצריא/דיקטה/ספר דיקטה.txt": "טקסט דיקטה",
                "dicta/ספרים/אוצריא/דיקטה/כפול דיקטה.txt": "טקסט כפול",
            },
            path=self.dicta_archive_path,
        )
        self.write_json(self.manifest_path, {})
        self.write_json(
            self.metadata_path,
            [
                {"title": "ברכות", "author": "מחבר א"},
                {"title": "ספר דיקטה", "author": "מחבר דיקטה"},
            ],
        )

        output_dir = self.root / "output_parquet"
        result = build_dataset(
            PipelineConfig(
                archive_path=self.archive_path,
                archive_paths=(self.archive_path, self.dicta_archive_path),
                parquet_output_dir=output_dir,
                parquet_output_file="judaic_texts.parquet",
                manifest_path=self.manifest_path,
                metadata_path=self.metadata_path,
                github_release="library-143",
                parquet_shards=1,
                batch_size=10,
                show_progress=False,
            )
        )

        self.assertEqual(result.processed_records, 3)
        self.assertEqual(result.duplicate_records, 1)

        rows = pq.read_table(result.parquet_paths[0]).to_pylist()
        self.assertEqual(
            [row["metadata"]["source_path"] for row in rows],
            [
                "main/ספרים/אוצריא/תלמוד/ברכות.txt",
                "main/ספרים/אוצריא/הלכה/כפול.txt",
                "dicta/ספרים/אוצריא/דיקטה/ספר דיקטה.txt",
            ],
        )
        self.assertEqual([row["metadata"]["github_release"] for row in rows], ["library-143"] * 3)


if __name__ == "__main__":
    unittest.main()
