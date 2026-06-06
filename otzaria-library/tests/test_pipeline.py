import importlib.util
import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from otzaria_dataset.pipeline import (
    ArchiveInput,
    PipelineConfig,
    build_dataset,
    historical_only_archive_paths,
    load_metadata_index,
    make_record,
)

CREATE_DATASET_PATH = Path(__file__).resolve().parents[1] / "create_dataset.py"
CREATE_DATASET_SPEC = importlib.util.spec_from_file_location(
    "otzaria_create_dataset",
    CREATE_DATASET_PATH,
)
CREATE_DATASET_MODULE = importlib.util.module_from_spec(CREATE_DATASET_SPEC)
assert CREATE_DATASET_SPEC.loader is not None
CREATE_DATASET_SPEC.loader.exec_module(CREATE_DATASET_MODULE)


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

    def test_parse_archive_spec_supports_optional_path_component_filter(self):
        plain = CREATE_DATASET_MODULE.parse_archive_spec("source/latest.zip")
        filtered = CREATE_DATASET_MODULE.parse_archive_spec(
            "source/otzaria_library_141.zip::ExtraBooks"
        )

        self.assertEqual(plain, ArchiveInput(Path("source/latest.zip")))
        self.assertEqual(
            filtered,
            ArchiveInput(
                Path("source/otzaria_library_141.zip"),
                required_path_component="ExtraBooks",
            ),
        )

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

    def test_historical_manifest_difference_uses_flattened_zip_paths(self):
        current_manifest_path = self.root / "files_manifest_current.json"
        historical_manifest_path = self.root / "files_manifest_141.json"
        self.write_json(
            current_manifest_path,
            {
                "MoreBooks/ספרים/אוצריא/הלכה/ספר קיים.txt": {"hash": "current"},
                "current/links/קישור.json": {"hash": "link"},
            },
        )
        self.write_json(
            historical_manifest_path,
            {
                "sefariaToOtzaria/sefaria_export/ספרים/אוצריא/הלכה/ספר קיים.txt": {
                    "hash": "old"
                },
                "sefariaToOtzaria/sefaria_export/ספרים/אוצריא/מחשבה/ספר שנשמר.txt": {
                    "hash": "preserved"
                },
                "sefariaToOtzaria/sefaria_export/links/קישור.json": {"hash": "link"},
            },
        )

        paths = historical_only_archive_paths(
            historical_manifest_path,
            current_manifest_path,
        )

        self.assertEqual(paths, frozenset({"אוצריא/מחשבה/ספר שנשמר.txt"}))

    def test_historical_archive_only_adds_manifest_paths_and_latest_path_wins(self):
        historical_archive_path = self.root / "otzaria_library_141.zip"
        self.write_archive(
            {
                "אוצריא/הלכה/ספר קיים.txt": "נוסח עדכני",
                "אוצריא/הלכה/טקסט משותף.txt": "אותו טקסט",
            }
        )
        self.write_archive(
            {
                "אוצריא/הלכה/ספר קיים.txt": "נוסח ישן",
                "אוצריא/מחשבה/ספר שנשמר.txt": "תוכן שנשמר רק ב־141",
                "אוצריא/מחשבה/כפילות תוכן.txt": "אותו טקסט",
                "אוצריא/מחשבה/ספר שלא ייכלל.txt": "תוכן לא רצוי",
            },
            path=historical_archive_path,
        )
        self.write_json(self.manifest_path, {})
        self.write_json(self.metadata_path, [])

        result = build_dataset(
            PipelineConfig(
                archive_path=self.archive_path,
                archive_inputs=(
                    ArchiveInput(self.archive_path),
                    ArchiveInput(
                        historical_archive_path,
                        included_paths=frozenset(
                            {
                                "אוצריא/הלכה/ספר קיים.txt",
                                "אוצריא/מחשבה/ספר שנשמר.txt",
                                "אוצריא/מחשבה/כפילות תוכן.txt",
                            }
                        ),
                    ),
                ),
                parquet_output_dir=self.root / "output_parquet",
                parquet_output_file="judaic_texts.parquet",
                manifest_path=self.manifest_path,
                metadata_path=self.metadata_path,
                github_release="library-150",
                parquet_shards=1,
                batch_size=10,
                show_progress=False,
            )
        )

        rows = pq.read_table(result.parquet_paths[0]).to_pylist()
        self.assertEqual(
            [(row["metadata"]["book_name"], row["text"]) for row in rows],
            [
                ("ספר קיים", "נוסח עדכני"),
                ("טקסט משותף", "אותו טקסט"),
                ("ספר שנשמר", "תוכן שנשמר רק ב־141"),
            ],
        )
        self.assertEqual(result.processed_records, 3)
        self.assertEqual(result.duplicate_records, 2)


if __name__ == "__main__":
    unittest.main()
