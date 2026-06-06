import unittest
from pathlib import Path


class OtzariaWorkflowTests(unittest.TestCase):
    def test_downloads_library_141_archive_and_filters_it_to_extrabooks(self):
        workflow_path = (
            Path(__file__).resolve().parents[2]
            / ".github"
            / "workflows"
            / "upload_otzaria_dataset.yml"
        )
        workflow = workflow_path.read_text(encoding="utf-8")

        self.assertIn('OTZARIA_LEGACY_RELEASE: "library-141"', workflow)
        self.assertIn('OTZARIA_LEGACY_ARCHIVE: "otzaria_library_141.zip"', workflow)
        self.assertIn('gh release download "$OTZARIA_LEGACY_RELEASE"', workflow)
        self.assertIn('--pattern "$OTZARIA_RELEASE_ASSET"', workflow)
        self.assertIn('--output "$OTZARIA_SOURCE_DIR/$OTZARIA_LEGACY_ARCHIVE"', workflow)
        self.assertIn(
            '--archive-spec "$OTZARIA_SOURCE_DIR/$OTZARIA_LEGACY_ARCHIVE::ExtraBooks"',
            workflow,
        )


if __name__ == "__main__":
    unittest.main()
