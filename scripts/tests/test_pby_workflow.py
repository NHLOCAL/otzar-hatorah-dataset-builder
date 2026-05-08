from pathlib import Path
import unittest


WORKFLOW_PATH = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "upload_pby_dataset.yml"


class PbyWorkflowTests(unittest.TestCase):
    def test_downloads_release_assets_by_default_and_keeps_local_source_option(self):
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

        self.assertIn("pby_release:", workflow)
        self.assertIn("use_local_source:", workflow)
        self.assertIn('PBY_SOURCE_REPO: "projectbenyehuda/public_domain_dump"', workflow)
        self.assertIn('PBY_TEXT_ARCHIVE: "txt.zip"', workflow)
        self.assertIn('PBY_CATALOG_ASSET: "pseudocatalogue.csv"', workflow)
        self.assertIn('gh release download "${{ steps.release.outputs.tag }}"', workflow)
        self.assertIn('--pattern "$PBY_TEXT_ARCHIVE"', workflow)
        self.assertIn('--pattern "$PBY_CATALOG_ASSET"', workflow)
        self.assertIn('unzip -q "$PBY_SOURCE_DIR/$PBY_TEXT_ARCHIVE" -d "$PBY_SOURCE_DIR"', workflow)
        self.assertIn('PBY_TEXT_SOURCE_DIR="$PBY_SOURCE_DIR/txt"', workflow)
        self.assertIn('--source-dir "$PBY_TEXT_SOURCE_DIR"', workflow)
        self.assertIn("${{ (inputs.use_local_source || 'false') != 'true' }}", workflow)


if __name__ == "__main__":
    unittest.main()
