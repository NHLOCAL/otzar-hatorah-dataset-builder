from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from alonim.pdf_mirror import create_horizontally_mirrored_pdf


class PdfMirrorTests(unittest.TestCase):
    def test_create_horizontally_mirrored_pdf_writes_pdf(self) -> None:
        from tempfile import TemporaryDirectory

        from pypdf import PdfReader, PdfWriter

        with TemporaryDirectory() as temp_dir:
            source_path = Path(temp_dir) / "source.pdf"
            output_path = Path(temp_dir) / "mirrored.pdf"

            writer = PdfWriter()
            writer.add_blank_page(width=600, height=800)
            with source_path.open("wb") as handle:
                writer.write(handle)

            create_horizontally_mirrored_pdf(source_path, output_path)

            self.assertTrue(output_path.is_file())
            self.assertEqual(len(PdfReader(str(output_path)).pages), 1)


if __name__ == "__main__":
    unittest.main()
