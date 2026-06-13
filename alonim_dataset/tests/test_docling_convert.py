from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from alonim.docling_convert import docling_json_output_path


class DoclingConvertTests(unittest.TestCase):
    def test_docling_json_output_path_preserves_source_relative_structure(self) -> None:
        source_root = Path("alonim_dataset/source_data/pdf")
        output_root = Path("alonim_dataset/intermediate/docling_json")
        pdf_path = source_root / "תשפ_ג" / "בראשית" / "ויקרא שמם אדם" / "מאמרי הרב מרדכי בלס.pdf"

        self.assertEqual(
            docling_json_output_path(pdf_path, output_root, source_root),
            output_root / "תשפ_ג" / "בראשית" / "ויקרא שמם אדם" / "מאמרי הרב מרדכי בלס.json",
        )

    def test_docling_json_output_path_uses_stem_when_source_is_outside_root(self) -> None:
        self.assertEqual(
            docling_json_output_path(Path("C:/tmp/input.pdf"), Path("out"), Path("alonim_dataset/source_data/pdf")),
            Path("out/input.json"),
        )


if __name__ == "__main__":
    unittest.main()
