from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from alonim.markdown_convert import process_docling_document, sort_page_elements
from alonim.profiles import BulletinProfile


def text_item(text: str, left: float, top: float, right: float, bottom: float) -> dict:
    return {
        "label": "text",
        "text": text,
        "prov": [{"page_no": 1, "bbox": {"l": left, "t": top, "r": right, "b": bottom}}],
    }


class MarkdownConvertTests(unittest.TestCase):
    def test_process_docling_document_includes_text_children_inside_pictures(self) -> None:
        data = {
            "body": {"children": [{"$ref": "#/pictures/0"}]},
            "texts": [
                {
                    "self_ref": "#/texts/0",
                    "label": "text",
                    "text": "כותרת מתוך מסגרת",
                    "prov": [{"page_no": 1, "bbox": {"l": 200, "t": 760, "r": 400, "b": 730}}],
                }
            ],
            "pictures": [
                {
                    "self_ref": "#/pictures/0",
                    "label": "picture",
                    "children": [{"$ref": "#/texts/0"}],
                }
            ],
        }

        pages = process_docling_document(data)

        self.assertEqual([item["text"] for item in pages[1]], ["כותרת מתוך מסגרת"])

    def test_two_column_rtl_profile_reads_right_column_before_left_column(self) -> None:
        elements = [
            text_item("שמאל עליון", 40, 700, 280, 650),
            text_item("ימין תחתון", 320, 620, 560, 570),
            text_item("ימין עליון", 320, 700, 560, 650),
            text_item("שמאל תחתון", 40, 620, 280, 570),
        ]

        body, footnotes = sort_page_elements(elements, page_width=600, profile=BulletinProfile.MORDECHAI_BLASS)

        self.assertEqual([item["text"] for item in body], ["ימין עליון", "ימין תחתון", "שמאל עליון", "שמאל תחתון"])
        self.assertEqual(footnotes, [])

    def test_two_column_rtl_profile_keeps_centered_headers_before_columns(self) -> None:
        header = text_item("כותרת מרכזית", 240, 760, 360, 735)
        header["label"] = "section_header"
        elements = [
            text_item("שמאל", 40, 700, 280, 650),
            text_item("ימין", 320, 700, 560, 650),
            header,
        ]

        body, footnotes = sort_page_elements(elements, page_width=600, profile=BulletinProfile.MORDECHAI_BLASS)

        self.assertEqual([item["text"] for item in body], ["כותרת מרכזית", "ימין", "שמאל"])
        self.assertEqual(footnotes, [])

    def test_two_column_rtl_profile_keeps_top_masthead_before_body(self) -> None:
        elements = [
            text_item("גוף ימין", 320, 650, 560, 500),
            text_item("גוף שמאל", 40, 650, 280, 500),
            text_item("גליון מס' 2", 60, 790, 120, 770),
            text_item("שם העלון", 240, 790, 360, 770),
        ]

        body, footnotes = sort_page_elements(elements, page_width=600, profile=BulletinProfile.METIKUT_HAPARSHA)

        self.assertEqual([item["text"] for item in body], ["גליון מס' 2", "שם העלון", "גוף ימין", "גוף שמאל"])
        self.assertEqual(footnotes, [])

    def test_single_column_profile_keeps_vertical_order_without_column_split(self) -> None:
        elements = [
            text_item("אמצע", 80, 500, 520, 450),
            text_item("ראשון", 320, 700, 560, 650),
            text_item("אחרון", 40, 300, 280, 250),
        ]

        body, footnotes = sort_page_elements(elements, page_width=600, profile=BulletinProfile.BIRKAT_YITZCHAK)

        self.assertEqual([item["text"] for item in body], ["ראשון", "אמצע", "אחרון"])
        self.assertEqual(footnotes, [])


if __name__ == "__main__":
    unittest.main()
