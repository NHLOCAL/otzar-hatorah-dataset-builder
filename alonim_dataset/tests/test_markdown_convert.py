from __future__ import annotations

import sys
import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from alonim.markdown_convert import (
    convert_docling_json_to_markdown,
    postprocess_markdown,
    process_docling_document,
    sort_page_elements,
)
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
        self.assertTrue(pages[1][0]["is_framed"])

    def test_process_docling_document_skips_picture_children_seen_as_body_text(self) -> None:
        data = {
            "body": {"children": [{"$ref": "#/pictures/0"}, {"$ref": "#/texts/0"}]},
            "texts": [
                {
                    "self_ref": "#/texts/0",
                    "label": "text",
                    "text": "טקסט במסגרת",
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

        self.assertEqual([item["text"] for item in pages[1]], ["טקסט במסגרת"])

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

    def test_mirrored_docling_markdown_keeps_docling_body_order(self) -> None:
        data = {
            "metadata": {"rtl_mirrored_input": True},
            "pages": {"1": {"size": {"width": 600, "height": 800}}},
            "body": {
                "children": [
                    {"$ref": "#/texts/0"},
                    {"$ref": "#/texts/1"},
                    {"$ref": "#/texts/2"},
                ]
            },
            "texts": [
                {
                    "self_ref": "#/texts/0",
                    "label": "text",
                    "text": "ראשון לפי דוקלינג",
                    "prov": [{"page_no": 1, "bbox": {"l": 40, "t": 300, "r": 280, "b": 250}}],
                },
                {
                    "self_ref": "#/texts/1",
                    "label": "text",
                    "text": "שני לפי דוקלינג",
                    "prov": [{"page_no": 1, "bbox": {"l": 320, "t": 700, "r": 560, "b": 650}}],
                },
                {
                    "self_ref": "#/texts/2",
                    "label": "text",
                    "text": "שלישי לפי דוקלינג",
                    "prov": [{"page_no": 1, "bbox": {"l": 40, "t": 700, "r": 280, "b": 650}}],
                },
            ],
        }

        with TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "input.json"
            output_path = Path(temp_dir) / "output.md"
            json_path.write_text(__import__("json").dumps(data, ensure_ascii=False), encoding="utf-8")

            convert_docling_json_to_markdown(json_path, output_path)

            output = output_path.read_text(encoding="utf-8")
            self.assertLess(output.index("ראשון לפי דוקלינג"), output.index("שני לפי דוקלינג"))
            self.assertLess(output.index("שני לפי דוקלינג"), output.index("שלישי לפי דוקלינג"))

    def test_postprocess_markdown_splits_inline_star_section_markers(self) -> None:
        text = 'מדוע היה חשוב שאדם יתבע זאת בפיו. *"ויקרא האדם שמות לכל הבהמה"'

        processed = postprocess_markdown(text)

        self.assertEqual(
            processed,
            'מדוע היה חשוב שאדם יתבע זאת בפיו.\n\n*\n\n"ויקרא האדם שמות לכל הבהמה"',
        )

    def test_postprocess_markdown_moves_interleaved_question_before_open_quote_section(self) -> None:
        text = (
            "*\n\nמקור אחד - 'תחילת ציטוט ארוך שממשיך אחרי שאלת מעבר. "
            'והוא שהכתוב אומר "פסוק לדוגמה" (א, ב).\n\n'
            "מהי משמעות הדברים הללו. המשך הציטוט הפתוח שנבלע אחרי השאלה"
        )

        processed = postprocess_markdown(text)

        self.assertLess(processed.index("מהי משמעות הדברים הללו"), processed.index("מקור אחד"))
        self.assertIn("(א, ב). המשך הציטוט הפתוח שנבלע אחרי השאלה", processed)


if __name__ == "__main__":
    unittest.main()
