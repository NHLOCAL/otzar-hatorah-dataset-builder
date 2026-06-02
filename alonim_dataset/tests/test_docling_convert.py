from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from alonim.docling_convert import (
    DEFAULT_DOCLING_ARGS,
    convert_pdf_to_docling_json,
    docling_json_output_path,
    merge_original_text_into_mirrored_layout,
    restore_mirrored_docling_layout,
)


class DoclingConvertTests(unittest.TestCase):
    def test_default_docling_args_use_table_model_without_ocr(self) -> None:
        self.assertIn("--no-ocr", DEFAULT_DOCLING_ARGS)
        self.assertIn("--table-mode", DEFAULT_DOCLING_ARGS)
        self.assertIn("accurate", DEFAULT_DOCLING_ARGS)

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

    def test_restore_mirrored_docling_layout_flips_bbox_coordinates_back(self) -> None:
        data = {
            "pages": {"1": {"size": {"width": 600, "height": 800}}},
            "texts": [
                {
                    "text": "סלב לאכימ יכדרמ",
                    "prov": [{"page_no": 1, "bbox": {"l": 100, "r": 220, "t": 700, "b": 650}}],
                }
            ],
            "pictures": [
                {
                    "prov": [{"page_no": 1, "bbox": {"l": 40, "r": 140, "t": 300, "b": 200}}],
                }
            ],
        }

        restored = restore_mirrored_docling_layout(data)

        self.assertEqual(restored["texts"][0]["text"], "מרדכי מיכאל בלס")
        self.assertEqual(restored["texts"][0]["prov"][0]["bbox"]["l"], 380)
        self.assertEqual(restored["texts"][0]["prov"][0]["bbox"]["r"], 500)
        self.assertEqual(restored["pictures"][0]["prov"][0]["bbox"]["l"], 460)
        self.assertEqual(restored["pictures"][0]["prov"][0]["bbox"]["r"], 560)
        self.assertTrue(restored["metadata"]["rtl_mirrored_input"])

    def test_merge_original_text_into_mirrored_layout_uses_overlapping_source_text(self) -> None:
        original = {
            "texts": [
                {
                    "self_ref": "#/texts/0",
                    "label": "text",
                    "text": "כך מצינו ב'מגן אברהם' שביאר טעם הדבר שמברכים על נטילת ידים בכל בוקר.",
                    "prov": [{"page_no": 1, "bbox": {"l": 300, "t": 340, "r": 550, "b": 130}}],
                }
            ]
        }
        mirrored = {
            "texts": [
                {
                    "self_ref": "#/texts/0",
                    "label": "text",
                    "text": "*",
                    "prov": [{"page_no": 1, "bbox": {"l": 430, "t": 325, "r": 433, "b": 322}}],
                },
                {
                    "self_ref": "#/texts/1",
                    "label": "text",
                    "text": "רקוב לכב םידי תליטנ לע םיכרבמש רבד",
                    "prov": [{"page_no": 1, "bbox": {"l": 310, "t": 310, "r": 545, "b": 145}}],
                },
                {
                    "self_ref": "#/texts/2",
                    "label": "text",
                    "text": "לפוכמ עטק",
                    "prov": [{"page_no": 1, "bbox": {"l": 320, "t": 250, "r": 540, "b": 180}}],
                },
                {
                    "self_ref": "#/texts/3",
                    "label": "section_header",
                    "text": "כותרת שזוהתה במירור",
                    "prov": [{"page_no": 1, "bbox": {"l": 320, "t": 250, "r": 540, "b": 180}}],
                },
            ]
        }

        merge_original_text_into_mirrored_layout(original, mirrored)

        self.assertEqual(mirrored["texts"][0]["text"], "*")
        self.assertEqual(
            mirrored["texts"][1]["text"],
            "כך מצינו ב'מגן אברהם' שביאר טעם הדבר שמברכים על נטילת ידים בכל בוקר.",
        )
        self.assertEqual(mirrored["texts"][2]["text"], "")
        self.assertEqual(mirrored["texts"][3]["text"], "כותרת שזוהתה במירור")

    def test_convert_pdf_to_docling_json_mirrors_input_without_enabling_ocr(self) -> None:
        from tempfile import TemporaryDirectory

        from pypdf import PdfWriter

        captured_argv: list[list[str]] = []

        def fake_docling_app() -> None:
            captured_argv.append(sys.argv[:])
            input_path = Path(sys.argv[-3])
            output_dir = Path(sys.argv[-1])

            self.assertTrue(input_path.is_file())
            self.assertEqual(input_path.name, "עלון.pdf")

            output_dir.mkdir(parents=True, exist_ok=True)
            text = "מרדכי מיכאל בלס" if len(captured_argv) == 1 else "סלב לאכימ יכדרמ"
            (output_dir / "עלון.json").write_text(
                (
                    '{"pages":{"1":{"size":{"width":600,"height":800}}},'
                    f'"texts":[{{"text":"{text}","prov":[{{"page_no":1,"bbox":{{"l":100,"r":220,"t":700,"b":650}}}}]}}]}}'
                ),
                encoding="utf-8",
            )
            raise SystemExit(0)

        modules_to_restore = {name: sys.modules.get(name) for name in ("docling", "docling.cli", "docling.cli.main")}
        sys.modules["docling"] = types.ModuleType("docling")
        sys.modules["docling.cli"] = types.ModuleType("docling.cli")
        sys.modules["docling.cli.main"] = types.ModuleType("docling.cli.main")
        sys.modules["docling.cli.main"].app = fake_docling_app

        try:
            with TemporaryDirectory() as temp_dir:
                source_root = Path(temp_dir) / "source"
                output_dir = Path(temp_dir) / "json"
                pdf_path = source_root / "series" / "עלון.pdf"
                pdf_path.parent.mkdir(parents=True)

                writer = PdfWriter()
                writer.add_blank_page(width=600, height=800)
                with pdf_path.open("wb") as handle:
                    writer.write(handle)

                output_path = convert_pdf_to_docling_json(
                    pdf_path,
                    output_dir=output_dir,
                    source_root=source_root,
                    rtl_mirror_input=True,
                )

                self.assertEqual(output_path, output_dir / "series" / "עלון.json")
                self.assertEqual(len(captured_argv), 2)
                self.assertIn("--no-ocr", captured_argv[-1])
                self.assertIn("--table-mode", captured_argv[-1])
                self.assertIn("accurate", captured_argv[-1])
                output_text = output_path.read_text(encoding="utf-8")
                self.assertIn('"rtl_mirrored_input": true', output_text)
                self.assertIn("מרדכי מיכאל בלס", output_text)
        finally:
            for name, module in modules_to_restore.items():
                if module is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = module


if __name__ == "__main__":
    unittest.main()
