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
                    "text": "מרדכי מיכאל בלס",
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

    def test_convert_pdf_to_docling_json_mirrors_input_without_enabling_ocr(self) -> None:
        from tempfile import TemporaryDirectory

        captured_pipeline_options: list[object] = []
        test_case = self

        class FakeDocument:
            def export_to_dict(self) -> dict:
                return {
                    "pages": {"1": {"size": {"width": 600, "height": 800}}},
                    "texts": [
                        {
                            "text": "מרדכי מיכאל בלס",
                            "prov": [{"page_no": 1, "bbox": {"l": 100, "r": 220, "t": 700, "b": 650}}],
                        }
                    ],
                }

        class FakeConversionResult:
            document = FakeDocument()

        class FakeDocumentConverter:
            def __init__(self, *, allowed_formats: list[object], format_options: dict[object, object]) -> None:
                self.allowed_formats = allowed_formats
                self.format_options = format_options
                option = next(iter(format_options.values()))
                captured_pipeline_options.append(option.pipeline_options)
                test_case.assertEqual(option.pipeline_cls.__name__, "RtlMirrorLayoutPipeline")

            def convert(self, source: Path) -> FakeConversionResult:
                test_case.assertEqual(source.name, "עלון.pdf")
                return FakeConversionResult()

        modules_to_restore = {
            name: sys.modules.get(name)
            for name in (
                "docling",
                "docling_core",
                "docling_core.types",
                "docling_core.types.doc",
                "docling_core.types.doc.page",
                "docling.datamodel",
                "docling.datamodel.base_models",
                "docling.datamodel.pipeline_options",
                "docling.document_converter",
                "docling.backend",
                "docling.backend.pypdfium2_backend",
                "docling.models",
                "docling.models.stages",
                "docling.models.stages.page_preprocessing",
                "docling.models.stages.page_preprocessing.page_preprocessing_model",
                "docling.pipeline",
                "docling.pipeline.standard_pdf_pipeline",
            )
        }
        sys.modules["docling"] = types.ModuleType("docling")
        sys.modules["docling_core"] = types.ModuleType("docling_core")
        sys.modules["docling_core.types"] = types.ModuleType("docling_core.types")
        sys.modules["docling_core.types.doc"] = types.ModuleType("docling_core.types.doc")
        sys.modules["docling_core.types.doc.page"] = types.ModuleType("docling_core.types.doc.page")
        sys.modules["docling.datamodel"] = types.ModuleType("docling.datamodel")
        sys.modules["docling.datamodel.base_models"] = types.ModuleType("docling.datamodel.base_models")
        sys.modules["docling.datamodel.pipeline_options"] = types.ModuleType("docling.datamodel.pipeline_options")
        sys.modules["docling.document_converter"] = types.ModuleType("docling.document_converter")
        sys.modules["docling.backend"] = types.ModuleType("docling.backend")
        sys.modules["docling.backend.pypdfium2_backend"] = types.ModuleType("docling.backend.pypdfium2_backend")
        sys.modules["docling.models"] = types.ModuleType("docling.models")
        sys.modules["docling.models.stages"] = types.ModuleType("docling.models.stages")
        sys.modules["docling.models.stages.page_preprocessing"] = types.ModuleType(
            "docling.models.stages.page_preprocessing"
        )
        sys.modules["docling.models.stages.page_preprocessing.page_preprocessing_model"] = types.ModuleType(
            "docling.models.stages.page_preprocessing.page_preprocessing_model"
        )
        sys.modules["docling.pipeline"] = types.ModuleType("docling.pipeline")
        sys.modules["docling.pipeline.standard_pdf_pipeline"] = types.ModuleType("docling.pipeline.standard_pdf_pipeline")

        class FakeInputFormat:
            PDF = "pdf"

        class FakeBoundingBox:
            def __init__(self, *, l: float, r: float, t: float, b: float, coord_origin: object = None) -> None:
                self.l = l
                self.r = r
                self.t = t
                self.b = b
                self.coord_origin = coord_origin

        class FakeTableFormerMode:
            ACCURATE = "accurate"

        class FakeTableStructureOptions:
            def __init__(self) -> None:
                self.mode = None

        class FakePdfPipelineOptions:
            def __init__(self, *, do_ocr: bool, do_table_structure: bool) -> None:
                self.do_ocr = do_ocr
                self.do_table_structure = do_table_structure
                self.table_structure_options = FakeTableStructureOptions()

        class FakePdfFormatOption:
            def __init__(self, *, pipeline_cls: type, backend: type, pipeline_options: object) -> None:
                self.pipeline_cls = pipeline_cls
                self.backend = backend
                self.pipeline_options = pipeline_options

        class FakePyPdfiumDocumentBackend:
            pass

        class FakeBoundingRectangle:
            @classmethod
            def from_bounding_box(cls, bbox: object) -> object:
                return bbox

        class FakePagePreprocessingModel:
            def __init__(self, options: object) -> None:
                self.options = options

            def _populate_page_images(self, page: object) -> object:
                return page

            def _parse_page_cells(self, conv_res: object, page: object) -> object:
                return page

        class FakePagePreprocessingOptions:
            def __init__(self, images_scale: object) -> None:
                self.images_scale = images_scale

        class FakeStandardPdfPipeline:
            def _init_models(self) -> None:
                self.preprocessing_model = None

        sys.modules["docling.datamodel.base_models"].InputFormat = FakeInputFormat
        sys.modules["docling.datamodel.base_models"].BoundingBox = FakeBoundingBox
        sys.modules["docling.datamodel.pipeline_options"].PdfPipelineOptions = FakePdfPipelineOptions
        sys.modules["docling.datamodel.pipeline_options"].TableFormerMode = FakeTableFormerMode
        sys.modules["docling.document_converter"].DocumentConverter = FakeDocumentConverter
        sys.modules["docling.document_converter"].PdfFormatOption = FakePdfFormatOption
        sys.modules["docling.backend.pypdfium2_backend"].PyPdfiumDocumentBackend = FakePyPdfiumDocumentBackend
        sys.modules["docling_core.types.doc.page"].BoundingRectangle = FakeBoundingRectangle
        sys.modules[
            "docling.models.stages.page_preprocessing.page_preprocessing_model"
        ].PagePreprocessingModel = FakePagePreprocessingModel
        sys.modules[
            "docling.models.stages.page_preprocessing.page_preprocessing_model"
        ].PagePreprocessingOptions = FakePagePreprocessingOptions
        sys.modules["docling.pipeline.standard_pdf_pipeline"].StandardPdfPipeline = FakeStandardPdfPipeline

        try:
            with TemporaryDirectory() as temp_dir:
                source_root = Path(temp_dir) / "source"
                output_dir = Path(temp_dir) / "json"
                pdf_path = source_root / "series" / "עלון.pdf"
                pdf_path.parent.mkdir(parents=True)
                pdf_path.write_bytes(b"%PDF-1.7\n")

                output_path = convert_pdf_to_docling_json(
                    pdf_path,
                    output_dir=output_dir,
                    source_root=source_root,
                    rtl_mirror_input=True,
                )

                self.assertEqual(output_path, output_dir / "series" / "עלון.json")
                self.assertEqual(len(captured_pipeline_options), 1)
                self.assertFalse(captured_pipeline_options[0].do_ocr)
                self.assertTrue(captured_pipeline_options[0].do_table_structure)
                self.assertEqual(captured_pipeline_options[0].table_structure_options.mode, "accurate")
                output_text = output_path.read_text(encoding="utf-8")
                self.assertIn('"rtl_mirrored_input": true', output_text)
                self.assertIn("מרדכי מיכאל בלס", output_text)
                self.assertNotIn("סלב לאכימ יכדרמ", output_text)
        finally:
            for name, module in modules_to_restore.items():
                if module is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = module


if __name__ == "__main__":
    unittest.main()
