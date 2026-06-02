from __future__ import annotations

import json
import sys
from pathlib import Path

from .config import DEFAULT_DOCLING_JSON_DIR, DEFAULT_PDF_DIR


DEFAULT_DOCLING_ARGS = (
    "--no-ocr",
    "--pdf-backend",
    "pypdfium2",
    "--table-mode",
    "accurate",
    "--image-export-mode",
    "placeholder",
    "--to",
    "json",
)


def convert_pdf_to_docling_json(
    pdf_path: Path,
    output_dir: Path = DEFAULT_DOCLING_JSON_DIR,
    source_root: Path = DEFAULT_PDF_DIR,
    extra_args: tuple[str, ...] = (),
    rtl_mirror_input: bool = False,
) -> Path:
    pdf_path = pdf_path.expanduser().resolve()
    if not pdf_path.is_file():
        raise FileNotFoundError(f"Source PDF was not found: {pdf_path}")

    output_path = docling_json_output_path(pdf_path, output_dir, source_root)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if rtl_mirror_input:
        if extra_args:
            raise ValueError("extra_args are not supported with rtl_mirror_input")
        _convert_pdf_to_docling_json_with_mirror_pipeline(pdf_path, output_path)
    else:
        _run_docling_cli(pdf_path, output_path.parent, extra_args)

    return output_path


def _convert_pdf_to_docling_json_with_mirror_pipeline(pdf_path: Path, output_path: Path) -> None:
    from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
    from docling.document_converter import DocumentConverter, PdfFormatOption

    opts = PdfPipelineOptions(do_ocr=False, do_table_structure=True)
    opts.table_structure_options.mode = TableFormerMode.ACCURATE

    converter = DocumentConverter(
        allowed_formats=[InputFormat.PDF],
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_cls=_rtl_mirror_layout_pipeline_class(),
                backend=PyPdfiumDocumentBackend,
                pipeline_options=opts,
            )
        },
    )
    result = converter.convert(pdf_path)
    data = restore_mirrored_docling_layout(result.document.export_to_dict())
    output_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
        newline="\n",
    )


def _rtl_mirror_layout_pipeline_class() -> type:
    from PIL import ImageOps
    from docling.datamodel.base_models import BoundingBox
    from docling.models.stages.page_preprocessing.page_preprocessing_model import (
        PagePreprocessingModel,
        PagePreprocessingOptions,
    )
    from docling.pipeline.standard_pdf_pipeline import StandardPdfPipeline
    from docling_core.types.doc.page import BoundingRectangle

    def mirror_bbox(bbox: BoundingBox, width: float) -> BoundingBox:
        return BoundingBox(
            l=width - bbox.r,
            r=width - bbox.l,
            t=bbox.t,
            b=bbox.b,
            coord_origin=bbox.coord_origin,
        )

    class RtlMirrorPagePreprocessingModel(PagePreprocessingModel):
        def _populate_page_images(self, page):  # type: ignore[no-untyped-def]
            page = super()._populate_page_images(page)
            for scale, image in list(page._image_cache.items()):
                page._image_cache[scale] = ImageOps.mirror(image)
            return page

        def _parse_page_cells(self, conv_res, page):  # type: ignore[no-untyped-def]
            page = super()._parse_page_cells(conv_res, page)
            if page.size is None or page.parsed_page is None:
                return page

            page_width = page.size.width
            for cell in page.parsed_page.textline_cells:
                cell.rect = BoundingRectangle.from_bounding_box(
                    mirror_bbox(cell.rect.to_bounding_box(), page_width)
                )
            return page

    class RtlMirrorLayoutPipeline(StandardPdfPipeline):
        def _init_models(self) -> None:
            super()._init_models()
            self.preprocessing_model = RtlMirrorPagePreprocessingModel(
                options=PagePreprocessingOptions(images_scale=self.pipeline_options.images_scale)
            )

    return RtlMirrorLayoutPipeline


def _run_docling_cli(input_path: Path, output_dir: Path, extra_args: tuple[str, ...]) -> None:
    from docling.cli.main import app

    original_argv = sys.argv[:]
    sys.argv = [
        "docling",
        *DEFAULT_DOCLING_ARGS,
        *extra_args,
        str(input_path),
        "--output",
        str(output_dir),
    ]

    try:
        try:
            app()
        except SystemExit as exc:
            if exc.code not in (0, None):
                raise
    finally:
        sys.argv = original_argv


def docling_json_output_path(pdf_path: Path, output_dir: Path, source_root: Path = DEFAULT_PDF_DIR) -> Path:
    pdf_path = pdf_path.expanduser().resolve()
    output_dir = output_dir.expanduser()
    source_root = source_root.expanduser().resolve()

    try:
        relative_path = pdf_path.relative_to(source_root)
    except ValueError:
        relative_path = Path(pdf_path.name)

    return output_dir / relative_path.with_suffix(".json")


def restore_mirrored_docling_layout(data: dict) -> dict:
    page_widths = {
        str(page_no): page.get("size", {}).get("width")
        for page_no, page in data.get("pages", {}).items()
        if isinstance(page, dict)
    }

    for collection_key in ("texts", "pictures", "tables", "groups"):
        for item in data.get(collection_key, []):
            if isinstance(item, dict):
                _restore_item_bbox(item, page_widths)

    metadata = data.setdefault("metadata", {})
    if isinstance(metadata, dict):
        metadata["rtl_mirrored_input"] = True
        metadata["rtl_single_pass_layout_mirror"] = True

    return data


def _restore_item_bbox(item: dict, page_widths: dict[str, float | int | None]) -> None:
    for provenance in item.get("prov", []):
        if not isinstance(provenance, dict):
            continue
        bbox = provenance.get("bbox")
        if not isinstance(bbox, dict):
            continue

        page_no = str(provenance.get("page_no", ""))
        page_width = page_widths.get(page_no)
        if page_width is None:
            continue

        left = bbox.get("l")
        right = bbox.get("r")
        if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
            continue

        bbox["l"] = page_width - right
        bbox["r"] = page_width - left
