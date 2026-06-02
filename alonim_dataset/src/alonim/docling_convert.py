from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from .config import DEFAULT_DOCLING_JSON_DIR, DEFAULT_PDF_DIR
from .pdf_mirror import create_horizontally_mirrored_pdf


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

    with tempfile.TemporaryDirectory(prefix="alonim-docling-rtl-") as temp_dir:
        temp_path = Path(temp_dir)
        original_data = None
        if rtl_mirror_input:
            original_output_dir = temp_path / "original"
            _run_docling_cli(pdf_path, original_output_dir, extra_args)
            original_json_path = original_output_dir / pdf_path.with_suffix(".json").name
            original_data = json.loads(original_json_path.read_text(encoding="utf-8"))

        docling_input_path = pdf_path
        if rtl_mirror_input:
            docling_input_path = temp_path / pdf_path.name
            create_horizontally_mirrored_pdf(pdf_path, docling_input_path)

        _run_docling_cli(docling_input_path, output_path.parent, extra_args)

        if rtl_mirror_input:
            data = json.loads(output_path.read_text(encoding="utf-8"))
            restored_data = restore_mirrored_docling_layout(data)
            if original_data is not None:
                merge_original_text_into_mirrored_layout(original_data, restored_data)
            output_path.write_text(
                json.dumps(restored_data, ensure_ascii=False, indent=2),
                encoding="utf-8",
                newline="\n",
            )

    return output_path


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
                _restore_item_text(item)
                _restore_item_bbox(item, page_widths)

    metadata = data.setdefault("metadata", {})
    if isinstance(metadata, dict):
        metadata["rtl_mirrored_input"] = True

    return data


def merge_original_text_into_mirrored_layout(original_data: dict, mirrored_data: dict) -> dict:
    original_items = [
        item
        for item in original_data.get("texts", [])
        if isinstance(item, dict) and isinstance(item.get("text"), str) and len(item.get("text", "").strip()) > 1
    ]
    used_original_refs: set[str] = set()
    used_original_items: list[dict] = []

    for mirrored_item in mirrored_data.get("texts", []):
        if not isinstance(mirrored_item, dict):
            continue
        if len(mirrored_item.get("text", "").strip()) <= 1:
            continue

        match = _best_overlapping_text_item(mirrored_item, original_items, used_original_refs)
        if match is None:
            if mirrored_item.get("label") != "section_header" and _overlaps_used_original_item(
                mirrored_item, used_original_items
            ):
                mirrored_item["text"] = ""
            continue

        mirrored_item["text"] = match["text"]
        if isinstance(match.get("orig"), str):
            mirrored_item["orig"] = match["orig"]
        if isinstance(match.get("self_ref"), str):
            used_original_refs.add(match["self_ref"])
            used_original_items.append(match)

    return mirrored_data


def _restore_item_text(item: dict) -> None:
    for key, value in item.items():
        if key == "text" and isinstance(value, str):
            item[key] = value[::-1]
        elif isinstance(value, dict):
            _restore_item_text(value)
        elif isinstance(value, list):
            for child in value:
                if isinstance(child, dict):
                    _restore_item_text(child)


def _best_overlapping_text_item(mirrored_item: dict, original_items: list[dict], used_refs: set[str]) -> dict | None:
    mirrored_prov = _first_provenance(mirrored_item)
    mirrored_bbox = mirrored_prov.get("bbox")
    if not isinstance(mirrored_bbox, dict):
        return None

    best_score = 0.0
    best_item = None
    for original_item in original_items:
        original_ref = original_item.get("self_ref")
        if isinstance(original_ref, str) and original_ref in used_refs:
            continue

        original_prov = _first_provenance(original_item)
        if original_prov.get("page_no") != mirrored_prov.get("page_no"):
            continue
        original_bbox = original_prov.get("bbox")
        if not isinstance(original_bbox, dict):
            continue

        score = _bbox_iou(mirrored_bbox, original_bbox)
        if score > best_score:
            best_score = score
            best_item = original_item

    return best_item if best_score >= 0.08 else None


def _overlaps_used_original_item(mirrored_item: dict, used_original_items: list[dict]) -> bool:
    mirrored_prov = _first_provenance(mirrored_item)
    mirrored_bbox = mirrored_prov.get("bbox")
    if not isinstance(mirrored_bbox, dict):
        return False

    for original_item in used_original_items:
        original_prov = _first_provenance(original_item)
        if original_prov.get("page_no") != mirrored_prov.get("page_no"):
            continue
        original_bbox = original_prov.get("bbox")
        if isinstance(original_bbox, dict) and _bbox_iou(mirrored_bbox, original_bbox) >= 0.08:
            return True
    return False


def _first_provenance(item: dict) -> dict:
    provenance = item.get("prov", [])
    if not provenance or not isinstance(provenance[0], dict):
        return {}
    return provenance[0]


def _bbox_iou(first: dict, second: dict) -> float:
    left = max(float(first.get("l", 0)), float(second.get("l", 0)))
    right = min(float(first.get("r", 0)), float(second.get("r", 0)))
    bottom = max(float(first.get("b", 0)), float(second.get("b", 0)))
    top = min(float(first.get("t", 0)), float(second.get("t", 0)))
    intersection = max(0.0, right - left) * max(0.0, top - bottom)
    if intersection == 0:
        return 0.0

    first_area = _bbox_area(first)
    second_area = _bbox_area(second)
    union = first_area + second_area - intersection
    return intersection / union if union else 0.0


def _bbox_area(bbox: dict) -> float:
    width = max(0.0, float(bbox.get("r", 0)) - float(bbox.get("l", 0)))
    height = max(0.0, float(bbox.get("t", 0)) - float(bbox.get("b", 0)))
    return width * height


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
