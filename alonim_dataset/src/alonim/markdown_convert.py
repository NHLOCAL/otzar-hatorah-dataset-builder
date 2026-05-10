from __future__ import annotations

import json
import re
from pathlib import Path

from .config import DEFAULT_MARKDOWN_DIR
from .profiles import BulletinProfile, profile_for_source
from .rtl_normalize import normalize_hebrew_text


def clean_ocr_text(text: str) -> str:
    return normalize_hebrew_text(text) if isinstance(text, str) else text


def process_docling_document(data: dict) -> dict[int, list[dict]]:
    pages_data: dict[int, list[dict]] = {}
    element_map = {
        item["self_ref"]: item
        for list_key in ("texts", "pictures")
        for item in data.get(list_key, [])
        if "self_ref" in item
    }
    body_children_refs = [child.get("$ref") for child in data.get("body", {}).get("children", [])]
    processed_refs: set[str] = set()

    for ref_path in body_children_refs:
        if not ref_path or ref_path in processed_refs:
            continue

        item = element_map.get(ref_path)
        if not item:
            continue

        if item.get("label") == "picture" and "children" in item:
            for child_ref in item.get("children", []):
                child_item = element_map.get(child_ref.get("$ref"))
                if child_item and "text" in child_item:
                    page_no = _page_number(child_item)
                    pages_data.setdefault(page_no, [])
                    if child_item.get("self_ref") not in {element.get("self_ref") for element in pages_data[page_no]}:
                        pages_data[page_no].append(child_item)
                    processed_refs.add(child_item.get("self_ref", ""))
            processed_refs.add(ref_path)
            continue

        page_no = _page_number(item)
        pages_data.setdefault(page_no, [])
        if ref_path not in {element.get("self_ref") for element in pages_data[page_no]}:
            pages_data[page_no].append(item)
        processed_refs.add(ref_path)

    return pages_data


def convert_docling_json_to_markdown(json_path: Path, output_path: Path | None = None) -> Path:
    json_path = json_path.expanduser().resolve()
    if not json_path.is_file():
        raise FileNotFoundError(f"Source JSON was not found: {json_path}")

    if output_path is None:
        output_path = _default_markdown_output_path(json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    data = json.loads(json_path.read_text(encoding="utf-8"))
    profile = profile_for_source(json_path)
    pages_data = process_docling_document(data)
    content: list[str] = []

    for page_no in sorted(pages_data):
        page_elements = pages_data[page_no]
        page_size = data.get("pages", {}).get(str(page_no), {}).get("size", {})
        page_width = page_size.get("width", 600)
        body_items, footnotes = sort_page_elements(page_elements, page_width, profile)

        content.append(f"\n---\n\n<!-- Page {page_no} -->\n\n")
        content.extend(markdown_for_item(item) for item in body_items)

        if footnotes:
            content.append("---\n\n")
            for item in footnotes:
                content.append(f"*{clean_ocr_text(item.get('text', ''))}*\n\n")

    final_output = "".join(content)
    final_output = re.sub(r"(?<!\n)\n(?!\n|#|\*|>)", " ", final_output)
    final_output = re.sub(r" +", " ", final_output)
    output_path.write_text(final_output, encoding="utf-8", newline="\n")
    return output_path


def sort_page_elements(
    page_elements: list[dict],
    page_width: float,
    profile: BulletinProfile = BulletinProfile.DEFAULT,
) -> tuple[list[dict], list[dict]]:
    if not page_width or not page_elements:
        return page_elements, []

    right_col: list[dict] = []
    left_col: list[dict] = []
    interrupting_blocks: list[dict] = []
    footnotes: list[dict] = []
    center_margin = page_width * 0.1
    page_center = page_width / 2
    top_threshold = _top_header_threshold(page_elements)

    for item in page_elements:
        label = item.get("label", "")
        if label in {"page_footer", "page_header"}:
            continue
        if label == "footnote":
            footnotes.append(item)
            continue

        try:
            bbox = item["prov"][0]["bbox"]
            item_left = bbox["l"]
            item_right = bbox["r"]
            item_width = item_right - item_left
        except (KeyError, IndexError, TypeError):
            interrupting_blocks.append(item)
            continue

        if (
            item.get("is_framed")
            or item_width > page_width * 0.65
            or _is_centered_block(item, page_center)
            or (profile == BulletinProfile.METIKUT_HAPARSHA and _is_top_masthead(item, top_threshold))
        ):
            interrupting_blocks.append(item)
        elif item_right < page_center + center_margin:
            left_col.append(item)
        elif item_left > page_center - center_margin:
            right_col.append(item)
        else:
            interrupting_blocks.append(item)

    sort_key = lambda item: item.get("prov", [{}])[0].get("bbox", {}).get("t", 0)
    for group in (interrupting_blocks, right_col, left_col, footnotes):
        group.sort(key=sort_key, reverse=True)

    if profile == BulletinProfile.BIRKAT_YITZCHAK:
        body_items = interrupting_blocks + right_col + left_col
        body_items.sort(key=sort_key, reverse=True)
    elif profile in {BulletinProfile.MORDECHAI_BLASS, BulletinProfile.METIKUT_HAPARSHA}:
        body_items = interrupting_blocks + right_col + left_col
    else:
        body_items = interrupting_blocks + right_col + left_col
        body_items.sort(key=sort_key, reverse=True)
    return body_items, footnotes


def markdown_for_item(item: dict) -> str:
    text = clean_ocr_text(item.get("text", ""))
    if not text:
        return ""

    label = item.get("label", "text")
    if item.get("is_framed"):
        return f"> {text}\n\n"
    if label == "section_header":
        level = item.get("level", 1)
        return f"\n{'#' * (level + 1)} {text}\n\n"
    if label == "list_item":
        return f"* {text}\n"
    return f"{text}\n\n"


def _page_number(item: dict) -> int:
    try:
        return int(item["prov"][0]["page_no"])
    except (KeyError, IndexError, TypeError, ValueError):
        return 1


def _is_centered_block(item: dict, page_center: float) -> bool:
    try:
        bbox = item["prov"][0]["bbox"]
        return bbox["l"] < page_center < bbox["r"]
    except (KeyError, IndexError, TypeError):
        return False


def _top_header_threshold(page_elements: list[dict]) -> float | None:
    tops: list[float] = []
    for item in page_elements:
        try:
            tops.append(float(item["prov"][0]["bbox"]["t"]))
        except (KeyError, IndexError, TypeError, ValueError):
            continue
    if not tops:
        return None
    return max(tops) - 80


def _is_top_masthead(item: dict, top_threshold: float | None) -> bool:
    if top_threshold is None:
        return False
    try:
        return float(item["prov"][0]["bbox"]["t"]) >= top_threshold
    except (KeyError, IndexError, TypeError, ValueError):
        return False


def _default_markdown_output_path(json_path: Path) -> Path:
    parts = json_path.parts
    if "docling_json" not in parts:
        return DEFAULT_MARKDOWN_DIR / f"{json_path.stem}.md"

    relative_parts = parts[parts.index("docling_json") + 1 :]
    if not relative_parts:
        return DEFAULT_MARKDOWN_DIR / f"{json_path.stem}.md"
    return DEFAULT_MARKDOWN_DIR.joinpath(*relative_parts).with_suffix(".md")
