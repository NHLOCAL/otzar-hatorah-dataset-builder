from __future__ import annotations

import re

from .text import contains_hebrew


HEBREW_BLOCK = "\u0590-\u05FF"
HEBREW_MARKS = "\u0591-\u05C7"
RTL_CONTROL_CHARS = re.compile("[\u200e\u200f\u202a-\u202e\u2066-\u2069]")
TEXT_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
SUSPECT_MOJIBAKE_SPAN = re.compile(rf"[^{HEBREW_BLOCK}A-Za-z0-9\n]{{4,}}")
MIRRORED_BRACKET_PAIRS = (
    (")", "(", "(", ")"),
    ("]", "[", "[", "]"),
    ("}", "{", "{", "}"),
    (">", "<", "<", ">"),
    ("）", "（", "（", "）"),
    ("］", "［", "［", "］"),
    ("｝", "｛", "｛", "｝"),
    ("〉", "〈", "〈", "〉"),
    ("»", "«", "«", "»"),
)
MOJIBAKE_SOURCE_ENCODINGS = ("latin1", "cp1252", "mac_roman")
HEBREW_TARGET_ENCODINGS = ("utf-8", "cp1255", "iso8859_8")
MOJIBAKE_MARKERS = set(
    "×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ�"
    "˘˜¯‰‡·‚„ÂÊÁËÈÍÎÏÌÓÒÙÚÛÙ˙"
)
COMMON_HEBREW_WORDS = {
    "את",
    "עם",
    "על",
    "כי",
    "כל",
    "לא",
    "וכן",
    "בלי",
    "שלום",
    "עולם",
}
FINAL_HEBREW_LETTERS = set("ךםןףץ")


def normalize_hebrew_text(text: str) -> str:
    if not text:
        return text

    normalized = _repair_mojibake(text)
    normalized = RTL_CONTROL_CHARS.sub("", normalized)
    normalized = TEXT_CONTROL_CHARS.sub("", normalized)
    normalized = _clean_split_quote_fragment(normalized)
    if contains_hebrew(normalized):
        normalized = _align_mirrored_brackets(normalized)
        normalized = _join_artificially_spaced_marked_words(normalized)

    normalized = _clean_punctuation_spacing(normalized)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    return normalized.strip()


def _clean_split_quote_fragment(text: str) -> str:
    stripped = text.strip()
    if stripped == '"':
        return ""
    if len(stripped) <= 80 and stripped.count('"') == 1:
        return stripped.strip('"').strip()
    return text


def _repair_mojibake(text: str) -> str:
    best = _best_mojibake_repair(text)
    segmented = SUSPECT_MOJIBAKE_SPAN.sub(_repair_mojibake_span, best)
    if _hebrew_quality_score(segmented) > _hebrew_quality_score(best):
        return segmented
    return best


def _repair_mojibake_span(match: re.Match[str]) -> str:
    span = match.group(0)
    if not _looks_like_mojibake(span):
        return span

    repaired = _best_mojibake_repair(span)
    if _hebrew_quality_score(repaired) > _hebrew_quality_score(span):
        return repaired
    return span


def _looks_like_mojibake(text: str) -> bool:
    marker_chars = sum(1 for char in text if char in MOJIBAKE_MARKERS)
    high_chars = sum(1 for char in text if ord(char) > 127 and not ("\u0590" <= char <= "\u05ff"))
    control_chars = sum(1 for char in text if char not in "\n\t" and ord(char) < 32)
    return marker_chars >= 2 or high_chars >= 4 or control_chars > 0


def _best_mojibake_repair(text: str) -> str:
    best = text
    best_score = _hebrew_quality_score(text)

    for raw in _mojibake_byte_candidates(text):
        for target_encoding in HEBREW_TARGET_ENCODINGS:
            try:
                decoded = raw.decode(target_encoding)
            except UnicodeDecodeError:
                continue

            for candidate in (decoded, _reverse_lines(decoded)):
                candidate_score = _hebrew_quality_score(candidate)
                if candidate_score > best_score:
                    best = candidate
                    best_score = candidate_score

    return best


def _mojibake_byte_candidates(text: str) -> list[bytes]:
    candidates: list[bytes] = []
    seen: set[bytes] = set()

    for source_encoding in MOJIBAKE_SOURCE_ENCODINGS:
        try:
            raw = text.encode(source_encoding)
        except UnicodeEncodeError:
            continue
        if raw not in seen:
            candidates.append(raw)
            seen.add(raw)

    raw = _encode_cp1252_preserving_c1_controls(text)
    if raw is not None and raw not in seen:
        candidates.append(raw)

    return candidates


def _encode_cp1252_preserving_c1_controls(text: str) -> bytes | None:
    raw = bytearray()

    for char in text:
        codepoint = ord(char)
        if codepoint <= 0xFF:
            raw.append(codepoint)
            continue
        try:
            raw.extend(char.encode("cp1252"))
        except UnicodeEncodeError:
            return None

    return bytes(raw)


def _hebrew_quality_score(text: str) -> int:
    words = re.findall(rf"[{HEBREW_BLOCK}]+", text)
    hebrew_chars = sum(1 for char in text if "\u0590" <= char <= "\u05ff")
    mojibake_chars = sum(1 for char in text if char in MOJIBAKE_MARKERS)
    replacement_chars = text.count("\ufffd")
    control_chars = sum(1 for char in text if char not in "\n\t" and ord(char) < 32)
    impossible_word_starts = sum(1 for word in words if word and word[0] in FINAL_HEBREW_LETTERS)
    common_words = sum(1 for word in words if word in COMMON_HEBREW_WORDS)

    return (
        hebrew_chars * 10
        + len(words) * 4
        + common_words * 12
        - impossible_word_starts * 20
        - mojibake_chars * 15
        - replacement_chars * 50
        - control_chars * 20
    )


def _reverse_lines(text: str) -> str:
    return "\n".join(line[::-1] for line in text.split("\n"))


def _align_mirrored_brackets(text: str) -> str:
    aligned = text
    for mirrored_open, mirrored_close, open_bracket, close_bracket in MIRRORED_BRACKET_PAIRS:
        pattern = re.compile(
            rf"{re.escape(mirrored_open)}([^{re.escape(mirrored_open + mirrored_close)}\n]{{1,160}})"
            rf"{re.escape(mirrored_close)}"
        )
        aligned = pattern.sub(
            lambda match, open_bracket=open_bracket, close_bracket=close_bracket: (
                f"{open_bracket}{match.group(1).strip()}{close_bracket}"
            ),
            aligned,
        )
    return aligned


def _join_artificially_spaced_marked_words(text: str) -> str:
    pattern = re.compile(rf"(?<=[{HEBREW_MARKS}])\s+(?=[{HEBREW_BLOCK}])")
    return pattern.sub("", text)


def _clean_punctuation_spacing(text: str) -> str:
    cleaned = text
    cleaned = re.sub(r'"\s*([^"\n]*?)\s*"', lambda match: f'"{match.group(1).strip()}"', cleaned)
    cleaned = re.sub(rf'(?<=[{HEBREW_BLOCK}"])[ \t]*(?=[\[({{<])', " ", cleaned)
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    cleaned = re.sub(r"([,.;:!?])(?=\S)", r"\1 ", cleaned)
    cleaned = re.sub(r"([\[({<])\s+", r"\1", cleaned)
    cleaned = re.sub(r"\s+([\])}>])", r"\1", cleaned)
    cleaned = re.sub(r"(?<=[\])}>])(?=[\[(<{])", " ", cleaned)
    return cleaned
