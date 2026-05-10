from __future__ import annotations

import re

from .text import contains_hebrew


HEBREW_BLOCK = "\u0590-\u05FF"
HEBREW_MARKS = "\u0591-\u05C7"
RTL_CONTROL_CHARS = re.compile("[\u200e\u200f\u202a-\u202e\u2066-\u2069]")


def normalize_hebrew_text(text: str) -> str:
    if not text:
        return text

    normalized = RTL_CONTROL_CHARS.sub("", text)
    normalized = _clean_split_quote_fragment(normalized)
    if contains_hebrew(normalized):
        normalized = _swap_parentheses(normalized)
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


def _swap_parentheses(text: str) -> str:
    return text.translate(str.maketrans({"(": ")", ")": "("}))


def _join_artificially_spaced_marked_words(text: str) -> str:
    previous = None
    current = text
    pattern = re.compile(rf"(?<=[{HEBREW_MARKS}])\s+(?=[{HEBREW_BLOCK}])")
    while current != previous:
        previous = current
        current = pattern.sub("", current)
    return current


def _clean_punctuation_spacing(text: str) -> str:
    cleaned = text
    cleaned = re.sub(r'"\s*([^"\n]*?)\s*"', lambda match: f'"{match.group(1).strip()}"', cleaned)
    cleaned = re.sub(rf'(?<=[{HEBREW_BLOCK}"]) (?=[\[(])', " ", cleaned)
    cleaned = re.sub(rf'(?<=[{HEBREW_BLOCK}"])(?=[\[(])', " ", cleaned)
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    cleaned = re.sub(r"([,.;:!?])(?=\S)", r"\1 ", cleaned)
    cleaned = re.sub(r"([\[(])\s+", r"\1", cleaned)
    cleaned = re.sub(r"\s+([\])])", r"\1", cleaned)
    return cleaned
