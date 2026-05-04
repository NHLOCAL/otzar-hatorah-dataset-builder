from __future__ import annotations

import re


HEBREW_RANGE = r"\u0590-\u05FF"
WINDOWS_INVALID_PATH_CHARS = re.compile(r'[\\/*?:"<>|\n\r]+')


def clean_path_part(text: str | None, max_length: int = 80) -> str:
    """Return a Windows-safe file or directory name without changing RTL order."""
    if not text:
        return ""

    cleaned = WINDOWS_INVALID_PATH_CHARS.sub("_", text.strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip().rstrip(". ")
    if max_length > 0 and len(cleaned) > max_length:
        cleaned = cleaned[: max_length - 3].rstrip(". ") + "..."
    return cleaned


def contains_hebrew(text: str | None) -> bool:
    return bool(re.search(f"[{HEBREW_RANGE}]", text or ""))


def reverse_hebrew_words_naive(text: str) -> str:
    if not text:
        return text
    pattern = re.compile(rf"[{HEBREW_RANGE}]+(?:[{HEBREW_RANGE}\-\u05BE\u05F3\u05F4]+)*")
    return pattern.sub(lambda match: match.group(0)[::-1], text)


def display_hebrew(text: str) -> str:
    """Format Hebrew for terminals that render RTL poorly.

    This is display-only. Never use it for filenames or stored dataset text.
    """
    if not text or not contains_hebrew(text):
        return text

    try:
        from bidi.algorithm import get_display

        return get_display(text)
    except Exception:
        return reverse_hebrew_words_naive(text)
