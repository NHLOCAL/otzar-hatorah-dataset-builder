from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from alonim.rtl_normalize import normalize_hebrew_text


class RtlNormalizeTests(unittest.TestCase):
    def test_normalize_hebrew_text_joins_artificially_spaced_niqqud_words(self) -> None:
        text = "דַּ בֵּר אֶ ל בְּ נֵּי יִשְּ רָ אֵּ ל"

        self.assertEqual(normalize_hebrew_text(text), "דַּבֵּר אֶל בְּנֵּי יִשְּרָאֵּל")

    def test_normalize_hebrew_text_cleans_spacing_around_punctuation(self) -> None:
        text = ' " אדם " ) א\', ב\' ( , וכן '

        self.assertEqual(normalize_hebrew_text(text), '"אדם" (א\', ב\'), וכן')

    def test_normalize_hebrew_text_preserves_newline_before_opening_punctuation(self) -> None:
        text = '"title"\n(note)'

        self.assertEqual(normalize_hebrew_text(text), '"title"\n(note)')

    def test_normalize_hebrew_text_drops_short_split_quote_fragments(self) -> None:
        self.assertEqual(normalize_hebrew_text('"'), "")
        self.assertEqual(normalize_hebrew_text('מתיקות הפרשה"'), "מתיקות הפרשה")


if __name__ == "__main__":
    unittest.main()
