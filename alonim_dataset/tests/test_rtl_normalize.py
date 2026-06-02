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

    def test_normalize_hebrew_text_aligns_common_bracket_pairs(self) -> None:
        text = " }פסוק{ ]מקור[ >ביאור< )הערה( "

        self.assertEqual(normalize_hebrew_text(text), "{פסוק} [מקור] <ביאור> (הערה)")

    def test_normalize_hebrew_text_aligns_inline_source_parentheses(self) -> None:
        text = "נאמר בישעיה )יא,יב(, והדברים מבוארים"

        self.assertEqual(normalize_hebrew_text(text), "נאמר בישעיה (יא, יב), והדברים מבוארים")

    def test_normalize_hebrew_text_preserves_newline_before_opening_punctuation(self) -> None:
        text = '"title"\n(note)'

        self.assertEqual(normalize_hebrew_text(text), '"title"\n(note)')

    def test_normalize_hebrew_text_drops_short_split_quote_fragments(self) -> None:
        self.assertEqual(normalize_hebrew_text('"'), "")
        self.assertEqual(normalize_hebrew_text('מתיקות הפרשה"'), "מתיקות הפרשה")

    def test_normalize_hebrew_text_repairs_windows_hebrew_mojibake(self) -> None:
        text = "ùìåí òåìí"

        self.assertEqual(normalize_hebrew_text(text), "שלום עולם")

    def test_normalize_hebrew_text_repairs_utf8_mojibake(self) -> None:
        text = "\u00d7\u00a9\u00d7\u009c\u00d7\u2022\u00d7\u009d \u00d7\u00a2\u00d7\u2022\u00d7\u009c\u00d7\u009d"

        self.assertEqual(normalize_hebrew_text(text), "שלום עולם")

    def test_normalize_hebrew_text_repairs_reversed_mac_roman_hebrew_mojibake(self) -> None:
        text = "˜ÒÙ‰ ÈÏ· Â·¯˙ÈÂ Â„ÈÓ˙È Í˙¯Â˙·Â Í˙·‰‡· Â·Ï ˙Â¯¯ÂÚ˙‰ ÈÙ˘¯Â"

        self.assertEqual(
            normalize_hebrew_text(text),
            "ורשפי התעוררות לבו באהבתך ובתורתך יתמידו ויתרבו בלי הפסק",
        )

    def test_normalize_hebrew_text_repairs_mojibake_segment_inside_hebrew_text(self) -> None:
        text = (
            ",'˜ÒÙ‰ ÈÏ· Â·¯˙ÈÂ Â„ÈÓ˙È Í˙¯Â˙·Â Í˙·‰‡· Â\x0e·Ï ˙Â¯¯ÂÚ˙‰ ÈÙ˘¯Â' "
            "אומרים אנו תשליך לאחר א\"החיד"
        )

        self.assertEqual(
            normalize_hebrew_text(text),
            "'ורשפי התעוררות לבו באהבתך ובתורתך יתמידו ויתרבו בלי הפסק', "
            'אומרים אנו תשליך לאחר א"החיד',
        )


if __name__ == "__main__":
    unittest.main()
