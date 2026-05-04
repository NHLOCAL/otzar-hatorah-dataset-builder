from __future__ import annotations

import unittest

from alonim.text import clean_path_part, contains_hebrew


class TextTests(unittest.TestCase):
    def test_clean_path_part_replaces_windows_invalid_characters(self) -> None:
        self.assertEqual(clean_path_part(' א/ב:ג*ד? '), "א_ב_ג_ד_")

    def test_clean_path_part_truncates_long_values(self) -> None:
        cleaned = clean_path_part("א" * 100, max_length=12)
        self.assertEqual(len(cleaned), 12)
        self.assertTrue(cleaned.endswith("..."))

    def test_contains_hebrew(self) -> None:
        self.assertTrue(contains_hebrew("Bulletin עברית"))
        self.assertFalse(contains_hebrew("Bulletin"))


if __name__ == "__main__":
    unittest.main()
