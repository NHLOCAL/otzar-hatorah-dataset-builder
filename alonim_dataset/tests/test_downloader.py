from __future__ import annotations

import unittest

from alonim.downloader import BulletinItem, YearOption, parse_year_selection, sorted_year_options


class DownloaderTests(unittest.TestCase):
    def test_parse_year_selection_accepts_ranges_and_lists(self) -> None:
        years = [YearOption("3", "תשפג"), YearOption("2", "תשפב"), YearOption("1", "תשפא")]
        selected = parse_year_selection("1,3", years)
        self.assertEqual([year.value for year in selected], ["3", "1"])

    def test_parse_year_selection_accepts_all(self) -> None:
        years = [YearOption("2", "תשפב"), YearOption("1", "תשפא")]
        self.assertEqual(parse_year_selection("all", years), years)

    def test_sorted_year_options_descending_by_value(self) -> None:
        sorted_years = sorted_year_options([YearOption("1", "א"), YearOption("3", "ג"), YearOption("2", "ב")])
        self.assertEqual([year.value for year in sorted_years], ["3", "2", "1"])

    def test_bulletin_relative_pdf_path_keeps_data_hierarchy(self) -> None:
        item = BulletinItem(
            title='עלון/מספר: 1',
            download_url="https://example.test/a.pdf",
            year="תשפד",
            main_topic="הלכה",
            specific_subject="שבת",
        )
        self.assertEqual(item.relative_pdf_path().as_posix(), "תשפד/הלכה/שבת/עלון_מספר_ 1.pdf")


if __name__ == "__main__":
    unittest.main()
