from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import pdf_to_json  # noqa: E402


class PdfToJsonCliTests(unittest.TestCase):
    def test_rtl_mirror_input_is_enabled_by_default(self) -> None:
        with (
            patch.object(sys, "argv", ["pdf_to_json.py", "input.pdf"]),
            patch.object(pdf_to_json, "convert_pdf_to_docling_json", return_value=Path("out.json")) as convert,
        ):
            pdf_to_json.main()

        convert.assert_called_once_with(Path("input.pdf"), rtl_mirror_input=True)

    def test_no_rtl_mirror_input_explicitly_disables_mirror_pipeline(self) -> None:
        with (
            patch.object(sys, "argv", ["pdf_to_json.py", "input.pdf", "--no-rtl-mirror-input"]),
            patch.object(pdf_to_json, "convert_pdf_to_docling_json", return_value=Path("out.json")) as convert,
        ):
            pdf_to_json.main()

        convert.assert_called_once_with(Path("input.pdf"), rtl_mirror_input=False)


if __name__ == "__main__":
    unittest.main()
