from __future__ import annotations

from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[2]

BASE_URL = "https://beinenu.com"
ALONIM_PAGE_URL = f"{BASE_URL}/alonim"

DEFAULT_PDF_DIR = PROJECT_DIR / "source_data" / "pdf"
DEFAULT_SAMPLES_DIR = PROJECT_DIR / "source_data" / "samples"
DEFAULT_DOCLING_JSON_DIR = PROJECT_DIR / "intermediate" / "docling_json"
DEFAULT_MARKDOWN_DIR = PROJECT_DIR / "intermediate" / "markdown"
DEFAULT_LOG_DIR = PROJECT_DIR / "intermediate" / "logs"
DEFAULT_OUTPUT_PARQUET_DIR = PROJECT_DIR / "output_parquet"

DEFAULT_PARQUET_FILE = "alonim_dataset.parquet"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}
