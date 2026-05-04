from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from .config import ALONIM_PAGE_URL, BASE_URL, DEFAULT_HEADERS, DEFAULT_PDF_DIR
from .text import clean_path_part


LogFn = Callable[[str], None]


@dataclass(frozen=True)
class YearOption:
    value: str
    label: str


@dataclass(frozen=True)
class BulletinItem:
    title: str
    download_url: str
    year: str
    main_topic: str = ""
    specific_subject: str = ""

    def relative_pdf_path(self) -> Path:
        parts = [clean_path_part(self.year), clean_path_part(self.main_topic), clean_path_part(self.specific_subject)]
        safe_parts = [part for part in parts if part]
        return Path(*safe_parts) / f"{clean_path_part(self.title)}.pdf"


@dataclass(frozen=True)
class DownloadConfig:
    output_dir: Path = DEFAULT_PDF_DIR
    delay_seconds: float = 0.8
    timeout_seconds: int = 30
    max_retries: int = 5
    skip_existing: bool = True


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def get_year_options(session: requests.Session) -> list[YearOption]:
    response = _get(session, ALONIM_PAGE_URL)
    soup = BeautifulSoup(response.content, "html.parser")
    year_select = soup.find("select", {"id": "edit-field-year-value-many-to-one"})
    if not year_select:
        return []

    return [
        YearOption(value=option["value"], label=option.text.strip())
        for option in year_select.find_all("option")
        if option.get("value") and option.get("value") != "All"
    ]


def sorted_year_options(years: Iterable[YearOption]) -> list[YearOption]:
    return sorted(years, key=lambda year: year.value, reverse=True)


def parse_year_selection(selection: str, sorted_years: list[YearOption]) -> list[YearOption]:
    value = (selection or "").strip().lower()
    if value in {"*", "all", "הכל"}:
        return sorted_years

    indices: set[int] = set()
    for part in filter(None, value.replace(",", " ").split()):
        if "-" in part:
            start_text, end_text = part.split("-", 1)
            start, end = int(start_text) - 1, int(end_text) - 1
            if start > end:
                start, end = end, start
            indices.update(range(start, end + 1))
        else:
            indices.add(int(part) - 1)

    if not indices:
        raise ValueError("No years were selected.")

    max_index = len(sorted_years) - 1
    invalid = [index + 1 for index in indices if index < 0 or index > max_index]
    if invalid:
        raise ValueError(f"Invalid year selection index: {', '.join(map(str, invalid))}")

    return [sorted_years[index] for index in sorted(indices)]


def iter_bulletins_for_year(
    session: requests.Session,
    year: YearOption,
    *,
    delay_seconds: float = 0.8,
) -> Iterable[BulletinItem]:
    url = f"{ALONIM_PAGE_URL}?field_year_value_many_to_one={year.value}"

    while url:
        response = _get(session, url)
        soup = BeautifulSoup(response.content, "html.parser")
        if soup.select_one(".view-empty"):
            return

        for item in soup.select(".views-view-grid tr td"):
            title_tag = item.select_one(".views-field-title-1 a")
            link_tag = item.select_one("a.bulletin-link")
            if not (title_tag and link_tag and link_tag.get("href")):
                continue

            main_topic_tag = item.select_one(".subject_dic")
            specific_subject_tag = item.select_one(".views-field-field-summary .part-summary")

            yield BulletinItem(
                title=title_tag.text.strip(),
                download_url=urljoin(BASE_URL, link_tag["href"]),
                year=year.label,
                main_topic=(main_topic_tag.text.rstrip(",").strip() if main_topic_tag else ""),
                specific_subject=(specific_subject_tag.text.strip() if specific_subject_tag else ""),
            )

        next_tag = soup.select_one("li.pager-next a")
        url = urljoin(BASE_URL, next_tag["href"]) if next_tag and next_tag.get("href") else None
        if url:
            time.sleep(delay_seconds)


def download_years(
    years: Iterable[YearOption],
    *,
    session: requests.Session | None = None,
    config: DownloadConfig = DownloadConfig(),
    log: LogFn | None = print,
) -> list[Path]:
    session = session or make_session()
    downloaded: list[Path] = []

    for year in years:
        _log(log, f"Processing year: {year.label} ({year.value})")
        for bulletin in iter_bulletins_for_year(session, year, delay_seconds=config.delay_seconds):
            path = download_bulletin(session, bulletin, config=config, log=log)
            if path is not None:
                downloaded.append(path)
        time.sleep(config.delay_seconds)

    return downloaded


def download_bulletin(
    session: requests.Session,
    bulletin: BulletinItem,
    *,
    config: DownloadConfig = DownloadConfig(),
    log: LogFn | None = print,
) -> Path | None:
    target_path = config.output_dir / bulletin.relative_pdf_path()
    if config.skip_existing and target_path.exists():
        _log(log, f"Exists: {target_path}")
        return None

    target_path.parent.mkdir(parents=True, exist_ok=True)
    response = _get(
        session,
        bulletin.download_url,
        stream=True,
        timeout=config.timeout_seconds,
        max_retries=config.max_retries,
    )

    with response:
        with target_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    handle.write(chunk)

    _log(log, f"Downloaded: {target_path}")
    return target_path


def _get(
    session: requests.Session,
    url: str,
    *,
    stream: bool = False,
    timeout: int = 30,
    max_retries: int = 5,
) -> requests.Response:
    last_error: requests.RequestException | None = None
    for attempt in range(max_retries):
        try:
            response = session.get(url, stream=stream, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            last_error = error
            status_code = getattr(getattr(error, "response", None), "status_code", None)
            should_retry = (
                status_code is not None and status_code >= 500
            ) or status_code in {403, 418, 429} or isinstance(
                error,
                (requests.exceptions.ConnectionError, requests.exceptions.Timeout),
            )
            if not should_retry or attempt >= max_retries - 1:
                break
            time.sleep(min(2**attempt, 300))

    raise RuntimeError(f"Failed to fetch {url}") from last_error


def _log(log: LogFn | None, message: str) -> None:
    if log is not None:
        log(message)
