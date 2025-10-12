#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Alonim downloader with a Rich CLI and Hebrew 'flip-on-print' for Windows CMD.

שינויים מרכזיים:
- הוספת פונקציית heb() שמבצעת היפוך להצגה עבור טקסט עברי בלבד.
- שימוש ב-rich ללא markup בתוך המחרוזות (כדי לא לשבור תגיות), סטיילים מועברים בפרמטר style.
- כל הדפסה/טקסט מוצג עוברת דרך heb(), אבל שמות קבצים/תיקיות בפועל נשמרים לא הפוכים בדיסק!
"""

from __future__ import annotations
import os
import re
import time
from typing import Dict, List, Tuple

import requests
from bs4 import BeautifulSoup

# --- Rich ---
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt
from rich.progress import (
    Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn,
    TimeRemainingColumn, DownloadColumn, TransferSpeedColumn
)
from rich.traceback import install as rich_traceback_install

# --- BIDI (אופציונלי) ---
try:
    from bidi.algorithm import get_display as bidi_get_display
    _HAS_BIDI = True
except Exception:
    _HAS_BIDI = False

# Tracebacks יפים
rich_traceback_install(show_locals=False)
# חשוב: markup=False כדי שלא נהפוך סוגריים מרובעים של Rich
console = Console(highlight=False, markup=False)

# --------------------------- קבועים ---------------------------

BASE_URL = "https://beinenu.com"
ALONIM_PAGE_URL = f"{BASE_URL}/alonim"
DOWNLOAD_DIR = "Downloaded_Alonim"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}

# --------------------------- BIDI Helpers ---------------------------

_HEB_RANGE = r"\u0590-\u05FF"

def _contains_hebrew(s: str) -> bool:
    return bool(re.search(f"[{_HEB_RANGE}]", s or ""))

def _reverse_hebrew_words_naive(s: str) -> str:
    """
    גיבוי כשאין python-bidi:
    מהפך רק ריצות של עברית (מילים/רצפים), משאיר אנגלית/מספרים כמו שהם.
    """
    if not s:
        return s
    # רצף 'מילה עברית' כולל אותיות עבריות + גרשיים/מקף
    pattern = re.compile(rf"[{_HEB_RANGE}]+(?:[{_HEB_RANGE}\-\u05BE\u05F3\u05F4]+)*")
    def _flip(m):
        seg = m.group(0)
        return seg[::-1]
    return pattern.sub(_flip, s)

def heb(s: str) -> str:
    """
    מציג טקסט 'הפוך על הפוך' עבור עברית כדי ש-CMD יראה תקין.
    - אם יש python-bidi: נעשה רה-סידור מלא (עדיף).
    - אחרת: נהפוך רק מילים עבריות כדי לא לפגוע באנגלית/מספרים.
    חשוב: בשימוש רק להצגה! לא לשמות קבצים/תיקיות בפועל.
    """
    if not s or not _contains_hebrew(s):
        return s
    if _HAS_BIDI:
        # bidi ינסה לסדר את הטקסט כולו נכון ויזואלית
        try:
            return bidi_get_display(s)
        except Exception:
            pass
    # גיבוי נאיבי
    return _reverse_hebrew_words_naive(s)

def hpanel(title: str, subtitle: str | None = None, border_style: str = "cyan") -> Panel:
    text = heb(title) if title else ""
    if subtitle:
        text = f"{text}\n{heb(subtitle)}"
    return Panel.fit(text, border_style=border_style, padding=(1, 2))

# --------------------------- כלי עזר ---------------------------

def clean_name(text: str) -> str:

    """ניקוי שם מתווים לא חוקיים ותיקון אורך, ללא היפוך."""

    if not text:

        return ""

    # החלפת תווים לא חוקיים ויישור שורות

    cleaned_text = re.sub(r'[\\/*?:"<>|\n\r]+', "_", text.strip())

    # קיצוץ למניעת חריגה ממגבלת אורך הנתיב של מערכת ההפעלה

    if len(cleaned_text) > 60:

        # Truncate

        cleaned_text = cleaned_text[:57]

        # Remove trailing dots or spaces that are invalid at the end of a directory name

        cleaned_text = cleaned_text.rstrip('. ')

        # Add ellipsis to indicate it was shortened

        cleaned_text += "..."

    return cleaned_text





def make_request(

    session: requests.Session,

    url: str,

    stream: bool = False,

    max_retries: int = 5,

    **kwargs,

) -> requests.Response | None:

    """

    Makes a GET request with retries and exponential backoff.

    If streaming, the caller is responsible for closing the response.

    """

    last_exception = None

    for attempt in range(max_retries):

        try:

            resp = session.get(url, stream=stream, **kwargs)

            resp.raise_for_status()

            return resp

        except requests.RequestException as e:

            last_exception = e

            wait_time = min(2 ** attempt, 300)  # Exponential backoff, max 5 minutes



            status_code = getattr(getattr(e, 'response', None), 'status_code', None)



            should_retry = (

                status_code in {403, 418, 429} or

                status_code >= 500 or

                isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout))

            )



            if should_retry and attempt < max_retries - 1:

                console.log(

                    heb(f"שגיאת רשת (קוד: {status_code or 'N/A'}), ניסיון חוזר בעוד {wait_time} שניות... ({attempt + 1}/{max_retries})"),

                    style="yellow"

                )

                time.sleep(wait_time)

            else:

                break # Give up



    if last_exception:

        status_code = getattr(getattr(last_exception, 'response', None), 'status_code', None)

        error_msg = heb(f"שגיאת רשת מתמשכת (קוד: {status_code or 'N/A'}), מוותר על הכתובת: ") + url

        console.print(error_msg, style="bold red")



    return None



# --------------------------- שלב 1: שליפת שנים ---------------------------



def get_year_options(session: requests.Session) -> Dict[str, str]:

    """חילוץ אפשרויות השנים מהעמוד."""

    with console.status(heb("טוען שנות עלונים זמינות...")):

        resp = make_request(session, ALONIM_PAGE_URL, headers=HEADERS, timeout=30)

        if not resp:

            console.print(heb("שגיאה בטעינת העמוד, לא ניתן להמשיך."), style="bold red")

            return {}



    soup = BeautifulSoup(resp.content, "html.parser")

    year_select = soup.find("select", {"id": "edit-field-year-value-many-to-one"})

    if not year_select:

        console.print(heb("לא נמצא תפריט שנים. יתכן שהאתר השתנה."), style="bold yellow")

        return {}



    years = {

        opt["value"]: opt.text

        for opt in year_select.find_all("option")

        if opt.get("value") and opt.get("value") != "All"

    }

    console.print(

        hpanel(f"נמצאו {len(years)} שנים לסריקה והורדה.", border_style="green")

    )

    return years



# --------------------------- שלב 2: בחירת שנים ---------------------------



def render_years_table(sorted_years: List[Tuple[str, str]]) -> None:

    """מציג טבלת שנים ממוספרת (הצגה הפוכה לעברית בלבד)."""

    table = Table(title=heb("בחר שנים להורדה"), title_style="bold magenta")

    table.add_column("#", style="bold cyan", justify="right", no_wrap=True)

    table.add_column(heb("שנה"), style="bold", no_wrap=True)

    table.add_column("Value", style="dim", no_wrap=True)



    for idx, (year_val, year_name) in enumerate(sorted_years, start=1):

        table.add_row(str(idx), heb(year_name), year_val)



    console.print(table)

    console.print(

        hpanel(

            "קלט אפשרי",

            "מספר יחיד (למשל 5), טווח (3-7), "

            "רשימה עם פסיקים (1,4,8) או טווחים משולבים (1,3-5,9). "

            "ניתן גם להקליד all / הכל.",

            border_style="cyan",

        )

    )





def parse_year_selection(selection: str, sorted_years: List[Tuple[str, str]]) -> List[Tuple[str, str]]:

    """פענוח בחירת המשתמש: יחיד/טווח/רשימה/הכול."""

    s = (selection or "").strip().lower()

    if s in {"*", "all", "הכל"}:

        return sorted_years



    parts = re.split(r"[,\s]+", s)

    indices: set[int] = set()

    for part in parts:

        if not part:

            continue

        if "-" in part:

            a_str, b_str = part.split("-", 1)

            a, b = int(a_str) - 1, int(b_str) - 1

            if a > b:

                a, b = b, a

            for i in range(a, b + 1):

                indices.add(i)

        else:

            indices.add(int(part) - 1)



    if not indices:

        raise ValueError("לא נבחרו שנים.")



    max_idx = len(sorted_years) - 1

    bad = [i for i in indices if i < 0 or i > max_idx]

    if bad:

        bad_disp = ", ".join(map(lambda x: str(x + 1), bad))

        raise ValueError(f"אינדקסים לא תקינים: {bad_disp}")



    return [sorted_years[i] for i in sorted(indices)]



# --------------------------- שלב 3: סריקה והורדה ---------------------------



def scrape_and_download_for_year(

    session: requests.Session,

    year_val: str,

    year_name: str,

    safe_year_dir_name: str,

) -> None:

    """סריקת שנה והורדות, עם הצגה עברית מהופכת ב-CMD."""

    url = f"{ALONIM_PAGE_URL}?field_year_value_many_to_one={year_val}"

    page_num = 1



    console.print(

        Panel.fit(

            heb(f"שנה: {year_name}  (value={year_val})"),

            border_style="magenta",

        )

    )



    while url:

        with console.status(heb(f"סורק עמוד {page_num} לשנת {year_name}...")):

            resp = make_request(session, url, headers=HEADERS, timeout=30)

            if not resp:

                console.print(heb(f"שגיאה בסריקת עמוד {page_num}. מדלג על המשך השנה."), style="bold red")

                return



        soup = BeautifulSoup(resp.content, "html.parser")



        if page_num == 1 and soup.select_one(".view-empty"):

            console.print(heb("אין תוצאות לשנה זו. מדלג..."), style="yellow")

            return



        items = soup.select(".views-view-grid tr td")

        if not items:

            console.print(heb("לא נמצאו פריטים בעמוד. ממשיך..."), style="yellow")



        progress = Progress(

            SpinnerColumn(style="cyan"),

            TextColumn("{task.description}"),

            BarColumn(bar_width=None),

            DownloadColumn(),

            TransferSpeedColumn(),

            TimeElapsedColumn(),

            TimeRemainingColumn(),

            expand=True,

            console=console,

        )



        with progress:

            for item in items:

                title_tag = item.select_one(".views-field-title-1 a")

                link_tag = item.select_one("a.bulletin-link")

                if not (title_tag and link_tag):

                    continue



                file_title = clean_name(title_tag.text)              # לשם קובץ (אמיתי, לא מהופך)

                display_title = heb(title_tag.text)                  # להצגה מהופכת

                download_url = BASE_URL + link_tag["href"]



                main_topic_tag = item.select_one(".subject_dic")

                specific_subject_tag = item.select_one(".views-field-field-summary .part-summary")



                main_topic = clean_name(main_topic_tag.text.rstrip(",")) if main_topic_tag else ""

                specific_subject = clean_name(specific_subject_tag.text) if specific_subject_tag else ""



                path_parts = [DOWNLOAD_DIR, safe_year_dir_name]

                if main_topic:

                    path_parts.append(main_topic)

                if specific_subject:

                    path_parts.append(specific_subject)

                target_dir = os.path.join(*path_parts)



                # תיאור ידידותי למה שמורידים (מהופך רק לתצוגה)

                desc = heb(f"מוריד {display_title}.pdf")

                download_file(session, download_url, target_dir, f"{file_title}.pdf", progress, desc)



        next_tag = soup.select_one("li.pager-next a")

        if next_tag and next_tag.get("href"):

            url = BASE_URL + next_tag["href"]

            page_num += 1

            time.sleep(0.8)

        else:

            url = None



def download_file(

    session: requests.Session,

    url: str,

    directory: str,

    filename: str,

    progress: Progress,

    description: str,

) -> None:

    """הורדת קובץ יחיד עם תצוגת התקדמות, עם טיפול בשגיאות רשת וקלט/פלט."""

    path = os.path.join(directory, filename)



    if os.path.exists(path):

        progress.console.log(heb("קיים כבר: ") + path, style="dim")

        return



    try:

        os.makedirs(directory, exist_ok=True)

    except OSError as e:

        progress.console.log(heb("שגיאה ביצירת תיקיה: ") + str(e), style="bold red")

        progress.console.log(heb("הנתיב הבעייתי: ") + directory, style="dim red")

        return



    # The streaming request is now wrapped with retry logic

    r = make_request(session, url, stream=True, headers=HEADERS, timeout=60)

    if not r:

        # make_request already logged the error

        return



    try:

        with r: # Ensure the response is closed

            total_str = r.headers.get("Content-Length")

            total = int(total_str) if total_str and total_str.isdigit() else None



            task_id = progress.add_task(description, total=total)



            with open(path, "wb") as f:

                for chunk in r.iter_content(chunk_size=8192):

                    if not chunk:

                        continue

                    f.write(chunk)

                    progress.update(task_id, advance=len(chunk))



            if total is None:

                # If no content length, just mark as complete

                progress.update(task_id, completed=progress.tasks[task_id].total)



        progress.console.log(heb("הושלם: ") + path, style="green")



    except OSError as e:

        progress.console.log(heb("שגיאת קובץ/תיקיה בעת כתיבה: ") + str(e), style="bold red")

        progress.console.log(heb("הקובץ הבעייתי: ") + path, style="dim red")

    except Exception as e:

        # Catch any other potential errors during download/write

        progress.console.log(heb("שגיאה לא צפויה בהורדה: ") + str(e), style="bold red")

# --------------------------- main ---------------------------

def main() -> None:
    console.print(Panel.fit(heb("סריקת עלונים והורדה • ממשק Rich"), border_style="bright_magenta"))

    session = requests.Session()
    session.headers.update(HEADERS)

    years = get_year_options(session)
    if not years:
        console.print(heb("לא ניתן לאחזר שנים. יציאה."), style="bold red")
        return

    # לא לשנות סדר אמיתי; להצגה נשתמש ב-heb() רק כשמדפיסים
    sorted_years = sorted(years.items(), key=lambda kv: kv[0], reverse=True)
    render_years_table(sorted_years)

    selection = Prompt.ask(
        heb("בחר שנים (דוגמאות: 5 / 3-7 / 1,3-5,9 / all)"),
        default="all"
    )

    try:
        chosen = parse_year_selection(selection, sorted_years)
    except ValueError as e:
        console.print(heb("קלט שגוי: ") + heb(str(e)), style="bold red")
        return

    console.print(Panel.fit(heb(f"יתבצע עיבוד של {len(chosen)} שנים."), border_style="cyan"))

    for year_val, year_name in chosen:
        safe_year = clean_name(year_name)            # לשם תיקיה אמיתי
        scrape_and_download_for_year(session, year_val, year_name, safe_year)
        time.sleep(1.0)

    console.print(Panel.fit(heb("הסתיים! הקבצים נשמרו בתיקיה Downloaded_Alonim."), border_style="green"))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n" + heb("בוטל על ידי המשתמש."), style="bold yellow")
