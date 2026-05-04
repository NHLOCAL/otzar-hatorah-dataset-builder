from __future__ import annotations

import argparse
from pathlib import Path

import _bootstrap  # noqa: F401
from alonim.downloader import DownloadConfig, download_years, get_year_options, make_session, parse_year_selection, sorted_year_options
from alonim.text import display_hebrew


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download BeInenu bulletin PDFs with Rich terminal output.")
    parser.add_argument("selection", nargs="?", default=None, help="Year selection: 5, 3-7, 1,3-5,9, or all.")
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--delay-seconds", type=float, default=0.8)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> None:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.traceback import install

    install(show_locals=False)
    console = Console(highlight=False, markup=False)
    session = make_session()

    console.print(Panel.fit(display_hebrew("סריקת עלונים והורדה"), border_style="bright_magenta"))
    years = sorted_year_options(get_year_options(session))
    if not years:
        console.print(display_hebrew("לא נמצאו שנים. יתכן שהאתר השתנה."), style="bold red")
        raise SystemExit(1)

    table = Table(title=display_hebrew("בחר שנים להורדה"))
    table.add_column("#", justify="right")
    table.add_column(display_hebrew("שנה"))
    table.add_column("Value")
    for index, year in enumerate(years, start=1):
        table.add_row(str(index), display_hebrew(year.label), year.value)
    console.print(table)

    args = parse_args()
    selection = args.selection or console.input(display_hebrew("בחר שנים (5 / 3-7 / all): ")).strip()
    chosen = parse_year_selection(selection, years)

    config_kwargs = {
        "delay_seconds": args.delay_seconds,
        "skip_existing": not args.overwrite,
    }
    if args.output_dir is not None:
        config_kwargs["output_dir"] = args.output_dir

    paths = download_years(
        chosen,
        session=session,
        config=DownloadConfig(**config_kwargs),
        log=lambda message: console.log(display_hebrew(message)),
    )
    console.print(Panel.fit(display_hebrew(f"הסתיים. הורדו {len(paths)} קבצים חדשים."), border_style="green"))


if __name__ == "__main__":
    main()
