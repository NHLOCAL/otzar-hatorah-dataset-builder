from __future__ import annotations

import argparse
from pathlib import Path

import _bootstrap  # noqa: F401
from alonim.downloader import DownloadConfig, download_years, get_year_options, make_session, parse_year_selection, sorted_year_options


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download BeInenu bulletin PDFs into source_data/pdf.")
    parser.add_argument("selection", nargs="?", default=None, help="Year selection: 5, 3-7, 1,3-5,9, or all.")
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--delay-seconds", type=float, default=0.8)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = make_session()
    years = sorted_year_options(get_year_options(session))
    if not years:
        raise SystemExit("No year options were found. The source website may have changed.")

    for index, year in enumerate(years, start=1):
        print(f"{index}: {year.label} ({year.value})")

    selection = args.selection or input("Choose years (for example 5 / 3-7 / all): ").strip()
    chosen = parse_year_selection(selection, years)
    config_kwargs = {
        "delay_seconds": args.delay_seconds,
        "skip_existing": not args.overwrite,
    }
    if args.output_dir is not None:
        config_kwargs["output_dir"] = args.output_dir

    paths = download_years(chosen, session=session, config=DownloadConfig(**config_kwargs))
    print(f"Downloaded {len(paths)} new files.")


if __name__ == "__main__":
    main()
