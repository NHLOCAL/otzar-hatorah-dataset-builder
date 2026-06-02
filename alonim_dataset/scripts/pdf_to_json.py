from __future__ import annotations

import argparse
from pathlib import Path

import _bootstrap  # noqa: F401
from alonim.docling_convert import convert_pdf_to_docling_json


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert a PDF to Docling JSON.")
    parser.add_argument("pdf", type=Path)
    parser.add_argument("--output-dir", type=Path, default=None)
    mirror_group = parser.add_mutually_exclusive_group()
    mirror_group.add_argument(
        "--rtl-mirror-input",
        action="store_true",
        dest="rtl_mirror_input",
        default=True,
        help="Mirror pages horizontally before Docling, then restore layout coordinates in JSON (default).",
    )
    mirror_group.add_argument(
        "--no-rtl-mirror-input",
        action="store_false",
        dest="rtl_mirror_input",
        help="Disable the RTL mirror pipeline and use the older Docling CLI conversion path.",
    )
    args = parser.parse_args()

    kwargs = {"output_dir": args.output_dir} if args.output_dir is not None else {}
    kwargs["rtl_mirror_input"] = args.rtl_mirror_input
    path = convert_pdf_to_docling_json(args.pdf, **kwargs)
    print(f"JSON written to: {path}")


if __name__ == "__main__":
    main()
