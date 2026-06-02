from __future__ import annotations

import argparse
from pathlib import Path

import _bootstrap  # noqa: F401
from alonim.docling_convert import convert_pdf_to_docling_json


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert a PDF to Docling JSON.")
    parser.add_argument("pdf", type=Path)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument(
        "--rtl-mirror-input",
        action="store_true",
        help="Experiment: mirror pages horizontally before Docling, then restore layout coordinates in JSON.",
    )
    args = parser.parse_args()

    kwargs = {"output_dir": args.output_dir} if args.output_dir is not None else {}
    if args.rtl_mirror_input:
        kwargs["rtl_mirror_input"] = True
    path = convert_pdf_to_docling_json(args.pdf, **kwargs)
    print(f"JSON written to: {path}")


if __name__ == "__main__":
    main()
