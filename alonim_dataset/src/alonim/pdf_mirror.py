from __future__ import annotations

from pathlib import Path


def create_horizontally_mirrored_pdf(source_path: Path, output_path: Path) -> Path:
    source_path = source_path.expanduser().resolve()
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    from pypdf import PdfReader, PdfWriter, Transformation

    reader = PdfReader(str(source_path))
    writer = PdfWriter()

    for page in reader.pages:
        width = float(page.mediabox.width)
        page.add_transformation(Transformation().scale(sx=-1, sy=1).translate(tx=width, ty=0))
        writer.add_page(page)

    with output_path.open("wb") as handle:
        writer.write(handle)

    return output_path
