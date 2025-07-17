# התקנת התלויות:
# pip install docling

from docling.document_converter import DocumentConverter

def pdf_to_markdown_reversed(input_pdf_path: str, output_md_path: str):
    
    source = input_pdf_path
    
    converter = DocumentConverter()
    doc = converter.convert(source).document

    with open(output_md_path, "w", encoding="utf-8") as f:
        f.write(doc.export_to_markdown())

if __name__ == "__main__":
    input_pdf = input("write input pdf\n")
    output_md = input("write output md\n")
    pdf_to_markdown_reversed(input_pdf, output_md)
    print(f"✅ ההמרה הושלמה: {output_md}")
