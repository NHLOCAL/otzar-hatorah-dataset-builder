import json
import os
import argparse

def analyze_and_sort_page_elements(page_elements, page_width):
    """
    מנתח את פריסת העמוד וממיין את האלמנטים בהתאם.
    תומך בפריסת עמודה אחת או שתי עמודות.
    """
    if not page_width or not page_elements:
        return [], []

    midpoint = page_width / 2
    COLUMN_DETECTION_THRESHOLD = 3 

    right_column, left_column, full_width, footnotes = [], [], [], []

    # 1. סיווג אלמנטים
    for item in page_elements:
        if item.get('label') == 'footnote' or (item.get('text', '').startswith('>') and 'footnote' in item.get('label', '')):
            footnotes.append(item)
            continue
        if item.get('label') == 'page_footer':
            continue
        if '***' in item.get('text', '') and len(set(item.get('text', ''))) < 5:
            continue

        try:
            bbox = item['prov'][0]['bbox']
            if bbox['l'] < midpoint and bbox['r'] > midpoint:
                full_width.append(item)
            elif bbox['l'] >= midpoint:
                right_column.append(item)
            else:
                left_column.append(item)
        except (KeyError, IndexError):
            full_width.append(item)

    # 2. החלטה על פריסה ומיון
    is_two_column = (len(right_column) > COLUMN_DETECTION_THRESHOLD and 
                     len(left_column) > COLUMN_DETECTION_THRESHOLD)

    sort_key_top_down = lambda item: item.get('prov', [{}])[0].get('bbox', {}).get('t', 0)

    full_width.sort(key=sort_key_top_down, reverse=True)
    right_column.sort(key=sort_key_top_down, reverse=True)
    left_column.sort(key=sort_key_top_down, reverse=True)
    footnotes.sort(key=sort_key_top_down, reverse=True)

    sorted_body = []
    if is_two_column:
        print(f"  [זוהתה פריסת שתי עמודות]")
        sorted_body.extend(full_width)
        sorted_body.extend(right_column)
        sorted_body.extend(left_column)
    else:
        all_body_items = full_width + right_column + left_column
        all_body_items.sort(key=lambda item: (
            item.get('prov', [{}])[0].get('bbox', {}).get('t', 0),
            item.get('prov', [{}])[0].get('bbox', {}).get('r', 0)
        ), reverse=True)
        sorted_body.extend(all_body_items)

    return sorted_body, footnotes

def generate_markdown_text(item):
    """מייצר שורת Markdown עבור פריט בודד."""
    text = item.get('text', '').strip()
    label = item.get('label', 'text')

    if not text:
        return ""

    if label == 'section_header':
        level = item.get('level', 2)
        return f"{'#' * level} {text}\n\n"
    
    if text.startswith('"') and text.endswith('"'):
        stripped_text = text.strip('"')
        return f"> {stripped_text}\n\n"
        
    return f"{text}\n\n"

def convert_docling_json_to_md(json_path, output_path=None):
    """
    ממיר קובץ JSON במבנה DoclingDocument לקובץ Markdown,
    תוך זיהוי וטיפול אוטומטי בפריסות שונות.
    """
    if not os.path.exists(json_path):
        print(f"שגיאה: קובץ המקור לא נמצא בנתיב '{json_path}'")
        return

    # קביעת נתיב יעד אם לא סופק
    if output_path is None:
        base_name = os.path.splitext(json_path)[0]
        output_path = base_name + '.md'

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"שגיאה בקריאה או בפענוח של קובץ ה-JSON: {e}")
        return

    pages_data = {}
    all_elements = data.get('texts', []) + data.get('pictures', [])
    
    for item in all_elements:
        try:
            page_no = item['prov'][0]['page_no']
            if page_no not in pages_data:
                pages_data[page_no] = []
            pages_data[page_no].append(item)
        except (KeyError, IndexError):
            if 1 not in pages_data:
                pages_data[1] = []
            pages_data[1].append(item)
            
    md_content = []
    
    print(f"מתחיל עיבוד של '{os.path.basename(json_path)}'...")
    for page_no in sorted(pages_data.keys()):
        print(f"מעבד עמוד {page_no}...")
        page_elements = pages_data[page_no]
        page_width = data.get('pages', {}).get(str(page_no), {}).get('size', {}).get('width')
        
        if not page_width:
            print(f"  אזהרה: לא נמצא רוחב עבור עמוד {page_no}. משתמש ברוחב משוער.")
            page_width = data.get('pages', {}).get('1', {}).get('size', {}).get('width', 600)

        sorted_body, sorted_footnotes = analyze_and_sort_page_elements(page_elements, page_width)

        md_content.append(f"\n---\n\n<!-- Page {page_no} -->\n\n")

        for item in sorted_body:
            md_content.append(generate_markdown_text(item))

        if sorted_footnotes:
            md_content.append("---\n\n")
            for item in sorted_footnotes:
                text = item.get('text', '').strip()
                md_content.append(f"*{text}*\n\n")

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("".join(md_content))
        print("\n" + "="*35)
        print(f"ההמרה הושלמה בהצלחה! הקובץ נשמר ב:\n{output_path}")
        print("="*35)
    except Exception as e:
        print(f"שגיאה בכתיבת קובץ ה-Markdown: {e}")

def main():
    parser = argparse.ArgumentParser(
        description="ממיר קובץ Docling JSON לקובץ Markdown, עם זיהוי פריסה אוטומטי.",
        epilog="דוגמה: python %(prog)s \"C:\\path\\to\\file.json\" -o \"C:\\output\\file.md\""
    )
    parser.add_argument(
        "source_file",
        nargs='?',  # הופך את הפרמטר לאופציונלי
        help="נתיב לקובץ ה-JSON המהווה מקור."
    )
    parser.add_argument(
        "-o", "--output",
        dest="output_file",
        help="נתיב לקובץ ה-Markdown שייווצר (אופציונלי)."
    )
    
    args = parser.parse_args()
    
    source_path = args.source_file
    output_path = args.output_file
    
    # אם לא סופק נתיב מקור כארגומנט, נבקש מהמשתמש
    if not source_path:
        print("ממיר קבצי Docling JSON ל-Markdown (גרסה משודרגת)")
        print("=" * 50)
        source_path = input("הכנס את הנתיב המלא לקובץ ה-JSON: ").strip().strip('\'"')

    if not source_path:
        print("לא סופק נתיב. התהליך בוטל.")
        return

    convert_docling_json_to_md(source_path, output_path)

if __name__ == "__main__":
    main()