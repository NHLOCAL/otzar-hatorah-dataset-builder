import json
import os
import sys
import re
import argparse

def clean_ocr_text(text: str) -> str:
    """
    מנקה טקסט שעבר OCR.
    """
    if not isinstance(text, str):
        return text
    return text.strip()

def analyze_and_sort_page_elements(page_elements: list, page_width: float, page_no: int) -> tuple[list, list]:
    """
    מנתח את פריסת העמוד (עמודה 1 או 2) וממיין את האלמנטים
    בהתאם לסדר הקריאה בעברית (מימין לשמאל, מלמעלה למטה).
    מזהה ומפריד תוכן ממוסגר.
    """
    if not page_width or not page_elements:
        return page_elements, []

    # קטגוריות של אלמנטים בעמוד
    right_col, left_col, interrupting_blocks, footnotes = [], [], [], []
    center_margin = page_width * 0.1
    page_center = page_width / 2
    
    # שלב 1: סיווג כל פריט לקטגוריה המתאימה
    for item in page_elements:
        label = item.get('label', '')
        if label in ['page_footer', 'page_header']:
            continue
        if label == 'footnote':
            footnotes.append(item)
            continue
        
        try:
            bbox = item['prov'][0]['bbox']
            item_left = bbox['l']
            item_right = bbox['r']
            item_width = item_right - item_left

            # תוכן ממוסגר או אלמנטים רחבים מטופלים כבלוקים שוברים
            if item.get('is_framed') or item_width > page_width * 0.65:
                interrupting_blocks.append(item)
            elif item_right < page_center + center_margin:
                left_col.append(item)
            elif item_left > page_center - center_margin:
                right_col.append(item)
            else: 
                interrupting_blocks.append(item)
                
        except (KeyError, IndexError, TypeError):
            interrupting_blocks.append(item)

    # שלב 2: זיהוי סופי של פריסת העמוד לצורך הדפסת מידע
    layout_type = "2 עמודות" if len(right_col) > 1 and len(left_col) > 1 else "עמודה אחת"
    print(f"  [עמוד {page_no}] זוהתה פריסת {layout_type}.")
    if any(item.get('is_framed') for item in interrupting_blocks):
        print(f"    - זוהה תוכן ממוסגר בעמוד זה.")


    # שלב 3: מיון כל רשימה בנפרד (מלמעלה למטה)
    sort_key_top_down = lambda item: item.get('prov', [{}])[0].get('bbox', {}).get('t', 0)
    for col in [interrupting_blocks, right_col, left_col, footnotes]:
        col.sort(key=sort_key_top_down, reverse=True)

    # שלב 4: הרכבת סדר הקריאה הסופי - בלוקים שוברים, ואז עמודה ימנית, ואז שמאלית
    all_body_items = interrupting_blocks + right_col + left_col
    
    # מיון סופי של כל האלמנטים בגוף העמוד מלמעלה למטה
    all_body_items.sort(key=sort_key_top_down, reverse=True)
    
    return all_body_items, footnotes

def generate_markdown_text(item: dict) -> str:
    """
    מייצר שורת Markdown עבור פריט בודד, עם טיפול מיוחד לתוכן ממוסגר.
    """
    text = clean_ocr_text(item.get('text', ''))
    label = item.get('label', 'text')

    if not text:
        return ""

    # עיצוב מיוחד לתוכן ממוסגר כציטוט (blockquote)
    if item.get('is_framed'):
        return f"> {text}\n\n"

    if label == 'section_header':
        level = item.get('level', 1)
        prefix = "#" * (level + 1)
        return f"\n{prefix} {text}\n\n"
    
    if label == 'list_item':
        return f"* {text}\n"

    return f"{text}\n\n"

def process_and_structure_data(data: dict) -> dict:
    """
    מאגד את כל האלמנטים למבנה נתונים מאורגן לפי עמודים,
    ומזהה תוכן הנמצא בתוך מסגרות (תמונות).
    """
    pages_data = {}
    
    element_map = {item['self_ref']: item for list_key in ['texts', 'pictures'] for item in data.get(list_key, []) if 'self_ref' in item}
    
    body_children_refs = [child.get('$ref') for child in data.get('body', {}).get('children', [])]

    processed_refs = set()

    for ref_path in body_children_refs:
        if ref_path in processed_refs:
            continue
            
        item = element_map.get(ref_path)
        if not item:
            continue

        # בדיקה אם האלמנט הוא תמונה שמשמשת כמסגרת לטקסט
        if item.get('label') == 'picture' and 'children' in item:
            is_frame = False
            # סמן את כל הילדים של התמונה כ"ממוסגרים"
            for child_ref in item.get('children', []):
                child_path = child_ref.get('$ref')
                child_item = element_map.get(child_path)
                if child_item and 'text' in child_item:
                    child_item['is_framed'] = True
                    is_frame = True
            if is_frame:
                # אלמנט התמונה עצמו לא יוצג, רק הטקסטים שבתוכו
                processed_refs.add(ref_path)
                continue # דלג על הוספת התמונה עצמה

        # שייך את האלמנט (או ילדיו, אם סומנו) לעמוד המתאים
        try:
            page_no = item['prov'][0]['page_no']
            if page_no not in pages_data:
                pages_data[page_no] = []
            if ref_path not in [el.get('self_ref') for el in pages_data[page_no]]:
                 pages_data[page_no].append(item)
            processed_refs.add(ref_path)

        except (KeyError, IndexError):
            if 1 not in pages_data: pages_data[1] = []
            if ref_path not in [el.get('self_ref') for el in pages_data[1]]:
                pages_data[1].append(item)
            processed_refs.add(ref_path)
            
    return pages_data


def convert_docling_json_to_md(json_path: str, output_path: str = None):
    """
    ממיר קובץ JSON במבנה DoclingDocument לקובץ Markdown.
    """
    if not os.path.exists(json_path):
        print(f"שגיאה: קובץ המקור לא נמצא בנתיב '{json_path}'")
        return

    if output_path is None:
        output_path = os.path.splitext(json_path)[0] + '.md'

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    pages_data = process_and_structure_data(data)
    md_content = []
    
    print(f"מתחיל עיבוד של '{os.path.basename(json_path)}'...")
    print("מבצע ניתוח גיאומטרי, זיהוי מסגרות ומיון RTL...")

    for page_no in sorted(pages_data.keys()):
        page_elements = pages_data.get(page_no, [])
        page_size = data.get('pages', {}).get(str(page_no), {}).get('size', {})
        page_width = page_size.get('width', 600)

        sorted_body, sorted_footnotes = analyze_and_sort_page_elements(page_elements, page_width, page_no)
        
        md_content.append(f"\n---\n\n<!-- Page {page_no} -->\n\n")

        for item in sorted_body:
            md_content.append(generate_markdown_text(item))

        if sorted_footnotes:
            md_content.append("---\n\n")
            for item in sorted_footnotes:
                md_content.append(f"*{clean_ocr_text(item.get('text', ''))}*\n\n")
    
    final_output = "".join(md_content)
    
    final_output = re.sub(r'(?<!\n)\n(?!\n|#|\*|>)', ' ', final_output)
    final_output = re.sub(r' +', ' ', final_output)

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_output)
        print("\n" + "="*35)
        print(f"ההמרה הושלמה בהצלחה! הקובץ נשמר ב:\n{output_path}")
        print("="*35)
    except Exception as e:
        print(f"שגיאה בכתיבת קובץ ה-Markdown: {e}")

def main():
    """
    פונקציית הכניסה הראשית.
    """
    parser = argparse.ArgumentParser(
        description="ממיר קובץ Docling JSON לקובץ Markdown, עם זיהוי פריסה מתקדם (RTL) ותמיכה בתוכן ממוסגר.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("source_file", nargs='?', default=None, help="נתיב לקובץ ה-JSON המהווה מקור.")
    parser.add_argument("-o", "--output", dest="output_file", help="נתיב לקובץ ה-Markdown שייווצר (אופציונלי).")
    
    args = parser.parse_args()
    
    source_file = args.source_file
    
    if not source_file:
        try:
            source_file = input("נא להזין את הנתיב המלא לקובץ ה-JSON וללחוץ אנטר:\n> ")
        except KeyboardInterrupt:
            print("\nהפעולה בוטלה על ידי המשתמש.")
            return

    if source_file:
        source_file = source_file.strip().strip('"').strip("'")
    
    if not source_file:
        print("לא הוזן נתיב. התהליך נעצר.")
        return

    convert_docling_json_to_md(source_file, args.output_file)

if __name__ == "__main__":
    main()