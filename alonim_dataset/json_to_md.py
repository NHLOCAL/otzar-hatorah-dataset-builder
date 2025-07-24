import json
import os
import argparse
import re

def clean_ocr_text(text):
    """
    מנקה שגיאות OCR ומתקן אוטומטית תווי יוניקוד פגומים.
    """
    if not isinstance(text, str):
        return text

    def replace_unicode(match):
        hex_code = match.group(1)
        try:
            return chr(int(hex_code, 16))
        except ValueError:
            return match.group(0)

    return re.sub(r'/uni([0-9a-fA-F]{4})', replace_unicode, text)

def analyze_and_sort_page_elements(page_elements, page_width, page_no):
    """
    מנתח את פריסת העמוד (1 או 2 עמודות) וממיין את האלמנטים בהתאם לסדר הקריאה העברי (RTL).
    זוהי הגרסה המתוקנת והמדויקת של הפונקציה.
    """
    if not page_width or not page_elements:
        return page_elements, []

    # הגדרת ספים לזיהוי עמודות
    center_line = page_width / 2
    COLUMN_DETECTION_THRESHOLD = 2  # מספר מינימלי של פריטים כדי להחשיב עמודה כ"קיימת"

    right_col, left_col, full_width, footnotes = [], [], [], []

    # שלב 1: סיווג כל פריט לעמודה המתאימה
    for item in page_elements:
        label = item.get('label', '')
        
        # הפרדת הערות שוליים ופריטים לא רלוונטיים
        if label in ['footnote', 'page_footer', 'page_header']:
            if label == 'footnote':
                footnotes.append(item)
            continue
        
        try:
            bbox = item['prov'][0]['bbox']
            item_left = bbox['l']
            item_right = bbox['r']
            item_width = item_right - item_left
            item_center = (item_left + item_right) / 2

            # סיווג: רחב, ימני, או שמאלי
            if item_width > page_width * 0.7:  # קריטריון לפריט ברוחב מלא (כמו כותרת)
                full_width.append(item)
            elif item_center > center_line:
                right_col.append(item)
            else:
                left_col.append(item)
        except (KeyError, IndexError):
            full_width.append(item)

    # שלב 2: זיהוי סופי של פריסת העמוד
    is_two_columns = len(right_col) > COLUMN_DETECTION_THRESHOLD and len(left_col) > COLUMN_DETECTION_THRESHOLD
    print(f"  [עמוד {page_no}] זוהתה פריסת {'2 עמודות' if is_two_columns else 'עמודה אחת'}.")

    # שלב 3: מיון כל רשימה בנפרד (מלמעלה למטה)
    sort_key_top_down = lambda item: item.get('prov', [{}])[0].get('bbox', {}).get('t', 0)
    for col in [full_width, right_col, left_col, footnotes]:
        col.sort(key=sort_key_top_down, reverse=True)

    # שלב 4: הרכבת סדר הקריאה הסופי
    sorted_body = []
    if is_two_columns:
        # סדר קריאה של שתי עמודות בעברית: רחב -> ימין -> שמאל
        sorted_body.extend(full_width)
        sorted_body.extend(right_col)
        sorted_body.extend(left_col)
    else:
        # פריסה של עמודה אחת: ממיינים הכל יחד מלמעלה למטה
        all_body_items = full_width + right_col + left_col
        all_body_items.sort(key=sort_key_top_down, reverse=True)
        sorted_body.extend(all_body_items)

    return sorted_body, footnotes

def generate_markdown_text(item):
    """מייצר שורת Markdown עבור פריט בודד."""
    text = item.get('text', '').strip()
    label = item.get('label', 'text')

    if not text:
        return ""

    if label == 'section_header':
        level = item.get('level', 1)
        prefix = "##" if level == 1 else "###"
        return f"\n{prefix} {text}\n\n"
    
    if label == 'list_item':
        return f"* {text}\n"

    return f"{text}\n\n"

def process_and_structure_data(data):
    """
    מאגד את כל הטקסטים והקבוצות למבנה נתונים מאורגן לפי עמודים.
    """
    pages_data = {}
    all_elements = data.get('texts', []) + data.get('groups', [])
    
    element_map = {item['self_ref']: item for item in all_elements if 'self_ref' in item}
    
    body_children_refs = [child.get('$ref') for child in data.get('body', {}).get('children', [])]

    all_items_in_body = []
    for ref_path in body_children_refs:
        if ref_path in element_map:
             all_items_in_body.append(element_map[ref_path])

    for item in all_items_in_body:
        if 'children' in item: # Handle groups
            for child_ref in item.get('children', []):
                child_item = element_map.get(child_ref.get('$ref'))
                if child_item:
                    all_items_in_body.append(child_item)

    for item in all_items_in_body:
        if 'text' in item:
            item['text'] = clean_ocr_text(item['text'])
        
        try:
            page_no = item['prov'][0]['page_no']
            if page_no not in pages_data:
                pages_data[page_no] = []
            
            # מניעת כפילויות אם פריט כבר נוסף
            if not any(el['self_ref'] == item['self_ref'] for el in pages_data[page_no]):
                pages_data[page_no].append(item)
        except (KeyError, IndexError):
            if 1 not in pages_data: pages_data[1] = []
            if not any(el['self_ref'] == item['self_ref'] for el in pages_data[1]):
                pages_data[1].append(item)
            
    return pages_data

def convert_docling_json_to_md(json_path, output_path=None):
    """
    ממיר קובץ JSON במבנה DoclingDocument לקובץ Markdown.
    """
    if not os.path.exists(json_path):
        print(f"שגיאה: קובץ המקור לא נמצא בנתיב '{json_path}'")
        return

    if output_path is None:
        output_path = os.path.splitext(json_path)[0] + '_converted.md'

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    pages_data = process_and_structure_data(data)
    md_content = []
    
    print(f"מתחיל עיבוד של '{os.path.basename(json_path)}'...")
    print("  [מבצע תיקוני OCR ומיון גיאומטרי חכם]")

    for page_no in sorted(pages_data.keys()):
        page_elements = pages_data[page_no]
        page_width = data.get('pages', {}).get(str(page_no), {}).get('size', {}).get('width', 600)

        sorted_body, sorted_footnotes = analyze_and_sort_page_elements(page_elements, page_width, page_no)
        
        md_content.append(f"\n---\n\n<!-- Page {page_no} -->\n\n")

        for item in sorted_body:
            md_content.append(generate_markdown_text(item))

        if sorted_footnotes:
            md_content.append("---\n")
            for item in sorted_footnotes:
                md_content.append(f"*{item.get('text', '').strip()}*\n\n")
    
    final_output = "".join(md_content)
    # איחוד פסקאות שנשברו בגלל מעבר שורה
    final_output = re.sub(r'(?<!\n)\n(?!\n|#|\*|-)', ' ', final_output)

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_output)
        print("\n" + "="*35)
        print(f"ההמרה הושלמה בהצלחה! הקובץ נשמר ב:\n{output_path}")
        print("="*35)
    except Exception as e:
        print(f"שגיאה בכתיבת קובץ ה-Markdown: {e}")

def main():
    parser = argparse.ArgumentParser(
        description="ממיר קובץ Docling JSON לקובץ Markdown, עם תיקוני OCR וזיהוי פריסה אוטומטי.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("source_file", help="נתיב לקובץ ה-JSON המהווה מקור.")
    parser.add_argument("-o", "--output", dest="output_file", help="נתיב לקובץ ה-Markdown שייווצר (אופציונלי).")
    
    args = parser.parse_args()
    
    convert_docling_json_to_md(args.source_file, args.output_file)

if __name__ == "__main__":
    main()