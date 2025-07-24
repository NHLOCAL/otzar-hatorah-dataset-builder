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
    # פונקציה זו יכולה להכיל לוגיקות ניקוי נוספות בעתיד
    return text.strip()

def analyze_and_sort_page_elements(page_elements: list, page_width: float, page_no: int) -> tuple[list, list]:
    """
    מנתח את פריסת העמוד (עמודה 1, 2, או 3) וממיין את האלמנטים
    בהתאם לסדר הקריאה בעברית (מימין לשמאל, מלמעלה למטה).
    """
    if not page_width or not page_elements:
        return page_elements, []

    # הגדרת ספים לזיהוי עמודות
    right_col, left_col, full_width, footnotes = [], [], [], []
    center_margin = page_width * 0.1 # שוליים של 10% במרכז
    page_center = page_width / 2
    
    # שלב 1: סיווג כל פריט לעמודה המתאימה או להערת שוליים
    for item in page_elements:
        label = item.get('label', '')
        # הפרדת אלמנטים מיוחדים כמו כותרות עליונות, תחתונות והערות שוליים
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

            # סיווג לפי רוחב ומיקום
            if item_width > page_width * 0.65:
                full_width.append(item)
            elif item_right < page_center + center_margin:
                left_col.append(item)
            elif item_left > page_center - center_margin:
                right_col.append(item)
            else: # אלמנטים שחוצים את המרכז אבל אינם ברוחב מלא
                full_width.append(item)
                
        except (KeyError, IndexError, TypeError):
            # אם אין מידע מיקום, נניח שהאלמנט ברוחב מלא
            full_width.append(item)

    # שלב 2: זיהוי סופי של פריסת העמוד
    is_two_columns = len(right_col) > 0 and len(left_col) > 0
    layout_type = "2 עמודות" if is_two_columns else "עמודה אחת"
    print(f"  [עמוד {page_no}] זוהתה פריסת {layout_type}.")

    # שלב 3: מיון כל רשימה בנפרד (מלמעלה למטה)
    # המפתח למיון הוא הקואורדינטה העליונה של התיבה התוחמת (t)
    sort_key_top_down = lambda item: item.get('prov', [{}])[0].get('bbox', {}).get('t', 0)
    for col in [full_width, right_col, left_col, footnotes]:
        col.sort(key=sort_key_top_down, reverse=True)

    # שלב 4: הרכבת סדר הקריאה הסופי - מימין לשמאל
    sorted_body = []
    sorted_body.extend(full_width)
    sorted_body.extend(right_col)
    sorted_body.extend(left_col)
    
    return sorted_body, footnotes

def generate_markdown_text(item: dict) -> str:
    """
    מייצר שורת Markdown עבור פריט בודד.
    """
    text = clean_ocr_text(item.get('text', ''))
    label = item.get('label', 'text')

    if not text:
        return ""

    if label == 'section_header':
        level = item.get('level', 1)
        prefix = "#" * (level + 1) # רמה 1 תהיה ##, רמה 2 תהיה ###
        return f"\n{prefix} {text}\n\n"
    
    if label == 'list_item':
        return f"* {text}\n"

    # פסקאות רגילות
    return f"{text}\n\n"

def process_and_structure_data(data: dict) -> dict:
    """
    מאגד את כל הטקסטים למבנה נתונים מאורגן לפי עמודים.
    """
    pages_data = {}
    
    # יצירת מפה לגישה מהירה לכל אלמנט לפי ה-ID שלו
    element_map = {item['self_ref']: item for item in data.get('texts', []) if 'self_ref' in item}
    element_map.update({item['self_ref']: item for item in data.get('pictures', []) if 'self_ref' in item})

    # איסוף כל האלמנטים שמופיעים בגוף המסמך
    body_children_refs = [child.get('$ref') for child in data.get('body', {}).get('children', [])]

    for ref_path in body_children_refs:
        item = element_map.get(ref_path)
        if not item:
            continue

        # שיוך האלמנט לעמוד המתאים
        try:
            page_no = item['prov'][0]['page_no']
            if page_no not in pages_data:
                pages_data[page_no] = []
            pages_data[page_no].append(item)
        except (KeyError, IndexError):
            # אם אין מידע על עמוד, נשייך לעמוד הראשון כברירת מחדל
            if 1 not in pages_data: pages_data[1] = []
            pages_data[1].append(item)
            
    return pages_data

def convert_docling_json_to_md(json_path: str, output_path: str = None):
    """
    ממיר קובץ JSON במבנה DoclingDocument לקובץ Markdown,
    עם יכולת זיהוי פריסה ומיון RTL.
    """
    if not os.path.exists(json_path):
        print(f"שגיאה: קובץ המקור לא נמצא בנתיב '{json_path}'")
        return

    if output_path is None:
        output_path = os.path.splitext(json_path)[0] + '.md'

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # שלב א': ארגון כל המידע לפי עמודים
    pages_data = process_and_structure_data(data)
    md_content = []
    
    print(f"מתחיל עיבוד של '{os.path.basename(json_path)}'...")
    print("מבצע ניתוח גיאומטרי ומיון RTL...")

    # שלב ב': עיבוד כל עמוד בנפרד
    for page_no in sorted(pages_data.keys()):
        page_elements = pages_data.get(page_no, [])
        page_size = data.get('pages', {}).get(str(page_no), {}).get('size', {})
        page_width = page_size.get('width', 600) # רוחב ברירת מחדל אם חסר

        # המיון החכם מתבצע כאן
        sorted_body, sorted_footnotes = analyze_and_sort_page_elements(page_elements, page_width, page_no)
        
        md_content.append(f"\n---\n\n<!-- Page {page_no} -->\n\n")

        for item in sorted_body:
            md_content.append(generate_markdown_text(item))

        # הוספת הערות שוליים בסוף העמוד
        if sorted_footnotes:
            md_content.append("---\n\n")
            for item in sorted_footnotes:
                md_content.append(f"*{clean_ocr_text(item.get('text', ''))}*\n\n")
    
    # שלב ג': הרכבת הקובץ הסופי וניקוי
    final_output = "".join(md_content)
    
    # איחוד פסקאות שנשברו בגלל מעברי שורה, תוך שמירה על רשימות וכותרות
    final_output = re.sub(r'(?<!\n)\n(?!\n|#|\*|-)', ' ', final_output)
    # הסרת רווחים כפולים שנוצרו מהאיחוד
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
    פונקציית הכניסה הראשית. מנתחת ארגומנטים או מבקשת קלט מהמשתמש.
    """
    parser = argparse.ArgumentParser(
        description="ממיר קובץ Docling JSON לקובץ Markdown, עם זיהוי פריסה מתקדם (RTL).",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("source_file", nargs='?', default=None, help="נתיב לקובץ ה-JSON המהווה מקור.")
    parser.add_argument("-o", "--output", dest="output_file", help="נתיב לקובץ ה-Markdown שייווצר (אופציונלי).")
    
    args = parser.parse_args()
    
    source_file = args.source_file
    
    # אם לא סופק נתיב כארגומנט, בקש מהמשתמש
    if not source_file:
        try:
            source_file = input("נא להזין את הנתיב המלא לקובץ ה-JSON וללחוץ אנטר:\n> ")
        except KeyboardInterrupt:
            print("\nהפעולה בוטלה על ידי המשתמש.")
            return

    # ניקוי הנתיב ממרכאות ורווחים מיותרים
    if source_file:
        source_file = source_file.strip().strip('"').strip("'")
    
    if not source_file:
        print("לא הוזן נתיב. התהליך נעצר.")
        return

    convert_docling_json_to_md(source_file, args.output_file)


if __name__ == "__main__":
    main()