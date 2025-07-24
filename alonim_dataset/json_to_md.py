import json
import os
import argparse

def analyze_and_sort_page_elements(page_elements, page_width):
    """
    מנתח את פריסת העמוד (1, 2 או 3 עמודות) וממיין את האלמנטים בהתאם.
    """
    if not page_width or not page_elements:
        return [], []

    # הגדרת נקודות חיתוך לשלוש עמודות
    gutter1 = page_width / 3
    gutter2 = page_width * 2 / 3
    COLUMN_DETECTION_THRESHOLD = 3  # מספר מינימלי של פריטים כדי להגדיר עמודה

    right_col, middle_col, left_col, full_width, footnotes = [], [], [], [], []

    # 1. סיווג אלמנטים לקטגוריות
    for item in page_elements:
        label = item.get('label', '')
        text = item.get('text', '')
        
        if label == 'footnote' or (text.startswith('>') and 'footnote' in label):
            footnotes.append(item)
            continue
        if label == 'page_footer' or ('***' in text and len(set(text)) < 5):
            continue

        try:
            bbox = item['prov'][0]['bbox']
            # סיווג לפי מיקום אופקי
            if bbox['l'] >= gutter2:
                right_col.append(item)
            elif bbox['r'] <= gutter1:
                left_col.append(item)
            elif bbox['l'] >= gutter1 and bbox['r'] <= gutter2:
                middle_col.append(item)
            else: # פריט שחוצה "גבולות" ייחשב כפריט רחב
                full_width.append(item)
        except (KeyError, IndexError):
            full_width.append(item) # ברירת מחדל לפריטים ללא מיקום

    # 2. החלטה על פריסה
    num_cols = 1
    if len(right_col) > COLUMN_DETECTION_THRESHOLD and \
       len(middle_col) > COLUMN_DETECTION_THRESHOLD and \
       len(left_col) > COLUMN_DETECTION_THRESHOLD:
        num_cols = 3
    elif len(right_col) > COLUMN_DETECTION_THRESHOLD and \
         len(left_col) > COLUMN_DETECTION_THRESHOLD:
        num_cols = 2
    
    print(f"  [זוהתה פריסת {num_cols} עמודות]")

    # 3. מיון כל קטגוריה בנפרד (מלמעלה למטה)
    sort_key_top_down = lambda item: item.get('prov', [{}])[0].get('bbox', {}).get('t', 0)
    for col in [full_width, right_col, middle_col, left_col, footnotes]:
        col.sort(key=sort_key_top_down, reverse=True)

    # 4. איחוד הפריטים לפי סדר הקריאה הנכון
    sorted_body = []
    if num_cols == 3:
        sorted_body.extend(full_width)
        sorted_body.extend(right_col)
        sorted_body.extend(middle_col)
        sorted_body.extend(left_col)
    elif num_cols == 2:
        sorted_body.extend(full_width)
        sorted_body.extend(right_col)
        # מאחדים את העמודה האמצעית עם השמאלית למקרה של זליגה
        middle_and_left = middle_col + left_col
        middle_and_left.sort(key=sort_key_top_down, reverse=True)
        sorted_body.extend(middle_and_left)
    else: # פריסת עמודה אחת
        all_body_items = full_width + right_col + middle_col + left_col
        # מיון מדויק: אנכי (למעלה-למטה), ואז אופקי (ימין-לשמאל)
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
        # מנקה טקסטים מיותרים מהכותרת
        text = text.split('/uni05DF')[0].strip()
        return f"{'#' * level} {text}\n\n"
    
    if label == 'list_item':
        return f"* {text}\n"

    if text.startswith('"') and text.endswith('"'):
        stripped_text = text.strip('"')
        return f"> {stripped_text}\n\n"
        
    return f"{text}\n\n"

def process_groups(groups, pages_data):
    """מוסיף פריטי Group (כמו רשימות) למבנה הנתונים של העמודים."""
    for group in groups:
        if group.get('label') == 'list':
            for item in group.get('children', []):
                # מניחים שה-ref הוא תמיד ל-texts
                text_ref_index = int(item['$ref'].split('/')[-1])
                list_item_data = next((t for t in data.get('texts', []) if t['self_ref'] == f"#/texts/{text_ref_index}"), None)
                if list_item_data:
                    list_item_data['label'] = 'list_item' # מסמנים את הפריט כחלק מרשימה
                    try:
                        page_no = list_item_data['prov'][0]['page_no']
                        if page_no in pages_data:
                            # מוודאים שהפריט לא כבר קיים (למנוע כפילות)
                            if not any(el['self_ref'] == list_item_data['self_ref'] for el in pages_data[page_no]):
                                pages_data[page_no].append(list_item_data)
                    except (KeyError, IndexError):
                        continue

def convert_docling_json_to_md(json_path, output_path=None):
    """
    ממיר קובץ JSON במבנה DoclingDocument לקובץ Markdown,
    תוך זיהוי וטיפול אוטומטי בפריסות של 1, 2 או 3 עמודות.
    """
    global data # מאפשר גישה לנתונים הגלובליים מפונקציית process_groups

    if not os.path.exists(json_path):
        print(f"שגיאה: קובץ המקור לא נמצא בנתיב '{json_path}'")
        return

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
    
    # מאכלסים את pages_data רק עם פריטים מ-texts שאינם חלק מ-groups
    initial_texts = [item for item in data.get('texts', []) if 'parent' in item and item['parent']['$ref'] == '#/body']
    for item in initial_texts:
        try:
            page_no = item['prov'][0]['page_no']
            if page_no not in pages_data:
                pages_data[page_no] = []
            pages_data[page_no].append(item)
        except (KeyError, IndexError):
            if 1 not in pages_data: pages_data[1] = []
            pages_data[1].append(item)
    
    # מעבדים את ה-groups ומוסיפים את הפריטים שלהם למבנה pages_data
    process_groups(data.get('groups', []), pages_data)

    md_content = []
    
    print(f"מתחיל עיבוד של '{os.path.basename(json_path)}'...")
    for page_no in sorted(pages_data.keys()):
        print(f"מעבד עמוד {page_no}...")
        page_elements = pages_data[page_no]
        page_width = data.get('pages', {}).get(str(page_no), {}).get('size', {}).get('width')
        
        if not page_width:
            print(f"  אזהרה: לא נמצא רוחב עבור עמוד {page_no}. משתמש ברוחב ברירת מחדל.")
            page_width = 600

        sorted_body, sorted_footnotes = analyze_and_sort_page_elements(page_elements, page_width)

        md_content.append(f"\n---\n\n<!-- Page {page_no} -->\n\n")

        # איחוד פריטי רשימה רצופים
        is_in_list = False
        for item in sorted_body:
            is_list_item = item.get('label') == 'list_item'
            md_line = generate_markdown_text(item)

            # מוודאים שאין שורות ריקות מיותרות בתוך רשימה
            if is_list_item:
                md_content.append(md_line)
            else:
                 # מוסיפים רווח לפני פריט שאינו רשימה אם הפריט הקודם היה ברשימה
                if is_in_list:
                    md_content.append('\n')
                md_content.append(md_line)
            is_in_list = is_list_item


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
        description="ממיר קובץ Docling JSON לקובץ Markdown, עם זיהוי פריסה אוטומטי (1, 2 או 3 עמודות).",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
דוגמאות שימוש:
1. קלט אינטראקטיבי:
   python %(prog)s

2. המרה עם נתיב מקור בלבד (הפלט יישמר באותה תיקייה):
   python %(prog)s "C:\\path\\to\\file.json"

3. המרה עם נתיב מקור ונתיב יעד מוגדר:
   python %(prog)s "C:\\input\\source.json" -o "D:\\output\\final.md"
"""
    )
    parser.add_argument("source_file", nargs='?', help="נתיב לקובץ ה-JSON המהווה מקור.")
    parser.add_argument("-o", "--output", dest="output_file", help="נתיב לקובץ ה-Markdown שייווצר (אופציונלי).")
    
    args = parser.parse_args()
    
    source_path = args.source_file
    
    if not source_path:
        print("ממיר קבצי Docling JSON ל-Markdown (גרסה אוניברסלית)")
        print("=" * 55)
        source_path = input("הכנס את הנתיב המלא לקובץ ה-JSON: ").strip().strip('\'"')

    if not source_path:
        print("לא סופק נתיב. התהליך בוטל.")
        return

    convert_docling_json_to_md(source_path, args.output_file)

if __name__ == "__main__":
    main()