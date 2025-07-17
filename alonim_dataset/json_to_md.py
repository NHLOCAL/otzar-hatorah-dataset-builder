import json
import os

def convert_docling_json_to_md(json_path):
    """
     ממיר קובץ JSON במבנה DoclingDocument לקובץ Markdown,
    תוך שחזור סדר הקריאה על בסיס מיקום הטקסט (תומך בפריסת טורים).

    :param json_path: נתיב לקובץ ה-JSON.
    """
    if not os.path.exists(json_path):
        print(f"שגיאה: הקובץ לא נמצא בנתיב '{json_path}'")
        return

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"שגיאה בקריאה או בפענוח של קובץ ה-JSON: {e}")
        return

    base_name = os.path.splitext(json_path)[0]
    output_path = base_name + '.md'

    page_width = data.get('pages', {}).get('1', {}).get('size', {}).get('width')
    if not page_width:
        print("שגיאה: לא ניתן למצוא את רוחב העמוד בקובץ ה-JSON.")
        return
        
    midpoint = page_width / 2
    full_width_items, right_column_items, left_column_items, footer_items = [], [], [], []

    for item in data.get('texts', []):
        if item.get('label') == 'page_footer':
            footer_items.append(item)
            continue
        
        if '***' in item.get('text', '') and len(set(item.get('text', ''))) < 5:
            continue
            
        try:
            bbox = item['prov'][0]['bbox']
            if bbox['l'] < midpoint and bbox['r'] > midpoint:
                full_width_items.append(item)
            elif bbox['l'] >= midpoint:
                right_column_items.append(item)
            else:
                left_column_items.append(item)
        except (KeyError, IndexError):
            left_column_items.append(item)

    sort_key = lambda item: item.get('prov', [{}])[0].get('bbox', {}).get('t', 0)
    full_width_items.sort(key=sort_key, reverse=True)
    right_column_items.sort(key=sort_key, reverse=True)
    left_column_items.sort(key=sort_key, reverse=True)

    ordered_items = full_width_items + right_column_items + left_column_items

    md_lines = []
    for item in ordered_items:
        text = item.get('text', '').strip()
        label = item.get('label', 'text')

        if not text:
            continue

        if label == 'section_header':
            level = item.get('level', 2)
            md_lines.append(f"{'#' * level} {text}\n")
        else:
            # בדיקה אם הטקסט הוא ציטוט
            if (text.startswith('"') and text.endswith('"')) or (text.startswith('(') and text.endswith(')')):
                # הסרת המרכאות והוספת תגית ציטוט של Markdown
                stripped_text = text.strip('()"')
                md_lines.append(f"> {stripped_text}\n")
            else:
                md_lines.append(f"{text}\n")
            
    if footer_items:
        md_lines.append("---\n")
        footer_text = footer_items[0].get('text', '')
        md_lines.append(f"*{footer_text}*")

    md_content = "\n".join(md_lines)

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(md_content)
        print(f"ההמרה הושלמה בהצלחה! הקובץ נשמר ב:\n{output_path}")
    except Exception as e:
        print(f"שגיאה בכתיבת קובץ ה-Markdown: {e}")

if __name__ == "__main__":
    print("ממיר קבצי Docling JSON ל-Markdown")
    print("=" * 35)
    file_path_input = input("הכנס את הנתיב המלא לקובץ ה-JSON: ")
    
    cleaned_path = file_path_input.strip().strip('\'"')
    
    convert_docling_json_to_md(cleaned_path)