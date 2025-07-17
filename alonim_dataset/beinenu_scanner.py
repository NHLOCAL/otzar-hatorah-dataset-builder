import requests
from bs4 import BeautifulSoup
import os
import time
import re

BASE_URL = "https://beinenu.com"
ALONIM_PAGE_URL = f"{BASE_URL}/alonim"
DOWNLOAD_DIR = "Downloaded_Alonim"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def clean_name(text):
    """
    פונקציה מרכזית לניקוי שמות קבצים ותיקיות מתווים לא חוקיים.
    """
    if not text:
        return ""
    # מחליף תווים לא חוקיים בקו תחתון
    return re.sub(r'[\\/*?:"<>|]', '_', text.strip())

def get_year_options(session):
    """
    פונקציה שמחלצת את כל אפשרויות השנים מהפילטר.
    """
    print("Fetching available years...")
    try:
        response = session.get(ALONIM_PAGE_URL, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        year_select = soup.find('select', {'id': 'edit-field-year-value-many-to-one'})
        if not year_select:
            print("Could not find year filter dropdown. Exiting.")
            return {}
        
        years = {opt['value']: opt.text for opt in year_select.find_all('option') if opt.get('value') and opt.get('value') != 'All'}
        print(f"Found {len(years)} years to scan.")
        return years
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page to get year options: {e}")
        return {}

def scrape_and_download_for_year(session, year_val, year_name, safe_year_dir_name):
    """
    פונקציה שסורקת שנה ומורידה את העלונים למבנה התיקיות החדש.
    """
    current_url = f"{ALONIM_PAGE_URL}?field_year_value_many_to_one={year_val}"
    page_num = 1
    
    while current_url:
        print(f"  Scanning page {page_num} for year {year_name}...")
        try:
            response = session.get(current_url, headers=HEADERS)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            if page_num == 1 and soup.select_one('.view-empty'):
                print(f"  --> No results found for year {year_name}. Skipping to next year.")
                return

            items = soup.select('.views-view-grid tr td')
            for item in items:
                title_tag = item.select_one('.views-field-title-1 a')
                link_tag = item.select_one('a.bulletin-link')
                
                if title_tag and link_tag:
                    # חילוץ כל חלקי המידע
                    file_title = clean_name(title_tag.text)
                    download_url = BASE_URL + link_tag['href']

                    main_topic_tag = item.select_one('.subject_dic')
                    specific_subject_tag = item.select_one('.views-field-field-summary .part-summary')
                    
                    # ניקוי המידע והסרת פסיקים מיותרים
                    main_topic = clean_name(main_topic_tag.text.rstrip(',')) if main_topic_tag else ""
                    specific_subject = clean_name(specific_subject_tag.text) if specific_subject_tag else ""
                    
                    # בניית נתיב התיקיות באופן דינמי
                    path_parts = [DOWNLOAD_DIR, safe_year_dir_name]
                    if main_topic:
                        path_parts.append(main_topic)
                    if specific_subject:
                        path_parts.append(specific_subject)
                    
                    target_dir = os.path.join(*path_parts)
                    
                    # הורדת הקובץ
                    download_file(session, download_url, target_dir, file_title)

            next_page_tag = soup.select_one('li.pager-next a')
            if next_page_tag and next_page_tag.get('href'):
                current_url = BASE_URL + next_page_tag['href']
                page_num += 1
            else:
                current_url = None
                
        except requests.exceptions.RequestException as e:
            print(f"    Error scraping page {current_url}: {e}")
            current_url = None
            
        time.sleep(1)

def download_file(session, url, directory, filename):
    """
    פונקציה להורדת קובץ ושמירתו.
    """
    path = os.path.join(directory, f"{filename}.pdf")
    if os.path.exists(path):
        # print(f"    - File already exists: {filename}.pdf, skipping.") # אפשר להסיר הערה לפירוט יתר
        return

    print(f"    - Downloading: {filename}.pdf\n      to path: {directory}")
    try:
        os.makedirs(directory, exist_ok=True)
        response = session.get(url, headers=HEADERS, stream=True)
        response.raise_for_status()
        
        with open(path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
    except requests.exceptions.RequestException as e:
        print(f"      [ERROR] Failed to download {filename}.pdf: {e}")
    except OSError as e:
        print(f"      [ERROR] Could not create directory or save file: {e}")

# --- תהליך ראשי ---
if __name__ == "__main__":
    session = requests.Session()
    years = get_year_options(session)
    
    if not years:
        print("Could not retrieve years. Exiting.")
    else:
        print("\n--- Starting Scan and Download Process ---")
        for year_val, year_name in sorted(years.items(), reverse=True):
            print(f"\nProcessing Year: {year_name} (Value: {year_val})")
            
            safe_year_name = clean_name(year_name)
            
            scrape_and_download_for_year(session, year_val, year_name, safe_year_name)
            time.sleep(2)

    print("\n\nDownload script finished.")