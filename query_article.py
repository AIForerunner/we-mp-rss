import sqlite3
import base64

def query():
    conn = sqlite3.connect('data/db.db')
    cursor = conn.cursor()
    
    # 1. Get article details
    cursor.execute("""
        SELECT id, mp_id, title, url, 
               LENGTH(content), LENGTH(content_html), extinfo 
        FROM articles WHERE id = '2398602260-2649801374_1'
    """)
    art = cursor.fetchone()
    
    if not art:
        print("Article not found.")
        return

    a_id, a_mp_id, a_title, a_url, a_cnt_len, a_html_len, a_ext = art
    
    # 2. Extract numeric ID from MP_WXS_2398602260
    numeric_id = a_mp_id.replace("MP_WXS_", "")
    # Base64 encode it like "MjM5ODYwMjI2MA=="
    faker_id = base64.b64encode(numeric_id.encode()).decode()
    
    # 3. Query feeds using faker_id
    cursor.execute("SELECT id, mp_name, mp_cover FROM feeds WHERE faker_id = ?", (faker_id,))
    feed = cursor.fetchone()
    
    print(f"Article ID: {a_id}")
    print(f"MP ID: {a_mp_id}")
    print(f"Title: {a_title}")
    print(f"URL: {a_url}")
    print(f"Content Length: {a_cnt_len}")
    print(f"Content HTML Length: {a_html_len}")
    
    if feed:
        print(f"Feed ID: {feed[0]}")
        print(f"MP Name: {feed[1]}")
        print(f"MP Cover: {feed[2]}")
    else:
        print("Feed not found (tried faker_id: " + faker_id + ")")
        
    if a_ext:
        print(f"Extinfo (first 300 chars): {a_ext[:300]}")
    else:
        print("Extinfo: None or Empty")
    
    conn.close()

if __name__ == "__main__":
    query()
