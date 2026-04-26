import sqlite3

def check():
    conn = sqlite3.connect('data/db.db')
    cursor = conn.cursor()
    
    # Check articles table
    print("Articles table columns:")
    cursor.execute("PRAGMA table_info(articles)")
    for col in cursor.fetchall():
        print(f" - {col[1]}")
    
    # Check feeds table
    print("\nFeeds table columns:")
    cursor.execute("PRAGMA table_info(feeds)")
    for col in cursor.fetchall():
        print(f" - {col[1]}")
    
    conn.close()

if __name__ == "__main__":
    check()
