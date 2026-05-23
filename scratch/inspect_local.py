import sqlite3

def inspect():
    conn = sqlite3.connect("scraper_local.db")
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print("Tables in SQLite database:")
    for t in tables:
        print(f"- {t[0]}")
        cursor.execute(f"SELECT COUNT(*) FROM {t[0]}")
        count = cursor.fetchone()[0]
        print(f"  Count: {count}")
        
    cursor.execute("PRAGMA table_info(contacts)")
    columns = cursor.fetchall()
    print("\nColumns in 'contacts' table:")
    for col in columns:
        print(f"  {col[1]} ({col[2]})")
    
    conn.close()

if __name__ == '__main__':
    inspect()
