import sqlite3
from pathlib import Path

def check_logs():
    db_path = Path("scraper_local.db")
    if not db_path.exists():
        print("Database not found")
        return
        
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT level, message, source, created_at FROM scraper_logs ORDER BY created_at DESC LIMIT 30")
    rows = cur.fetchall()
    
    print("Recent logs in database:")
    for row in rows:
        print(f"[{row[3]}] {row[0]} | {row[2]} | {row[1]}")
        
    conn.close()

if __name__ == "__main__":
    check_logs()
