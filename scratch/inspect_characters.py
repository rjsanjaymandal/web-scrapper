import sqlite3
from pathlib import Path

def inspect():
    db_path = Path("scraper_local.db")
    if not db_path.exists():
        print("Database not found")
        return
        
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM contacts WHERE name LIKE 'School Dekho%' OR id IN (1, 2, 3, 4, 5)")
    rows = cur.fetchall()
    
    for row in rows:
        cid, name = row
        print(f"ID: {cid} | Name: {name}")
        print(f"  Unicode points: {[ord(c) for c in name]}")
        print(f"  Hex representation: {[hex(ord(c)) for c in name]}")
        
    conn.close()

if __name__ == "__main__":
    inspect()
