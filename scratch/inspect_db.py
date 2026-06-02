import sqlite3
import os
from pathlib import Path

def inspect():
    db_path = Path("scraper_local.db")
    if not db_path.exists():
        print("scraper_local.db does not exist in the current folder.")
        return
        
    print(f"Database file size: {db_path.stat().st_size} bytes")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Check tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cur.fetchall()]
    print(f"Tables: {tables}")
    
    if "contacts" in tables:
        cur.execute("SELECT COUNT(*) FROM contacts")
        total = cur.fetchone()[0]
        print(f"Total contacts: {total}")
        
        # Check empty or raw fields
        cur.execute("SELECT COUNT(*) FROM contacts WHERE phone IS NULL OR phone = ''")
        no_phone = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM contacts WHERE phone_clean IS NULL OR phone_clean = ''")
        no_phone_clean = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM contacts WHERE email IS NULL OR email = ''")
        no_email = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM contacts WHERE email_valid = 0 OR email_valid IS NULL")
        invalid_email = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM contacts WHERE name IS NULL OR name = ''")
        no_name = cur.fetchone()[0]
        
        print(f"Contacts with empty/null phone: {no_phone}")
        print(f"Contacts with empty/null phone_clean: {no_phone_clean}")
        print(f"Contacts with empty/null email: {no_email}")
        print(f"Contacts with invalid email: {invalid_email}")
        print(f"Contacts with empty/null name: {no_name}")
        
        # Check tier distribution
        cur.execute("SELECT quality_tier, COUNT(*), AVG(quality_score) FROM contacts GROUP BY quality_tier")
        tiers = cur.fetchall()
        print("Tiers distribution:")
        for t in tiers:
            print(f"  Tier: {t[0]}, Count: {t[1]}, Avg Score: {t[2]}")
            
        # Check duplicates based on phone_clean
        cur.execute("SELECT phone_clean, COUNT(*) as cnt FROM contacts WHERE phone_clean IS NOT NULL AND phone_clean <> '' GROUP BY phone_clean HAVING cnt > 1")
        dupes_phone = cur.fetchall()
        print(f"Duplicate phones (phone_clean): {len(dupes_phone)}")
        
        # Check duplicates based on email
        cur.execute("SELECT email, COUNT(*) as cnt FROM contacts WHERE email IS NOT NULL AND email <> '' GROUP BY email HAVING cnt > 1")
        dupes_email = cur.fetchall()
        print(f"Duplicate emails: {len(dupes_email)}")
        
        # Check some sample contacts
        cur.execute("SELECT id, name, phone, phone_clean, email, email_valid, quality_score, quality_tier FROM contacts LIMIT 5")
        samples = cur.fetchall()
        print("Sample records:")
        for s in samples:
            print(f"  ID: {s['id']}, Name: {s['name']}, Phone: {s['phone']}, Clean: {s['phone_clean']}, Email: {s['email']}, Valid: {s['email_valid']}, Score: {s['quality_score']}, Tier: {s['quality_tier']}")
            
    conn.close()

if __name__ == "__main__":
    inspect()
