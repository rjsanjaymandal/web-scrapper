import sqlite3
import os
from pathlib import Path
from processing import ProcessingHandler

def run_deep_clean():
    db_path = Path("scraper_local.db")
    if not db_path.exists():
        print("Database not found")
        return
        
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # 1. Print status before deep clean
    print("=== Database State BEFORE Deep Clean ===")
    cur.execute("SELECT COUNT(*) FROM contacts")
    total_before = cur.fetchone()[0]
    print(f"Total contacts: {total_before}")
    
    cur.execute("SELECT quality_tier, COUNT(*), AVG(quality_score) FROM contacts GROUP BY quality_tier")
    for t in cur.fetchall():
        print(f"  Tier: {t[0]}, Count: {t[1]}, Avg Score: {t[2]}")
        
    cur.execute("SELECT id, name, email, quality_score, quality_tier FROM contacts WHERE id IN (1, 2, 5)")
    print("Sample records before:")
    for s in cur.fetchall():
        print(f"  ID: {s['id']} | Name: {s['name']} | Email: {s['email']} | Score: {s['quality_score']} | Tier: {s['quality_tier']}")
        
    # 2. Run clean logic
    cur.execute("SELECT * FROM contacts")
    rows = cur.fetchall()
    
    deleted = 0
    updated = 0
    
    print("\nProcessing deep clean...")
    for row in rows:
        contact = dict(row)
        contact_id = contact['id']
        
        cleaned = ProcessingHandler.process_contact(contact)
        
        if cleaned is None:
            cur.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
            deleted += 1
            continue
            
        if (cleaned.get('phone') != row['phone'] or 
            cleaned.get('email') != row['email'] or 
            cleaned.get('name') != row['name'] or 
            cleaned.get('category') != row['category'] or
            cleaned.get('quality_score') != row['quality_score'] or
            cleaned.get('quality_tier') != row['quality_tier']):
            
            cur.execute(
                """UPDATE contacts SET 
                    name = ?, phone = ?, phone_clean = ?, email = ?, 
                    email_valid = ?, category = ?, quality_score = ?, 
                    quality_tier = ?, enriched = ? 
                   WHERE id = ?""",
                (cleaned.get('name'), cleaned.get('phone'), cleaned.get('phone_clean'), 
                 cleaned.get('email'), cleaned.get('email_valid'), cleaned.get('category'), 
                 cleaned.get('quality_score'), cleaned.get('quality_tier'), True, contact_id)
            )
            updated += 1
            
    conn.commit()
    print(f"Deep clean complete: Deleted {deleted} records, Updated {updated} records.")
    
    # 3. Print status after deep clean
    print("\n=== Database State AFTER Deep Clean ===")
    cur.execute("SELECT COUNT(*) FROM contacts")
    total_after = cur.fetchone()[0]
    print(f"Total contacts: {total_after}")
    
    cur.execute("SELECT quality_tier, COUNT(*), AVG(quality_score) FROM contacts GROUP BY quality_tier")
    for t in cur.fetchall():
        print(f"  Tier: {t[0]}, Count: {t[1]}, Avg Score: {t[2]}")
        
    cur.execute("SELECT id, name, email, quality_score, quality_tier FROM contacts WHERE id IN (1, 2, 5)")
    print("Sample records after:")
    for s in cur.fetchall():
        print(f"  ID: {s['id']} | Name: {s['name']} | Email: {s['email']} | Score: {s['quality_score']} | Tier: {s['quality_tier']}")
        
    conn.close()

if __name__ == "__main__":
    run_deep_clean()
