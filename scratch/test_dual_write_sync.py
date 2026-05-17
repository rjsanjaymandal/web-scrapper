import os
import asyncio
import sqlite3
import asyncpg
from scraper import ContactScraper, load_config

async def test_dual_write():
    print("[TEST] Starting Dual-Write Database Sync Integration Test...")
    
    # 1. Boot up Scraper
    config = load_config()
    db = ContactScraper(config)
    
    # Force SQLite fallback mode for testing
    db.use_sqlite = True
    db.sqlite_conn = sqlite3.connect("scraper_local.db")
    db.sqlite_conn.row_factory = sqlite3.Row
    
    # Prepare tables locally
    db._create_sqlite_tables()
    
    # 2. Mock a test lead with random identifier to verify uniqueness
    import random
    test_id = random.randint(100000, 999999)
    test_email = f"sync_test_{test_id}@maysanlabs.com"
    test_phone = f"98765{test_id}"
    
    mock_lead = {
        "name": f"Sync Test School {test_id}",
        "phone": test_phone,
        "email": test_email,
        "address": "123 Dual Write Street",
        "category": "School",
        "city": "Mumbai",
        "source": "TEST_SYNC",
        "source_url": "http://sync-test.maysanlabs.com"
    }
    
    print(f"[TEST] Mock Lead created: {mock_lead['name']} | Email: {mock_lead['email']}")
    
    # 3. Save mock lead through save_contacts (which triggers SQLite save + PG Sync)
    print("[TEST] Triggering dual-save...")
    saved = await db.save_contacts([mock_lead])
    print(f"[TEST] Local SQLite Rowcount Result: {saved}")
    
    # Verify it is in local SQLite
    cur = db.sqlite_conn.cursor()
    cur.execute("SELECT * FROM contacts WHERE email = ?", (test_email,))
    local_row = cur.fetchone()
    if local_row:
        print(f"[TEST] Verified: Saved locally in SQLite! Name: {local_row['name']}")
    else:
        print("[TEST] Error: Missing from local SQLite database!")
        
    db.sqlite_conn.close()
    
    # 4. Check hosted PostgreSQL if DATABASE_URL is set in local environment
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("[TEST] DATABASE_URL not set in environment. PostgreSQL verification skipped.")
        return
        
    print(f"[TEST] Connecting to hosted database to verify sync: {db_url[:40]}...")
    
    # Wait briefly for the background asyncio task to finish writing to PostgreSQL
    print("[TEST] Waiting for async PG sync handler to complete...")
    await asyncio.sleep(4)
    
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
        
    ssl_ctx = "require" if "@" in db_url else None
    
    try:
        conn = await asyncpg.connect(db_url, ssl=ssl_ctx, timeout=10)
        
        hosted_row = await conn.fetchrow("SELECT * FROM contacts WHERE email = $1", test_email)
        if hosted_row:
            print(f"[TEST] SUCCESS: Fully synchronized to hosted PostgreSQL database!")
            print(f"[TEST] Hosted Row Name: {hosted_row['name']} | Scraped At: {hosted_row['scraped_at']}")
            
            # Clean up the test row from hosted database
            await conn.execute("DELETE FROM contacts WHERE email = $1", test_email)
            print("[TEST] Cleaned up test lead from hosted database.")
        else:
            print("[TEST] Error: Lead did NOT sync to the hosted PostgreSQL database!")
            
        await conn.close()
        
    except Exception as e:
        print(f"[TEST] Failed to connect to hosted database for verification: {e}")

if __name__ == "__main__":
    asyncio.run(test_dual_write())
