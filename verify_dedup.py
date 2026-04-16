import asyncio
import os
import asyncpg
import json
from processing import ProcessingHandler

async def verify_dedup():
    print("Starting Deduplication Verification...")
    
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("DATABASE_URL not found!")
        return

    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql://', 1)

    conn = await asyncpg.connect(db_url)
    try:
        # 1. Clear test data or just prepare a unique test contact
        test_phone = "9999999999"
        test_email = "test@example.com"
        
        print(f"Cleaning test records for {test_phone}...")
        await conn.execute("DELETE FROM contacts WHERE phone_clean = $1 OR email = $2", test_phone, test_email)

        # 2. Insert first time
        contact = {
            "name": "Test User",
            "phone": test_phone,
            "email": test_email,
            "source": "TEST",
            "quality_score": 50
        }
        processed = ProcessingHandler.process_contact(contact)
        
        print("Inserting first record...")
        await conn.execute("""
            INSERT INTO contacts (name, phone, phone_clean, email, source, quality_score, quality_tier)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """, processed['name'], processed['phone'], processed['phone_clean'], 
             processed['email'], processed['source'], processed['quality_score'], processed['quality_tier'])

        # 3. Try to insert same phone again (UPSERT test)
        print("Attempting UPSERT with same phone but HIGHER quality...")
        higher_quality_contact = {
            "name": "Test User Improved",
            "phone": test_phone,
            "email": test_email,
            "source": "TEST_UPSERTER",
            "quality_score": 90
        }
        p2 = ProcessingHandler.process_contact(higher_quality_contact)
        
        # This mirrors scraper.py logic
        await conn.execute("""
            INSERT INTO contacts (name, phone, phone_clean, email, source, quality_score, quality_tier)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (phone_clean) WHERE phone_clean IS NOT NULL
            DO UPDATE SET
                quality_score = EXCLUDED.quality_score,
                quality_tier = EXCLUDED.quality_tier,
                name = EXCLUDED.name,
                source = EXCLUDED.source
            WHERE EXCLUDED.quality_score > contacts.quality_score
        """, p2['name'], p2['phone'], p2['phone_clean'], 
             p2['email'], p2['source'], p2['quality_score'], p2['quality_tier'])

        # 4. Verify count remains 1 and data is updated
        count = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE phone_clean = $1", test_phone)
        result = await conn.fetchrow("SELECT name, quality_score, source FROM contacts WHERE phone_clean = $1", test_phone)
        
        if count == 1 and result['quality_score'] == 90:
            print(f"SUCCESS: Count is {count}, Quality is {result['quality_score']}, Source is {result['source']}")
        else:
            print(f"FAILURE: Count is {count}, Result: {dict(result) if result else 'None'}")

    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(verify_dedup())
