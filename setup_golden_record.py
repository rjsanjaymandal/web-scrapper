
import os
import psycopg2
from pathlib import Path

def get_db_url():
    db_url = os.environ.get('DATABASE_URL')
    if db_url: return db_url
    # Fallback to defaults
    return "postgresql://postgres@localhost:5432/scraper_db"

def setup():
    print("🚀 Initializing Golden Record Synthesis Engine...")
    url = get_db_url()
    try:
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        
        # 1. Add phone_clean column if missing (should exist, but let's be safe)
        cur.execute("""
            ALTER TABLE contacts 
            ADD COLUMN IF NOT EXISTS phone_clean VARCHAR(50);
        """)
        
        # 2. Create UNIQUE index on phone_clean (ESSENTIAL for Golden Record synthesis)
        print("Creating unique index on phone_clean...")
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_phone_clean 
            ON contacts (phone_clean) 
            WHERE phone_clean IS NOT NULL;
        """)
        
        # 3. Add quality columns if missing
        cur.execute("""
            ALTER TABLE contacts 
            ADD COLUMN IF NOT EXISTS quality_score INTEGER DEFAULT 0,
            ADD COLUMN IF NOT EXISTS quality_tier VARCHAR(20) DEFAULT 'low';
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Database is ready for high-fidelity lead synthesis!")
    except Exception as e:
        print(f"❌ Error setting up database: {e}")

if __name__ == "__main__":
    setup()
