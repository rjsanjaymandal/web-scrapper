import os
import psycopg2
import yaml

def fix_duplicates():
    url = os.environ.get('DATABASE_URL')
    if not url:
        try:
            with open('config.yaml', 'r') as f:
                cfg = yaml.safe_load(f).get('database', {})
                url = f"postgresql://{cfg.get('user', 'postgres')}:{cfg.get('password', '')}@{cfg.get('host', 'localhost')}:{cfg.get('port', 5432)}/{cfg.get('name', 'scraper_db')}"
        except:
            print("❌ DATABASE_URL not found and config.yaml missing/malformed!")
            return

    try:
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        
        print("🔍 Searching for duplicate leads...")
        
        # Identify and remove duplicates keeping the one with the highest quality_score or latest ID
        cur.execute("""
            WITH duplicates AS (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY phone_clean 
                           ORDER BY quality_score DESC, scraped_at DESC, id DESC
                       ) as rank
                FROM contacts
                WHERE phone_clean IS NOT NULL AND phone_clean != ''
            )
            DELETE FROM contacts
            WHERE id IN (SELECT id FROM duplicates WHERE rank > 1);
        """)
        
        deleted_count = cur.rowcount
        print(f"✅ Removed {deleted_count} duplicate records.")
        
        print("⭐ Creating Unique Golden Record Constraint...")
        cur.execute("DROP INDEX IF EXISTS idx_contacts_phone_clean;") # Drop existing non-unique if exists
        cur.execute("""
            CREATE UNIQUE INDEX idx_contacts_phone_clean 
            ON contacts (phone_clean) 
            WHERE phone_clean IS NOT NULL AND phone_clean != '';
        """)
        
        conn.commit()
        print("✅ Database stabilized!")
        
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        if 'conn' in locals() and conn: conn.rollback()
    finally:
        if 'conn' in locals() and conn: conn.close()

if __name__ == "__main__":
    fix_duplicates()
