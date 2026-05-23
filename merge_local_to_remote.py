"""
Standalone script: Merge local SQLite contacts into remote PostgreSQL.
Usage:
    set DATABASE_URL=postgresql://user:pass@host:5432/dbname
    python merge_local_to_remote.py

Or:
    python merge_local_to_remote.py postgresql://user:pass@host:5432/dbname
"""
import sqlite3, asyncio, sys, os, json
from pathlib import Path

PROJ_DIR = Path(__file__).parent

async def merge():
    db_url = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: Provide DATABASE_URL as arg or env var")
        sys.exit(1)
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    # Read from SQLite
    db_path = PROJ_DIR / "scraper_local.db"
    if not db_path.exists():
        print(f"ERROR: Local DB not found at {db_path}")
        sys.exit(1)

    sqlite = sqlite3.connect(str(db_path))
    sqlite.row_factory = sqlite3.Row
    cur = sqlite.execute("SELECT * FROM contacts ORDER BY id")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    sqlite.close()
    print(f"Read {len(rows)} contacts from local SQLite")

    if not rows:
        print("Nothing to merge.")
        return

    # Connect to remote PostgreSQL
    import asyncpg
    conn = await asyncpg.connect(db_url, ssl="require")
    try:
        # Ensure table exists
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id SERIAL PRIMARY KEY,
                name TEXT, phone VARCHAR(50), email TEXT,
                address TEXT, category TEXT, city TEXT, area TEXT, state TEXT,
                source TEXT, source_url TEXT, phone_clean VARCHAR(50),
                email_valid BOOLEAN, enriched BOOLEAN,
                arn TEXT, license_no TEXT, membership_no TEXT,
                quality_score INT DEFAULT 0, quality_tier VARCHAR(20) DEFAULT 'low',
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Ensure unique index on phone_clean
        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_phone_clean_unique
            ON contacts(phone_clean) WHERE phone_clean IS NOT NULL
        """)

        saved = 0
        skipped = 0
        errors = 0
        for row in rows:
            try:
                result = await conn.execute("""
                    INSERT INTO contacts
                        (name, phone, email, address, category, city, area, state, source, source_url,
                         phone_clean, email_valid, enriched, arn, license_no, membership_no,
                         quality_score, quality_tier, scraped_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
                    ON CONFLICT (phone_clean) WHERE phone_clean IS NOT NULL DO NOTHING
                """,
                    row.get("name", ""), row.get("phone", ""), row.get("email", ""),
                    row.get("address", ""), row.get("category", ""), row.get("city", ""),
                    row.get("area", ""), row.get("state", ""), row.get("source", ""),
                    row.get("source_url", ""), row.get("phone_clean", ""),
                    bool(row.get("email_valid")), bool(row.get("enriched")),
                    row.get("arn", ""), row.get("license_no", ""), row.get("membership_no", ""),
                    row.get("quality_score", 0), row.get("quality_tier", "low"),
                    row.get("scraped_at"),
                )
                if "INSERT 0 1" in result:
                    saved += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"  Error on row {row.get('id')}: {e}")
                errors += 1

        print(f"\nMerge complete: {saved} saved, {skipped} skipped (duplicates), {errors} errors")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(merge())
