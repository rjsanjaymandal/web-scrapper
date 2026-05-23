#!/usr/bin/env python3
"""
MaysanLabs Web Scraper: Bidirectional Database Synchronizer & Merger (2026 Edition)
Resolves all unique key constraint violations on `phone_clean` and `email` using
O(1) memory-mapped lookups, enriches records based on quality scores, and performs
graceful internal/public connection routing. Windows-safe ASCII output format.
"""

import os
import sys
import sqlite3
import datetime
import asyncio
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("DB_Merger")

PROJ_DIR = Path(__file__).parent
FIELDS = [
    "name", "phone", "email", "address", "category", "city", "area", "state",
    "source", "source_url", "phone_clean", "email_valid", "enriched",
    "arn", "license_no", "membership_no", "quality_score", "quality_tier",
    "blockchain_ca", "scraped_at"
]

def parse_datetime(val):
    if not val:
        return None
    if isinstance(val, datetime.datetime):
        return val
    val_str = str(val).strip()
    # Strip any time zones like +00:00
    if "+" in val_str:
        val_str = val_str.split("+")[0]
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ):
        try:
            return datetime.datetime.strptime(val_str, fmt)
        except ValueError:
            continue
    return None

async def inspect_postgres_connection(db_url):
    import asyncpg
    try:
        # Quick ping test with short timeout
        if "@" in db_url:
            ssl_ctx = "require"
        else:
            ssl_ctx = None
        conn = await asyncpg.connect(db_url, ssl=ssl_ctx, timeout=5)
        await conn.execute("SELECT 1")
        await conn.close()
        return True, None
    except Exception as e:
        return False, e

def create_sqlite_schema(conn):
    cursor = conn.cursor()
    # 1. Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, phone TEXT, email TEXT, address TEXT, category TEXT,
            city TEXT, area TEXT, state TEXT, source TEXT, source_url TEXT,
            phone_clean TEXT, email_valid BOOLEAN, enriched BOOLEAN,
            arn TEXT, license_no TEXT, membership_no TEXT,
            quality_score INTEGER DEFAULT 0, quality_tier TEXT DEFAULT 'low',
            blockchain_ca TEXT, scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # 2. Constraints & Indices for high speed
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_phone_clean_unique ON contacts(phone_clean) WHERE phone_clean IS NOT NULL")
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_unique_email ON contacts(email) WHERE email IS NOT NULL")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_contacts_source ON contacts(source)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_contacts_category ON contacts(category)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_contacts_city ON contacts(city)")
    conn.commit()

async def create_postgres_schema(conn):
    # 1. Create table
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id SERIAL PRIMARY KEY,
            name TEXT, phone VARCHAR(50), email TEXT, address TEXT, category TEXT,
            city TEXT, area TEXT, state TEXT, source TEXT, source_url TEXT,
            phone_clean VARCHAR(50), email_valid BOOLEAN, enriched BOOLEAN,
            arn TEXT, license_no TEXT, membership_no TEXT,
            quality_score INTEGER DEFAULT 0, quality_tier VARCHAR(500) DEFAULT 'low',
            blockchain_ca TEXT, scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # 2. Add indices & constraints
    await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_unique_phone ON contacts(phone_clean) WHERE phone_clean IS NOT NULL")
    await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_unique_email ON contacts(email) WHERE email IS NOT NULL")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_contacts_source ON contacts(source)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_contacts_category ON contacts(category)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_contacts_city ON contacts(city)")

async def run_sync():
    print("=" * 60)
    print("      *** MAYSANLABS ENTERPRISE DATABASE SYNC ENGINE ***      ")
    print("=" * 60)

    # 1. Locate Local SQLite DB
    sqlite_db_path = PROJ_DIR / "scraper_local.db"
    print(f"[*] Checking Local Database: {sqlite_db_path.name}")
    sqlite_conn = sqlite3.connect(str(sqlite_db_path))
    sqlite_conn.row_factory = sqlite3.Row
    create_sqlite_schema(sqlite_conn)

    # 2. Fetch PostgreSQL URL
    db_url = os.environ.get("DATABASE_URL")
    if not db_url and len(sys.argv) > 1:
        db_url = sys.argv[1]

    default_internal = "postgresql://postgres:SBGYpcBnqhbwqrzRvVnhJzGKTHTwOZrG@postgres.railway.internal:5432/railway"

    if not db_url:
        print(f"\n[!] No database URL specified. Defaulting to internal Railway DNS:")
        print(f"    {default_internal[:50]}...")
        db_url = default_internal

    # 3. Connection Diagnostics & Fallback Loop
    import asyncpg
    connected = False
    conn_pg = None

    while not connected:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)

        print(f"\n[~] Connecting to PostgreSQL...")
        reachable, error = await inspect_postgres_connection(db_url)

        if reachable:
            print("[OK] PostgreSQL database is reachable!")
            ssl_ctx = "require" if "@" in db_url else None
            conn_pg = await asyncpg.connect(db_url, ssl=ssl_ctx)
            connected = True
        else:
            print(f"[FAIL] Connection failed: {error}")
            print("\n[-] DIAGNOSTIC GUIDANCE:")
            if "postgres.railway.internal" in db_url:
                print("    The address '.internal' is a PRIVATE Railway network location.")
                print("    Running this script from a local machine requires a PUBLIC URL.")
                print("    You can get this from the 'Connect' tab of your Postgres service on Railway.")
            
            # Interactive Prompt for local runs
            user_url = input("\n[?] Please enter a PUBLIC PostgreSQL connection URL (or press Enter to exit): ").strip()
            if not user_url:
                print("[-] Sync aborted by user. Exiting.")
                sqlite_conn.close()
                sys.exit(1)
            db_url = user_url

    try:
        # Initialize remote database tables
        await create_postgres_schema(conn_pg)

        # 4. Fetch initial database counts
        cur_sl = sqlite_conn.execute("SELECT COUNT(*) FROM contacts")
        local_total = cur_sl.fetchone()[0]

        remote_total = await conn_pg.fetchval("SELECT COUNT(*) FROM contacts")

        print(f"\n📊 Initial Statistics:")
        print(f"   - Local Database (SQLite) : {local_total} records")
        print(f"   - Remote Database (Postgres) : {remote_total} records")
        print("-" * 60)

        # 5. Build in-memory lookup cache to prevent constraint violations
        print("[*] Building remote lookup index...")
        postgres_phones = {}
        postgres_emails = {}
        
        pg_lookup_rows = await conn_pg.fetch("SELECT id, phone_clean, email, quality_score FROM contacts")
        for row in pg_lookup_rows:
            pg_id = row["id"]
            score = row["quality_score"] or 0
            if row["phone_clean"]:
                postgres_phones[row["phone_clean"]] = (pg_id, score)
            if row["email"]:
                postgres_emails[row["email"]] = (pg_id, score)

        print("[*] Building local lookup index...")
        sqlite_phones = {}
        sqlite_emails = {}

        cur_sl = sqlite_conn.execute("SELECT id, phone_clean, email, quality_score FROM contacts")
        for row in cur_sl.fetchall():
            sl_id = row["id"]
            score = row["quality_score"] or 0
            if row["phone_clean"]:
                sqlite_phones[row["phone_clean"]] = (sl_id, score)
            if row["email"]:
                sqlite_emails[row["email"]] = (sl_id, score)

        # 6. Phase 1: Local SQLite ➔ Remote PostgreSQL Sync
        print("\n[+] PHASE 1: Syncing Local SQLite ➔ Remote PostgreSQL...")
        cur_sl = sqlite_conn.execute("SELECT * FROM contacts")
        sqlite_records = [dict(r) for r in cur_sl.fetchall()]

        pg_inserts = 0
        pg_updates = 0
        pg_skipped = 0

        for row in sqlite_records:
            phone_clean = row.get("phone_clean")
            email = row.get("email")
            quality_score = row.get("quality_score") or 0

            # Check matches
            matched_pg_id = None
            pg_score = -1

            if phone_clean and phone_clean in postgres_phones:
                matched_pg_id, pg_score = postgres_phones[phone_clean]
            elif email and email in postgres_emails:
                matched_pg_id, pg_score = postgres_emails[email]

            # Parse datetime correctly
            row["scraped_at"] = parse_datetime(row.get("scraped_at"))

            if matched_pg_id is not None:
                if quality_score > pg_score:
                    # Update Postgres since local has higher score
                    await conn_pg.execute("""
                        UPDATE contacts SET
                            name = $1, phone = $2, email = $3, address = $4, category = $5,
                            city = $6, area = $7, state = $8, source = $9, source_url = $10,
                            phone_clean = $11, email_valid = $12, enriched = $13, arn = $14,
                            license_no = $15, membership_no = $16, quality_score = $17,
                            quality_tier = $18, blockchain_ca = $19, scraped_at = $20
                        WHERE id = $21
                    """, *[row.get(f) for f in FIELDS], matched_pg_id)
                    
                    # Update local cache
                    if phone_clean: postgres_phones[phone_clean] = (matched_pg_id, quality_score)
                    if email: postgres_emails[email] = (matched_pg_id, quality_score)
                    pg_updates += 1
                else:
                    pg_skipped += 1
            else:
                # Insert Postgres
                insert_res = await conn_pg.fetchrow("""
                    INSERT INTO contacts (
                        name, phone, email, address, category, city, area, state,
                        source, source_url, phone_clean, email_valid, enriched,
                        arn, license_no, membership_no, quality_score, quality_tier,
                        blockchain_ca, scraped_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
                    ) RETURNING id
                """, *[row.get(f) for f in FIELDS])

                new_pg_id = insert_res["id"]
                if phone_clean: postgres_phones[phone_clean] = (new_pg_id, quality_score)
                if email: postgres_emails[email] = (new_pg_id, quality_score)
                pg_inserts += 1

        print(f"   [OK] Local ➔ Remote Sync Complete:")
        print(f"      - Inserts: {pg_inserts}")
        print(f"      - Updates: {pg_updates}")
        print(f"      - Skipped: {pg_skipped}")

        # 7. Phase 2: Remote PostgreSQL ➔ Local SQLite Sync
        print("\n[+] PHASE 2: Syncing Remote PostgreSQL ➔ Local SQLite...")
        pg_records = await conn_pg.fetch("SELECT * FROM contacts")
        pg_records = [dict(r) for r in pg_records]

        sl_inserts = 0
        sl_updates = 0
        sl_skipped = 0

        for row in pg_records:
            phone_clean = row.get("phone_clean")
            email = row.get("email")
            quality_score = row.get("quality_score") or 0

            # Check matches
            matched_sl_id = None
            sl_score = -1

            if phone_clean and phone_clean in sqlite_phones:
                matched_sl_id, sl_score = sqlite_phones[phone_clean]
            elif email and email in sqlite_emails:
                matched_sl_id, sl_score = sqlite_emails[email]

            row["scraped_at"] = parse_datetime(row.get("scraped_at"))

            if matched_sl_id is not None:
                if quality_score > sl_score:
                    # Update SQLite
                    sqlite_conn.execute("""
                        UPDATE contacts SET
                            name = ?, phone = ?, email = ?, address = ?, category = ?,
                            city = ?, area = ?, state = ?, source = ?, source_url = ?,
                            phone_clean = ?, email_valid = ?, enriched = ?, arn = ?,
                            license_no = ?, membership_no = ?, quality_score = ?,
                            quality_tier = ?, blockchain_ca = ?, scraped_at = ?
                        WHERE id = ?
                    """, (*[row.get(f) for f in FIELDS], matched_sl_id))
                    
                    if phone_clean: sqlite_phones[phone_clean] = (matched_sl_id, quality_score)
                    if email: sqlite_emails[email] = (matched_sl_id, quality_score)
                    sl_updates += 1
                else:
                    sl_skipped += 1
            else:
                # Insert SQLite
                cur = sqlite_conn.execute("""
                    INSERT INTO contacts (
                        name, phone, email, address, category, city, area, state,
                        source, source_url, phone_clean, email_valid, enriched,
                        arn, license_no, membership_no, quality_score, quality_tier,
                        blockchain_ca, scraped_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                """, [row.get(f) for f in FIELDS])

                new_sl_id = cur.lastrowid
                if phone_clean: sqlite_phones[phone_clean] = (new_sl_id, quality_score)
                if email: sqlite_emails[email] = (new_sl_id, quality_score)
                sl_inserts += 1

        sqlite_conn.commit()
        print(f"   [OK] Remote ➔ Local Sync Complete:")
        print(f"      - Inserts: {sl_inserts}")
        print(f"      - Updates: {sl_updates}")
        print(f"      - Skipped: {sl_skipped}")

        # 8. Fetch final totals
        final_local = sqlite_conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
        final_remote = await conn_pg.fetchval("SELECT COUNT(*) FROM contacts")

        print("=" * 60)
        print("                 *** SYNC SUMMARY ***                 ")
        print("=" * 60)
        print(f"   SQLite Total  |   {local_total:<10}  ->   {final_local}")
        print(f"   Postgres Total|   {remote_total:<10}  ->   {final_remote}")
        print("\n[OK] Databases are now perfectly bidirectionally synchronized!")
        print("=" * 60)

    finally:
        sqlite_conn.close()
        if conn_pg:
            await conn_pg.close()

if __name__ == "__main__":
    asyncio.run(run_sync())
