import sqlite3
import psycopg2
import os
import sys

# Connect to local SQLite
local_conn = sqlite3.connect('scraper_local.db')
local_cur = local_conn.cursor()

# Get tables
local_cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in local_cur.fetchall()]
print(f"Local tables: {tables}")

# Check contacts table row count
for t in tables:
    local_cur.execute(f"SELECT COUNT(*) FROM {t}")
    count = local_cur.fetchone()[0]
    print(f"  {t}: {count} rows")

# If hosted DB exists, check it
db_url = os.environ.get('DATABASE_URL')
if db_url:
    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql://', 1)
    try:
        remote_conn = psycopg2.connect(db_url)
        remote_cur = remote_conn.cursor()
        remote_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        remote_tables = [r[0] for r in remote_cur.fetchall()]
        print(f"Remote tables: {remote_tables}")
        for t in remote_tables:
            remote_cur.execute(f"SELECT COUNT(*) FROM {t}")
            count = remote_cur.fetchone()[0]
            print(f"  {t}: {count} rows")
        remote_conn.close()
    except Exception as e:
        print(f"Remote DB error: {e}")
else:
    print("No DATABASE_URL - no remote DB")

local_conn.close()
print("\nDone.")