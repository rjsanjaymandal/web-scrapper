"""
Merge local SQLite data into hosted PostgreSQL
Usage: python merge_to_hosted.py
Requires DATABASE_URL environment variable for hosted DB
"""

import os
import sys
import sqlite3
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

def get_remote_connection():
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        return None
    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql://', 1)
    return psycopg2.connect(db_url)

def get_local_contacts():
    conn = sqlite3.connect('scraper_local.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM contacts")
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_remote_columns(cursor):
    cursor.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'contacts' ORDER BY ordinal_position
    """)
    return [r[0] for r in cursor.fetchall()]

def merge():
    print("=" * 50)
    print("MERGING LOCAL DATA TO HOSTED DATABASE")
    print("=" * 50)

    local_contacts = get_local_contacts()
    print(f"Local contacts: {len(local_contacts)}")

    if not local_contacts:
        print("No local data to merge.")
        return

    remote_conn = get_remote_connection()
    if not remote_conn:
        print("ERROR: Could not connect to remote DB")
        return

    remote_cur = remote_conn.cursor()
    remote_cols = get_remote_columns(remote_cur)
    print(f"Remote columns: {remote_cols}")

    # Check current remote count
    remote_cur.execute("SELECT COUNT(*) FROM contacts")
    remote_count = remote_cur.fetchone()[0]
    print(f"Remote contacts before: {remote_count}")

    # Check for duplicates (by email or phone + source)
    inserted = 0
    skipped = 0

    for contact in local_contacts:
        email = contact.get('email')
        phone = contact.get('phone') or contact.get('phone_clean')
        source = contact.get('source')

        # Check if exists
        if email:
            remote_cur.execute(
                "SELECT id FROM contacts WHERE email = %s",
                (email,)
            )
            if remote_cur.fetchone():
                skipped += 1
                continue
        elif phone and source:
            remote_cur.execute(
                "SELECT id FROM contacts WHERE (phone = %s OR phone_clean = %s) AND source = %s",
                (phone, phone, source)
            )
            if remote_cur.fetchone():
                skipped += 1
                continue

        # Insert new contact
        cols_to_insert = [c for c in remote_cols if c in contact and contact[c] is not None]
        vals = [contact.get(c) for c in cols_to_insert]

        placeholders = ', '.join(['%s'] * len(cols_to_insert))
        cols_str = ', '.join(cols_to_insert)
        sql = f"INSERT INTO contacts ({cols_str}) VALUES ({placeholders})"

        try:
            remote_cur.execute(sql, vals)
            inserted += 1
        except Exception as e:
            print(f"Insert error: {e}")
            skipped += 1

    remote_conn.commit()

    remote_cur.execute("SELECT COUNT(*) FROM contacts")
    final_count = remote_cur.fetchone()[0]

    print(f"\nRESULTS:")
    print(f"  Inserted: {inserted}")
    print(f"  Skipped (duplicates): {skipped}")
    print(f"  Remote before: {remote_count}")
    print(f"  Remote after: {final_count}")

    remote_cur.close()
    remote_conn.close()

    print("\n✓ Merge complete!")

if __name__ == "__main__":
    merge()