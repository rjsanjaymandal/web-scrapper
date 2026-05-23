import sqlite3
import csv

conn = sqlite3.connect('scraper_local.db')
cur = conn.cursor()
cur.execute("SELECT * FROM contacts")
rows = cur.fetchall()
cur.execute("PRAGMA table_info(contacts)")
cols = [c[1] for c in cur.fetchall()]

with open('local_contacts_export.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(rows)

conn.close()
print(f"Exported {len(rows)} contacts to local_contacts_export.csv")