import sqlite3, json

conn = sqlite3.connect("scraper_local.db")
cur = conn.cursor()

cur.execute("SELECT * FROM contacts ORDER BY id")
cols = [d[0] for d in cur.description]
contacts = [dict(zip(cols, row)) for row in cur.fetchall()]

# Convert types for JSON
for c in contacts:
    for k, v in c.items():
        if isinstance(v, bytes):
            c[k] = v.decode()
        elif v is None:
            c[k] = ""
        if k == "scraped_at" and v:
            c[k] = str(v)

with open("local_contacts_export.json", "w") as f:
    json.dump(contacts, f, indent=2, default=str)

print(f"Exported {len(contacts)} contacts to local_contacts_export.json")

conn.close()
