import sqlite3, json
conn = sqlite3.connect("scraper_local.db")
cur = conn.cursor()

# Schema
cur.execute("SELECT sql FROM sqlite_master WHERE type='table' ORDER BY name")
for row in cur.fetchall():
    print(f"SQL: {row[0]}")
    print()

# Counts
cur.execute("SELECT COUNT(*) FROM contacts")
print(f"Total contacts: {cur.fetchone()[0]}")

cat_counts = cur.execute("SELECT category, COUNT(*) FROM contacts GROUP BY category ORDER BY COUNT(*) DESC").fetchall()
print(f"\nCategories ({len(cat_counts)}):")
for cat, cnt in cat_counts:
    print(f"  {cat}: {cnt}")

cur.execute("SELECT COUNT(*) FROM contacts WHERE phone IS NOT NULL AND phone != ''")
print(f"With phone: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL AND email != ''")
print(f"With email: {cur.fetchone()[0]}")

cur.execute("SELECT * FROM contacts ORDER BY id DESC LIMIT 5")
cols = [d[0] for d in cur.description]
for r in cur.fetchall():
    d = dict(zip(cols, r))
    d = {k: (v if v else "") for k, v in d.items()}
    print(json.dumps(d, indent=2, default=str))

conn.close()
