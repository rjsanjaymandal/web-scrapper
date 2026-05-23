import sqlite3, json, re

conn = sqlite3.connect("scraper_local.db")
conn.row_factory = sqlite3.Row
cur = conn.execute("SELECT * FROM contacts ORDER BY id")
rows = [dict(r) for r in cur.fetchall()]
conn.close()

lines = [
    "BEGIN;",
    "CREATE TABLE IF NOT EXISTS contacts (",
    "    id SERIAL PRIMARY KEY,",
    "    name TEXT, phone VARCHAR(50), email TEXT,",
    "    address TEXT, category TEXT, city TEXT, area TEXT, state TEXT,",
    "    source TEXT, source_url TEXT, phone_clean VARCHAR(50),",
    "    email_valid BOOLEAN, enriched BOOLEAN,",
    "    arn TEXT, license_no TEXT, membership_no TEXT,",
    "    quality_score INT DEFAULT 0, quality_tier VARCHAR(20) DEFAULT 'low',",
    "    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    ");",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_phone_clean_unique",
    "    ON contacts(phone_clean) WHERE phone_clean IS NOT NULL;",
    "",
]

for r in rows:
    def esc(v):
        if v is None or v == "":
            return "NULL"
        v = str(v).replace("'", "''")
        return f"'{v}'"

    phone_clean = r.get("phone_clean", "") or r.get("phone", "") or ""
    phone_clean = re.sub(r"[^0-9]", "", phone_clean) if phone_clean else None

    cols = [
        "name", "phone", "email", "address", "category", "city",
        "area", "state", "source", "source_url", "phone_clean",
        "email_valid", "enriched", "arn", "license_no", "membership_no",
        "quality_score", "quality_tier", "scraped_at"
    ]
    vals = []
    for c in cols:
        v = r.get(c, "")
        if c in ("email_valid", "enriched"):
            vals.append("TRUE" if v else "FALSE")
        elif c in ("quality_score",):
            vals.append(str(v or 0))
        else:
            v = phone_clean if c == "phone_clean" and not v else v
            vals.append(esc(v))

    lines.append(
        "INSERT INTO contacts (" + ", ".join(cols) + ") VALUES ("
        + ", ".join(vals) + ") ON CONFLICT (phone_clean) WHERE phone_clean IS NOT NULL DO NOTHING;"
    )

lines.append("COMMIT;")

sql = "\n".join(lines)
with open("local_contacts_import.sql", "w", encoding="utf-8") as f:
    f.write(sql)

print(f"Exported {len(rows)} contacts to local_contacts_import.sql ({len(sql)} bytes)")
