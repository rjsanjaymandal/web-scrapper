import asyncio, os

async def verify():
    db_url = os.environ.get("DATABASE_URL")
    import asyncpg
    conn = await asyncpg.connect(db_url, ssl="require")
    try:
        # Count total
        total = await conn.fetchval("SELECT COUNT(*) FROM contacts")
        print(f"Total contacts in Railway PG: {total}")
        
        # Count by category
        cats = await conn.fetch("SELECT category, COUNT(*) as cnt FROM contacts GROUP BY category ORDER BY cnt DESC")
        print("\nBy category:")
        for r in cats:
            print(f"  {r['category']}: {r['cnt']}")
        
        # Count by source
        srcs = await conn.fetch("SELECT source, COUNT(*) as cnt FROM contacts GROUP BY source ORDER BY cnt DESC")
        print("\nBy source:")
        for r in srcs:
            print(f"  {r['source']}: {r['cnt']}")
        
        # Sample of new school contacts
        schools = await conn.fetch("SELECT name, city, phone, email FROM contacts WHERE category ILIKE '%school%' LIMIT 5")
        print("\nSample school contacts:")
        for r in schools:
            print(f"  {r['name']} | {r['city']} | {r['phone']} | {r['email']}")
        
        # Count with phone/email
        with_phone = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE phone IS NOT NULL AND phone != ''")
        with_email = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL AND email != ''")
        print(f"\nWith phone: {with_phone}")
        print(f"With email: {with_email}")
    finally:
        await conn.close()

asyncio.run(verify())
