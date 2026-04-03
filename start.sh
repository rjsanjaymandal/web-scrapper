#!/bin/bash
set -e

echo "Starting Contact Scraper..."

# Wait for database
echo "Waiting for database..."
while ! nc -z $DATABASE_HOST $DATABASE_PORT; do
  sleep 1
done
echo "Database is ready!"

# Run database migrations
echo "Initializing database..."
python -c "
import asyncio
import asyncpg
import os

async def init_db():
    pool = await asyncpg.create_pool(
        host=os.getenv('DATABASE_HOST'),
        port=int(os.getenv('DATABASE_PORT', 5432)),
        database=os.getenv('DATABASE_NAME'),
        user=os.getenv('DATABASE_USER'),
        password=os.getenv('DATABASE_PASSWORD')
    )
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS contacts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                phone VARCHAR(50),
                email VARCHAR(255),
                address TEXT,
                category VARCHAR(100),
                city VARCHAR(100),
                area VARCHAR(100),
                source VARCHAR(100),
                source_url TEXT,
                phone_clean VARCHAR(50),
                email_valid BOOLEAN,
                enriched BOOLEAN,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone_clean)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email)')
    await pool.close()
    print('Database initialized!')

asyncio.run(init_db())
"

# Start the application
echo "Starting dashboard..."
exec python dashboard.py