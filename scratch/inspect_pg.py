import sys
import psycopg2

def inspect_pg():
    db_url = 'postgresql://postgres:SBGYpcBnqhbwqrzRvVnhJzGKTHTwOZrG@postgres.railway.internal:5432/railway'
    print(f"Connecting to PostgreSQL database: {db_url.split('@')[1]}...")
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        tables = cur.fetchall()
        print("Tables in PostgreSQL database:")
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t[0]}")
            count = cur.fetchone()[0]
            print(f"- {t[0]} (Count: {count})")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)

if __name__ == '__main__':
    inspect_pg()
