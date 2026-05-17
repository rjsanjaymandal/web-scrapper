import psycopg2

def list_tables():
    db_url = 'postgresql://postgres:njgeagyQ2tIfVpF9@db.syppmhoshwxzhjpqzvaz.supabase.co:5432/postgres'
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = cur.fetchall()
    print("=== TABLES ===")
    for t in tables:
        print(t[0])
    cur.close()
    conn.close()

if __name__ == '__main__':
    list_tables()
