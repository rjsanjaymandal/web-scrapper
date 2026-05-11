import os
import psycopg2

def check():
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("DATABASE_URL not found")
        return
    
    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql://', 1)
        
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    
    print("--- TRIGGERS ---")
    cur.execute("SELECT trigger_name, event_object_table FROM information_schema.triggers")
    for row in cur.fetchall():
        print(row)
        
    print("\n--- ROUTINES (Functions) ---")
    cur.execute("SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'public'")
    for row in cur.fetchall():
        print(row)
        
    cur.close()
    conn.close()

if __name__ == '__main__':
    check()
