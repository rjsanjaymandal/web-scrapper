import os
import sys
import time
import subprocess
import traceback
from urllib.parse import urlparse

# Global diagnostic wrapper to catch early import errors
try:
    import psycopg2
except ImportError as e:
    print(f"❌ [BOOTSTRAP] CRITICAL: Missing dependency 'psycopg2': {e}", flush=True)
    sys.exit(1)

def log(msg):
    print(f"🚀 [BOOTSTRAP] {msg}", flush=True)

def wait_for_db():
    try:
        db_url = os.environ.get('DATABASE_URL')
        if db_url:
            if db_url.startswith('postgres://'):
                db_url = db_url.replace('postgres://', 'postgresql://', 1)
            url = urlparse(db_url)
            host, port = url.hostname, url.port or 5432
        else:
            host = os.environ.get('DATABASE_HOST', 'localhost')
            port = int(os.environ.get('DATABASE_PORT', 5432))
        
        log(f"Waiting for database at {host}:{port}...")
        start_time = time.time()
        timeout = 90
        while time.time() - start_time < timeout:
            try:
                conn = psycopg2.connect(
                    dsn=db_url if os.environ.get('DATABASE_URL') else None,
                    host=None if os.environ.get('DATABASE_URL') else host,
                    port=None if os.environ.get('DATABASE_URL') else port,
                    user=os.environ.get('DATABASE_USER'),
                    password=os.environ.get('DATABASE_PASSWORD'),
                    database=os.environ.get('DATABASE_NAME'),
                    connect_timeout=3
                )
                conn.close()
                log(f"Database at {host}:{port} is reachable!")
                return True
            except Exception:
                time.sleep(1)
        
        log("❌ Timeout waiting for database after 90s")
        return False
    except Exception as e:
        log(f"❌ Error in wait_for_db: {e}")
        traceback.print_exc()
        return False

def init_tables():
    log("Running eager database initialization...")
    try:
        import dashboard
        success = dashboard.init_tables()
        if not success:
            log("❌ Database initialization failed via dashboard.init_tables()")
            return False
        log("✅ Database tables ready!")
        return True
    except Exception as e:
        log(f"❌ Failed to import or run dashboard init: {e}")
        traceback.print_exc()
        return False

def main():
    try:
        # Check command line flags first, fallback to env var
        is_worker = "--worker" in sys.argv
        process_type = "worker" if is_worker else os.environ.get("PROCESS_TYPE", "web")
        
        log(f"Starting {process_type} process sequence...")

        # Shared DB Check
        if not wait_for_db():
            sys.exit(1)

        # Web-specific Init
        if process_type == "web":
            if not init_tables():
                sys.exit(1)
            
            port = os.environ.get("PORT", "8080")
            log(f"Launching Gunicorn on 0.0.0.0:{port}")
            
            cmd = [
                "gunicorn", "dashboard:app",
                "--bind", f"0.0.0.0:{port}",
                "--workers", "1",
                "--threads", "4",
                "--timeout", "120",
                "--preload",
                "--access-logfile", "-",
                "--error-logfile", "-"
            ]
            os.execvp(cmd[0], cmd)

        elif process_type == "worker":
            log("Launching Celery Worker")
            cmd = [
                "celery", "-A", "tasks.celery_app", "worker",
                "--loglevel=info",
                "--pool=solo",
                "--concurrency=1"
            ]
            os.execvp(cmd[0], cmd)
        
        else:
            log(f"❌ Unknown PROCESS_TYPE: {process_type}")
            sys.exit(1)
            
    except Exception as e:
        log(f"❌ CRITICAL FAILURE in main loop: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
