import os
import sys
# IMMEDIATE STDOUT/STDERR UNBUFFERING
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
sys.stderr.reconfigure(line_buffering=True) if hasattr(sys.stderr, 'reconfigure') else None

import time
import subprocess
import traceback
import socket
import threading
from urllib.parse import urlparse
from urllib.request import urlopen

def log(msg):
    print(f"[BOOTSTRAP] {msg}", file=sys.stderr, flush=True)

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
        
        import psycopg2
        
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
        
        log("[ERROR] Timeout waiting for database after 90s")
        return False
    except Exception as e:
        log(f"[ERROR] Error in wait_for_db: {e}")
        return False

def init_tables():
    log("Running eager database initialization...")
    try:
        from dashboard import init_tables as run_init
        run_init()
        return True
    except Exception as e:
        log(f"[ERROR] Failed to initialize tables: {e}")
        traceback.print_exc()
        return False

def wait_for_http(port, path="/", timeout=30, process=None):
    url = f"http://127.0.0.1:{port}{path}"
    log(f"Waiting for HTTP service at {url}...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        if process and process.poll() is not None:
            log("[ERROR] Process exited while waiting for HTTP")
            return False
        try:
            with urlopen(url, timeout=2) as response:
                if response.status == 200:
                    log(f"Dashboard answered {path} with HTTP 200.")
                    return True
        except Exception:
            time.sleep(1)
    log(f"[ERROR] Timeout waiting for HTTP at {url}")
    return False

def is_port_in_use(p):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', int(p))) == 0

def kill_port_owner(p):
    try:
        # Use a more targeted kill command
        cmd = f"fuser -k {p}/tcp"
        subprocess.run(cmd, shell=True, capture_output=True)
        log(f"Cleared port {p} via fuser.")
    except Exception as e:
        log(f"Port reaper warning: {e}")

def main():
    try:
        port = os.environ.get("PORT", "8080")
        env_process_type = os.environ.get("PROCESS_TYPE")
        railway_service = os.environ.get("RAILWAY_SERVICE_NAME", "").lower()

        if env_process_type:
            process_type = env_process_type.strip().lower()
        elif "worker" in railway_service:
            process_type = "worker"
        elif "automator" in railway_service or "enterprise" in railway_service:
            process_type = "automator"
        else:
            process_type = "web"
        
        log(f"Starting {process_type} process sequence...")

        if not wait_for_db():
            sys.exit(1)

        if process_type == "web":
            if not init_tables():
                sys.exit(1)
            
            # Brief check/clear for 8080
            if is_port_in_use(port):
                kill_port_owner(port)
                time.sleep(1)

            cmd = ["gunicorn", "--bind", f"0.0.0.0:{port}", "--workers", "1", "--threads", "8", "--timeout", "300", "dashboard:app"]
            os.execvp(cmd[0], cmd)

        elif process_type == "worker":
            # Worker health is handled internally by tasks/__init__.py on port 8081
            cmd = ["celery", "-A", "tasks", "worker", "--loglevel=info", "--concurrency=1", "--pool=solo"]
            os.execvp(cmd[0], cmd)
        
        elif process_type == "automator":
            if not init_tables():
                sys.exit(1)
            
            if is_port_in_use(port):
                kill_port_owner(port)
                time.sleep(1)

            gunicorn_cmd = ["gunicorn", "--bind", f"0.0.0.0:{port}", "--workers", "1", "--threads", "4", "--timeout", "300", "dashboard:app"]
            dashboard_proc = subprocess.Popen(gunicorn_cmd)
            
            if not wait_for_http(port, process=dashboard_proc):
                dashboard_proc.terminate()
                sys.exit(1)
            
            automator_script = os.path.join(os.path.dirname(__file__), "automate_100_cities.py")
            subprocess.run([sys.executable, automator_script])
            dashboard_proc.wait()
        
    except Exception as e:
        log(f"[ERROR] CRITICAL FAILURE: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
