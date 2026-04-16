import os
import sys
import time
import subprocess
import traceback
import socket
import threading
from urllib.parse import urlparse
from http.server import BaseHTTPRequestHandler, HTTPServer
from dashboard import init_tables, logger

# Global diagnostic wrapper to catch early import errors
try:
    import psycopg2
except ImportError as e:
    print(f"❌ [BOOTSTRAP] CRITICAL: Missing dependency 'psycopg2': {e}", flush=True)
    sys.exit(1)

def log(msg):
    print(f"🚀 [BOOTSTRAP] {msg}", flush=True)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        return

def start_health_server(port):
    def run_server():
        server = HTTPServer(('0.0.0.0', int(port)), HealthCheckHandler)
        server.serve_forever()
    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()

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
        # Detect service type
        # 1. Check command line flags (highest priority)
        # 2. Check explicitly set PROCESS_TYPE env var
        # 3. Auto-detect from RAILWAY_SERVICE_NAME
        is_worker_flag = "--worker" in sys.argv
        env_process_type = os.environ.get("PROCESS_TYPE")
        railway_service = os.environ.get("RAILWAY_SERVICE_NAME", "").lower()

        if is_worker_flag:
            process_type = "worker"
        elif env_process_type:
            process_type = env_process_type
        elif "worker" in railway_service:
            process_type = "worker"
        else:
            process_type = "web"
        
        log(f"Starting {process_type} process sequence (Detected service: {railway_service})...")

        # Shared DB Check
        if not wait_for_db():
            sys.exit(1)

        # Web-specific Init
        if process_type == "web":
            log("Running eager database initialization for Web module...")
            # Time the initialization to diagnose performance
            init_start = time.time()
            if not init_tables():
                log("❌ Database initialization failed. Process will exit.")
                sys.exit(1)
            init_duration = time.time() - init_start
            log(f"✅ Database tables ready in {init_duration:.2f}s!")
            
            port = os.environ.get("PORT", "8080")
            log(f"Finalizing environment for Web Service on port {port}...")
            
            # Diagnostic: Verify command existence before handoff
            try:
                subprocess.run(["gunicorn", "--version"], capture_output=True, check=True)
            except Exception:
                log("❌ CRITICAL: 'gunicorn' command not found in PATH!")
                sys.exit(1)

            # Use execvp for web process to give it full control
            cmd = [
                "gunicorn",
                "--bind", f"0.0.0.0:{port}",
                "--workers", "1",
                "--threads", "8",
                "--timeout", "300",
                "--access-logfile", "-",
                "--error-logfile", "-",
                "dashboard:app"
            ]
            log(f"🚀 Handoff to Gunicorn (0.0.0.0:{port})...")
            os.execvp(cmd[0], cmd)

        elif process_type == "worker":
            port = os.environ.get("PORT", "8080")
            log("Starting Healthcheck server for Worker...")
            start_health_server(port)
            log("🚀 Handoff to Celery Worker...")
            # Run Celery as subprocess to keep the healthcheck thread alive
            cmd = [
                "celery",
                "-A", "tasks",
                "worker",
                "--loglevel=info",
                "--concurrency=1",
                "--pool=solo"
            ]
            
            try:
                # Use subprocess.run to block until worker exits
                subprocess.run(cmd)
            except KeyboardInterrupt:
                log("Worker received interrupt, shutting down...")
                sys.exit(0)
            except Exception as e:
                log(f"❌ Worker crashed: {e}")
                sys.exit(1)
        
        else:
            log(f"❌ Unknown PROCESS_TYPE: {process_type}")
            sys.exit(1)
            
    except Exception as e:
        log(f"❌ CRITICAL FAILURE in main loop: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
