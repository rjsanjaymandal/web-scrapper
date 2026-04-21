import os
import sys
# IMMEDIATE STDOUT/STDERR UNBUFFERING
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
sys.stderr.reconfigure(line_buffering=True) if hasattr(sys.stderr, 'reconfigure') else None

print("--- [DEBUG] ENTRYPOINT LOADED ---", file=sys.stderr, flush=True)

import time
import subprocess
import traceback
import socket
import threading
from urllib.parse import urlparse
from http.server import BaseHTTPRequestHandler, HTTPServer
# Global diagnostic wrapper moved inside functions for faster initial bootstrap
def log(msg):
    # Use stderr to ensure logs appear immediately in Railway (stdout can be buffered)
    print(f"[BOOTSTRAP] {msg}", file=sys.stderr, flush=True)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Log incoming healthchecks for easier debugging in Railway
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(b"OK")
        log(f"[HEALTH] Responded with 200 OK to {self.path}")
    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()
    def log_message(self, format, *args):
        return

def start_health_server(port_str):
    def run_server():
        try:
            port = int(port_str)
            log(f"Starting Health Server on 0.0.0.0:{port}...")
            server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
            server.serve_forever()
        except Exception as e:
            log(f"❌ HEALTH SERVER CRITICAL ERROR: {e}")
            traceback.print_exc()

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
    log("Health Server thread spawned.")

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
        
        # Late import to speed up initial port binding
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
    # START HEALTH SERVER IMMEDIATELY (Railway requirement)
    port = os.environ.get("PORT", "8080")
    log(f"Initializing Worker Health Server on port {port}...")
    start_health_server(port)

    try:
        # Detect service type
        is_worker_flag = "--worker" in sys.argv
        env_process_type = os.environ.get("PROCESS_TYPE")
        railway_service = os.environ.get("RAILWAY_SERVICE_NAME", "").lower()

        if is_worker_flag:
            process_type = "worker"
        elif env_process_type:
            process_type = env_process_type
        elif "worker" in railway_service:
            process_type = "worker"
        elif "automator" in railway_service or "enterprise" in railway_service:
            process_type = "automator"
        else:
            process_type = "web"
        
        log(f"Starting {process_type} process sequence (Detected: {process_type}, Service: {railway_service or 'N/A'})...")

        # Shared DB Check
        if not wait_for_db():
            sys.exit(1)

        # Web-specific Init
        if process_type == "web":
            log("Running eager database initialization for Web module...")
            init_start = time.time()
            # Use the global init_tables() helper which imports dashboard locally
            if not init_tables():
                log("[ERROR] Database initialization failed. Process will exit.")
                sys.exit(1)
            init_duration = time.time() - init_start
            log(f"[SUCCESS] Database tables ready in {init_duration:.2f}s!")
            
            log(f"Finalizing environment for Web Service on port {port}...")
            
            # Diagnostic: Verify command existence before handoff
            try:
                subprocess.run(["gunicorn", "--version"], capture_output=True, check=True)
            except Exception:
                log("[ERROR] CRITICAL: 'gunicorn' command not found in PATH!")
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
            log(f"[LAUNCH] Handoff to Gunicorn (0.0.0.0:{port})...")
            os.execvp(cmd[0], cmd)

        elif process_type == "worker":
            # Re-read port just in case, though it's already started
            log("[LAUNCH] Handoff to Celery Worker...")
            # Run Celery as subprocess to keep the healthcheck thread alive
            cmd = [
                "celery",
                "-A", "tasks",
                "worker",
                "--loglevel=info",
                "--concurrency=1",
                "--pool=solo",
                "--max-tasks-per-child=5" # Guard against memory leaks
            ]
            
            try:
                # Use subprocess.run to block until worker exits
                subprocess.run(cmd)
            except KeyboardInterrupt:
                log("Worker received interrupt, shutting down...")
                sys.exit(0)
            except Exception as e:
                log(f"[ERROR] Worker crashed: {e}")
                sys.exit(1)
        
        elif process_type == "automator":
            port = os.environ.get("PORT", "8080")
            
            # Run DB init before starting anything
            log("Running database initialization for Automator+Dashboard...")
            init_tables()
            
            # Launch Gunicorn dashboard as a background subprocess
            log(f"Starting Dashboard (Gunicorn) on port {port} in background...")
            gunicorn_cmd = [
                "gunicorn",
                "--bind", f"0.0.0.0:{port}",
                "--workers", "1",
                "--threads", "4",
                "--timeout", "300",
                "--access-logfile", "-",
                "--error-logfile", "-",
                "dashboard:app"
            ]
            dashboard_proc = subprocess.Popen(gunicorn_cmd)
            log(f"[SUCCESS] Dashboard running (PID: {dashboard_proc.pid})")
            
            # Give gunicorn a moment to bind the port
            time.sleep(3)
            
            log("[LAUNCH] Starting Enterprise Automator...")
            automator_cmd = ["python3", "automate_100_cities.py"]
            try:
                subprocess.run(automator_cmd)
            except KeyboardInterrupt:
                log("Automator received interrupt, shutting down...")
                dashboard_proc.terminate()
                sys.exit(0)
            except Exception as e:
                log(f"[ERROR] Automator crashed: {e}")
                dashboard_proc.terminate()
                sys.exit(1)
            finally:
                # Keep dashboard alive after automator finishes
                log("Automator cycle complete. Dashboard still running...")
                try:
                    dashboard_proc.wait()
                except KeyboardInterrupt:
                    dashboard_proc.terminate()
        
        else:
            log(f"[ERROR] Unknown PROCESS_TYPE: {process_type}")
            sys.exit(1)
            
    except Exception as e:
        log(f"[ERROR] CRITICAL FAILURE in main loop: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
