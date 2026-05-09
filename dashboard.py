from flask import Flask, render_template_string, request, jsonify, Response, send_file
import psycopg2
import psycopg2.extras
import yaml
import io
import re
import os
import logging
import json
import time
import threading
from datetime import datetime, timedelta, date
from openpyxl import Workbook
from pathlib import Path
import sqlite3

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

app = Flask(__name__)
app.json.cls = CustomJSONEncoder

PROJ_DIR = Path(__file__).parent
LOGS_DIR = PROJ_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOGS_DIR / "dashboard.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Log environment for Railway diagnostics
PORT = os.environ.get("PORT", "5000")
RAILWAY_SERVICE = os.environ.get("RAILWAY_SERVICE_NAME", "Unknown")
logger.info(f"BOOTSTRAP: Railway Service: {RAILWAY_SERVICE} | Port: {PORT}")

@app.route("/health")
def health_check():
    """Lightweight health check for Railway"""
    return jsonify({
        "status": "healthy",
        "dashboard": "ready",
        "database": "ready" if DB_INIT_READY else "pending",
        "timestamp": int(time.time()),
        "process_type": os.environ.get("PROCESS_TYPE", "web").lower(),
        "service": "contact-scraper-dashboard"
    }), 200


@app.route("/up")
def up():
    """Detailed health check for internal status."""
    status = {"status": "ok", "db": DB_INIT_READY}
    return jsonify(status), 200


# Redis for live status (optional)
REDIS_ACTIVE = False
try:
    import redis

    REDIS_URL = os.environ.get("REDIS_URL")
    if REDIS_URL:
        redis_client = redis.Redis.from_url(REDIS_URL, socket_timeout=2)
        redis_client.ping()
        REDIS_ACTIVE = True
    else:
        redis_client = None
except Exception:
    redis_client = None


# DB Globals
USE_SQLITE = False
DB_INIT_READY = os.environ.get("DASHBOARD_DB_BOOTSTRAPPED") == "1"

# --- Watchdog System ---
class ScraperWatchdog(threading.Thread):
    """
    Idle-detection watchdog to monitor and reset stalled scraping processes.
    """
    def __init__(self, check_interval=60):
        super().__init__(daemon=True)
        self.check_interval = check_interval
        self.logger = logging.getLogger("watchdog")

    def run(self):
        self.logger.info("Watchdog active: Monitoring for idle stalls...")
        while True:
            try:
                self.check_status()
            except Exception as e:
                self.logger.error(f"Watchdog error: {e}")
            time.sleep(self.check_interval)

    def check_status(self):
        conn = None
        try:
            conn = _connect_db()
            cur = conn.cursor()
            placeholder = "?" if USE_SQLITE else "%s"
            
            # 1. Get current status
            cur.execute(f"SELECT value, updated_at FROM system_status WHERE key = {placeholder}", ("scraper_status",))
            row = cur.fetchone()
            if not row: return

            status = json.loads(row["value"])
            updated_at = row["updated_at"]
            
            # 2. If marked as running, check last log activity
            if status.get("running"):
                # If no update in 10 mins, it's likely stalled
                if datetime.now() - updated_at > timedelta(minutes=10):
                    self.logger.warning("Detected stalled scraper process. Resetting to IDLE.")
                    
                    idle_status = {"message": "Idle (Auto-Reset)", "running": False, "time": datetime.now().strftime("%H:%M:%S")}
                    val_json = json.dumps(idle_status)
                    
                    if USE_SQLITE:
                        cur.execute("INSERT OR REPLACE INTO system_status (id, key, value, updated_at) VALUES (1, 'scraper_status', ?, ?)", 
                                   (val_json, datetime.now()))
                        cur.execute("INSERT INTO scraper_logs (level, message, source, created_at) VALUES (?, ?, ?, ?)", 
                                   ("WARNING", "Watchdog: Process stalled and was auto-reset.", "WATCHDOG", datetime.now()))
                    else:
                        cur.execute("""
                            INSERT INTO system_status (id, key, value, updated_at) 
                            VALUES (1, 'scraper_status', %s, NOW())
                            ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                        """, (val_json,))
                        cur.execute("INSERT INTO scraper_logs (level, message, source, created_at) VALUES (%s, %s, %s, NOW())", 
                                   ("WARNING", "Watchdog: Process stalled and was auto-reset.", "WATCHDOG"))
                    
                    conn.commit()
                    if redis_client:
                        redis_client.set("scraper_status", val_json, ex=3600)
            
            cur.close()
            conn.close()
        except Exception as e:
            if conn: conn.close()
            self.logger.error(f"Status check failed: {e}")




DB_INIT_IN_PROGRESS = False
DB_INIT_LAST_ATTEMPT = 0.0
DB_INIT_LAST_ERROR = None
DB_INIT_RETRY_SECONDS = int(os.environ.get("DATABASE_INIT_RETRY_SECONDS", "15"))
DB_STATEMENT_TIMEOUT_MS = int(os.environ.get("DATABASE_STATEMENT_TIMEOUT_MS", "8000"))
FILTER_CACHE = {}  # Stores { 'cities': (data, timestamp), ... }
FILTER_CACHE_TTL = 300  # 5 minutes


def get_cached_filter(key, query, cur):
    """Get filter values with a 5-minute TTL to prevent heavy DB scans."""
    now = time.time()
    if key in FILTER_CACHE:
        val, ts = FILTER_CACHE[key]
        if (now - ts) < FILTER_CACHE_TTL:
            return val
    
    cur.execute(query, ("",))
    data = [r[next(iter(r.keys()))] for r in cur.fetchall()]
    FILTER_CACHE[key] = (data, now)
    return data


def get_db_url():
    """Build the database URL from environment variables."""
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        return db_url
    config = load_config()
    db_cfg = config.get("database", {}) if isinstance(config, dict) else {}
    host = os.environ.get("DATABASE_HOST", db_cfg.get("host", "localhost"))
    port = os.environ.get("DATABASE_PORT", db_cfg.get("port", 5432))
    name = os.environ.get("DATABASE_NAME", db_cfg.get("name", "scraper_db"))
    user = os.environ.get("DATABASE_USER", db_cfg.get("user", "postgres"))
    pw = os.environ.get("DATABASE_PASSWORD", db_cfg.get("password", ""))
    return f"postgresql://{user}:{pw}@{host}:{port}/{name}"


def _connect_db(statement_timeout_ms=None):
    """Open a database connection with a short timeout so web boot stays responsive."""
    global USE_SQLITE
    
    if USE_SQLITE or not os.environ.get("DATABASE_URL"):
        try:
            # Check if Postgres is reachable even if no URL is set (localhost)
            url = get_db_url()
            if "localhost" in url:
                 conn = psycopg2.connect(url, connect_timeout=1)
                 return conn
        except Exception:
            pass
            
        # Fallback to SQLite
        USE_SQLITE = True
        conn = sqlite3.connect(PROJ_DIR / "scraper_local.db")
        conn.row_factory = sqlite3.Row
        return conn

    url = get_db_url()
    # Railway may use postgres:// — psycopg2 needs postgresql://
    if url and url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    connect_timeout = int(os.environ.get("DATABASE_CONNECT_TIMEOUT", "5"))
    connect_kwargs = {
        "cursor_factory": psycopg2.extras.RealDictCursor,
        "connect_timeout": connect_timeout,
        "application_name": "dashboard",
    }
    if statement_timeout_ms:
        connect_kwargs["options"] = f"-c statement_timeout={statement_timeout_ms}"

    conn = psycopg2.connect(url, **connect_kwargs)
    conn.autocommit = True
    return conn


def ensure_db_initialized(force=False):
    """Initialize schema lazily so the web process can boot before Postgres is ready."""
    global DB_INIT_LAST_ATTEMPT

    if DB_INIT_READY:
        return True

    now = time.monotonic()
    if (
        not force
        and DB_INIT_LAST_ERROR
        and (now - DB_INIT_LAST_ATTEMPT) < DB_INIT_RETRY_SECONDS
    ):
        raise RuntimeError(f"Database not ready yet: {DB_INIT_LAST_ERROR}")

    if not init_tables():
        raise RuntimeError(
            f"Database initialization failed: {DB_INIT_LAST_ERROR or 'unknown error'}"
        )

    return True


def get_db():
    """Get a fresh database connection after lazy schema initialization."""
    # If not already ready, try to initialize it now
    if not DB_INIT_READY:
        try:
            ensure_db_initialized()
        except Exception as e:
            logger.error(f"Failed to initialize database on request: {e}")
            raise
    return _connect_db(statement_timeout_ms=DB_STATEMENT_TIMEOUT_MS)


def init_tables():
    """Create tables if they don't exist."""
    global DB_INIT_READY, DB_INIT_IN_PROGRESS, DB_INIT_LAST_ATTEMPT, DB_INIT_LAST_ERROR

    if DB_INIT_IN_PROGRESS:
        return DB_INIT_READY

    DB_INIT_IN_PROGRESS = True
    DB_INIT_LAST_ATTEMPT = time.monotonic()
    try:
        conn = _connect_db()
        cur = conn.cursor()
        # Use platform-aware types (SERIAL for Postgres, AUTOINCREMENT for SQLite)
        id_type = "INTEGER PRIMARY KEY AUTOINCREMENT" if USE_SQLITE else "SERIAL PRIMARY KEY"
        
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS contacts (
                id {id_type},
                name VARCHAR(255),
                phone VARCHAR(50),
                email VARCHAR(255),
                address TEXT,
                category VARCHAR(100),
                city VARCHAR(100),
                area VARCHAR(100),
                state VARCHAR(100),
                source VARCHAR(100),
                source_url TEXT,
                phone_clean VARCHAR(50),
                email_valid BOOLEAN,
                enriched BOOLEAN,
                arn VARCHAR(50),
                license_no VARCHAR(100),
                membership_no VARCHAR(100),
                quality_score INTEGER DEFAULT 0,
                quality_tier VARCHAR(20) DEFAULT 'low',
                blockchain_ca VARCHAR(255),
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # New: System status table for scraper state
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS system_status (
                id INTEGER PRIMARY KEY,
                key VARCHAR(100) UNIQUE,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # New: Scraper logs table for activity feed
        log_id_type = "INTEGER PRIMARY KEY AUTOINCREMENT" if USE_SQLITE else "SERIAL PRIMARY KEY"
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS scraper_logs (
                id {log_id_type},
                level VARCHAR(20),
                message TEXT,
                source VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Individual column checks for existing tables
        required_columns = {
            "area": "VARCHAR(100)",
            "state": "VARCHAR(100)",
            "source_url": "TEXT",
            "phone_clean": "VARCHAR(50)",
            "email_valid": "BOOLEAN DEFAULT FALSE",
            "enriched": "BOOLEAN DEFAULT FALSE",
            "arn": "VARCHAR(50)",
            "license_no": "VARCHAR(100)",
            "membership_no": "VARCHAR(100)",
            "quality_score": "INTEGER DEFAULT 0",
            "quality_tier": "VARCHAR(20) DEFAULT 'low'",
            "blockchain_ca": "VARCHAR(255)",
            "scraped_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }

        for column_name, column_type in required_columns.items():
            try:
                # SQLite doesn't support ADD COLUMN IF NOT EXISTS directly until very recently
                if USE_SQLITE:
                    cur.execute(f"PRAGMA table_info(contacts)")
                    existing = [r[1] for r in cur.fetchall()]
                    if column_name not in existing:
                        cur.execute(f"ALTER TABLE contacts ADD COLUMN {column_name} {column_type}")
                else:
                    cur.execute(
                        f"ALTER TABLE contacts ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                    )
            except Exception as col_err:
                # Ignore column errors
                pass

        # Optimization: Only run heavy cleanup if the unique index is missing
        index_exists = False
        if not USE_SQLITE:
            try:
                cur.execute("""
                    SELECT count(*) FROM pg_indexes 
                    WHERE indexname = 'idx_contacts_unique_phone'
                """)
                index_exists = cur.fetchone()['count'] > 0
            except:
                pass
        else:
            try:
                cur.execute("PRAGMA index_list('contacts')")
                indices = cur.fetchall()
                index_exists = any(idx[1] == 'idx_contacts_unique_phone' for idx in indices)
            except:
                pass

        if not index_exists:
            logger.info("Deduplication index missing. Running one-time cleanup...")
            
            # 1. Ensure phone_clean has a basic index to speed up the join
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tmp_phone_clean ON contacts(phone_clean)")
            
            if USE_SQLITE:
                # SQLite doesn't support USING for DELETE, it's simpler
                cur.execute("""
                    DELETE FROM contacts WHERE id NOT IN (
                        SELECT MAX(id) FROM contacts GROUP BY phone_clean
                    ) AND phone_clean IS NOT NULL
                """)
            else:
                cur.execute("""
                    DELETE FROM contacts a
                    USING contacts b
                    WHERE a.id < b.id
                    AND a.phone_clean = b.phone_clean
                    AND a.phone_clean IS NOT NULL
                """)
                
                cur.execute("""
                    DELETE FROM contacts a
                    USING contacts b
                    WHERE a.id < b.id
                    AND a.email = b.email
                    AND a.email IS NOT NULL
                """)
            
            # 3. Drop temporary index
            cur.execute("DROP INDEX IF EXISTS idx_tmp_phone_clean")
            logger.info("Cleanup completed.")

        # Constraints for Deduplication (UPSERT support)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_unique_phone ON contacts(phone_clean) WHERE phone_clean IS NOT NULL")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_unique_email ON contacts(email) WHERE email IS NOT NULL")
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_source ON contacts(source)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_category ON contacts(category)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_city ON contacts(city)")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_scraper_logs_created ON scraper_logs(created_at DESC)")
        cur.close()
        conn.close()
        DB_INIT_READY = True
        logger.info("Database tables ready!")
        return True
    except Exception as e:
        DB_INIT_READY = False
        DB_INIT_LAST_ERROR = str(e)
        logger.warning(f"Database init deferred: {e}")
        return False
    finally:
        DB_INIT_IN_PROGRESS = False

# Start Watchdog after DB utilities are defined
watchdog = ScraperWatchdog()
watchdog.start()


def load_config():
    try:
        with open("config.yaml", "r") as f:
            return yaml.safe_load(f)
    except Exception:
        return {}


# Note: In production, entrypoint.py handles eager bootstrap.
# We skip eager init at import time in Railway environments to allow Gunicorn to bind quickly.
if not DB_INIT_READY and RAILWAY_SERVICE != "Unknown":
    logger.info(f"BOOTSTRAP: Managed mode (Railway {RAILWAY_SERVICE}). Awaiting first request for local state sync.")
elif not DB_INIT_READY:
    logger.info("BOOTSTRAP: Local/Lazy mode (RAILWAY_SERVICE=Unknown).")


HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MaysanLabs Scrapper | Intelligence HUD</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@400;500;600;700&family=Outfit:wght@400;600;800&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-obsidian: #050608;
            --bg-sidebar: #08090d;
            --card-glass: rgba(15, 18, 25, 0.7);
            --card-glass-hover: rgba(20, 24, 35, 0.9);
            --accent-emerald: #10b981;
            --accent-blue: #3b82f6;
            --accent-amber: #f59e0b;
            --accent-red: #ef4444;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --text-muted: #475569;
            --border-muted: rgba(255,255,255,0.06);
            --border-glow: rgba(16, 185, 129, 0.2);
            --glow-emerald: rgba(16, 185, 129, 0.4);
            --shadow-sm: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            --shadow-lg: 0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04);
        }

        * { box-sizing: border-box; margin: 0; padding: 0; -webkit-font-smoothing: antialiased; }
        
        body { 
            font-family: 'Inter', sans-serif; 
            background: var(--bg-obsidian); 
            color: var(--text-primary); 
            overflow-x: hidden;
            background-image: 
                radial-gradient(circle at 0% 0%, rgba(16, 185, 129, 0.08) 0%, transparent 40%),
                radial-gradient(circle at 100% 100%, rgba(59, 130, 246, 0.08) 0%, transparent 40%);
            background-attachment: fixed;
        }

        .film-grain {
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background-image: url('https://upload.wikimedia.org/wikipedia/commons/7/76/1k_noise.png');
            opacity: 0.02; pointer-events: none; z-index: 9999; mix-blend-mode: overlay;
        }

        /* Typography */
        h1, h2, h3, .brand-box p { font-family: 'Outfit', sans-serif; }
        .mono { font-family: 'JetBrains Mono', monospace; }

        .brand-box { margin-bottom: 8px; padding-left: 4px; }
        .brand-box p { font-size: 18px; font-weight: 800; color: #fff; letter-spacing: -0.5px; }
        .brand-box span { font-size: 10px; text-transform: uppercase; letter-spacing: 3px; color: var(--accent-emerald); display: block; margin-top: 2px; font-weight: 600; opacity: 0.8; }

        .nav-group { display: flex; flex-direction: column; gap: 6px; }
        .nav-label { font-size: 10px; text-transform: uppercase; color: var(--text-muted); letter-spacing: 1.5px; margin: 12px 0 8px 12px; font-weight: 700; }
        
        .nav-item { 
            padding: 12px 14px; border-radius: 12px; color: var(--text-secondary); 
            text-decoration: none; font-size: 14px; font-weight: 500; transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
            display: flex; align-items: center; gap: 12px;
            border: 1px solid transparent;
        }
        .nav-item:hover { background: rgba(255,255,255,0.04); color: #fff; border-color: rgba(255,255,255,0.05); }
        .nav-item.active { 
            background: rgba(16, 185, 129, 0.08); 
            color: var(--accent-emerald); 
            border: 1px solid rgba(16, 185, 129, 0.15);
            box-shadow: inset 0 0 10px rgba(16, 185, 129, 0.05);
        }
        .nav-item svg { opacity: 0.6; transition: 0.2s; }
        .nav-item:hover svg, .nav-item.active svg { opacity: 1; filter: drop-shadow(0 0 5px currentColor); }

        /* Export Buttons */
        .export-btn {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: 6px;
            padding: 14px 8px;
            border-radius: 12px;
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.08);
            color: var(--text-secondary);
            cursor: pointer;
            transition: all 0.25s ease;
            font-weight: 600;
            font-size: 11px;
            letter-spacing: 0.5px;
        }
        .export-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 20px rgba(0, 0, 0, 0.3);
        }
        .export-btn:active {
            transform: translateY(0);
        }
        .export-csv {
            border-color: rgba(16, 185, 129, 0.3);
            background: linear-gradient(135deg, rgba(16, 185, 129, 0.1), transparent);
        }
        .export-csv:hover {
            background: linear-gradient(135deg, rgba(16, 185, 129, 0.2), rgba(16, 185, 129, 0.05));
            border-color: var(--accent-emerald);
            box-shadow: 0 4px 15px rgba(16, 185, 129, 0.2);
        }
        .export-csv svg { color: var(--accent-emerald); }
        .export-csv:hover svg { filter: drop-shadow(0 0 6px var(--accent-emerald)); }

        .export-excel {
            border-color: rgba(59, 130, 246, 0.3);
            background: linear-gradient(135deg, rgba(59, 130, 246, 0.1), transparent);
        }
        .export-excel:hover {
            background: linear-gradient(135deg, rgba(59, 130, 246, 0.2), rgba(59, 130, 246, 0.05));
            border-color: var(--accent-blue);
            box-shadow: 0 4px 15px rgba(59, 130, 246, 0.2);
        }
        .export-excel svg { color: var(--accent-blue); }
        .export-excel:hover svg { filter: drop-shadow(0 0 6px var(--accent-blue)); }

        .export-json {
            border-color: rgba(245, 158, 11, 0.3);
            background: linear-gradient(135deg, rgba(245, 158, 11, 0.1), transparent);
        }
        .export-json:hover {
            background: linear-gradient(135deg, rgba(245, 158, 11, 0.2), rgba(245, 158, 11, 0.05));
            border-color: var(--accent-amber);
            box-shadow: 0 4px 15px rgba(245, 158, 11, 0.2);
        }
        .export-json svg { color: var(--accent-amber); }
        .export-json:hover svg { filter: drop-shadow(0 0 6px var(--accent-amber)); }

        /* Sidebar Footer */
        .system-footer { 
            margin-top: auto; padding: 16px; background: rgba(255,255,255,0.02); 
            border-radius: 14px; border: 1px solid var(--border-muted);
            backdrop-filter: blur(4px);
        }
        .system-footer p { font-size: 9px; color: var(--text-muted); margin-bottom: 8px; text-transform: uppercase; letter-spacing: 1px; font-weight: 600; }
        .status-online { display: flex; align-items: center; gap: 10px; font-size: 13px; font-weight: 600; color: var(--accent-emerald); }
        .status-dot { 
            width: 8px; height: 8px; background: var(--accent-emerald); border-radius: 50%; 
            box-shadow: 0 0 12px var(--accent-emerald);
            animation: statusPulse 2s infinite;
        }
        @keyframes statusPulse {
            0% { transform: scale(1); opacity: 1; }
            50% { transform: scale(1.3); opacity: 0.6; }
            100% { transform: scale(1); opacity: 1; }
        }

        /* Layout Wrapper */
        .layout-wrapper { 
            display: grid; 
            grid-template-columns: 180px 1fr; 
            min-height: 100vh; 
            width: 100%;
        }
        .sidebar { 
            background: var(--bg-sidebar); 
            border-right: 1px solid var(--border-muted); 
            padding: 24px 16px; 
            display: flex; 
            flex-direction: column; 
            gap: 24px; 
            height: 100vh; 
            position: sticky; 
            top: 0; 
            z-index: 100;
        }
        .main-view { 
            padding: 40px; 
            min-width: 0; 
            position: relative;
            z-index: 1;
            display: flex;
            flex-direction: column;
            gap: 32px;
        }
        .header-row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
        .header-row h2 { font-size: 28px; font-weight: 800; letter-spacing: -1px; background: linear-gradient(to right, #fff, var(--text-secondary)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        
        /* HUD Components */
        .stats-hud { display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; }
        .stat-card { 
            background: var(--card-glass); padding: 24px; border-radius: 20px; 
            border: 1px solid var(--border-muted);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            backdrop-filter: blur(12px);
            position: relative;
            overflow: hidden;
        }
        .stat-card:hover { 
            background: var(--card-glass-hover); 
            border-color: var(--border-glow); 
            transform: translateY(-4px);
            box-shadow: var(--shadow-lg), 0 0 20px rgba(16, 185, 129, 0.05);
        }
        .stat-card::after {
            content: ''; position: absolute; top: 0; left: -100%; width: 100%; height: 100%;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.03), transparent);
            transition: 0.5s;
        }
        .stat-card:hover::after { left: 100%; }
        
        .stat-card .label { font-size: 11px; text-transform: uppercase; color: var(--text-secondary); letter-spacing: 1.5px; margin-bottom: 12px; display: block; font-weight: 700; }
        .stat-card .value { font-size: 32px; font-weight: 800; }
        .stat-card.emerald .value { color: var(--accent-emerald); text-shadow: 0 0 20px rgba(16, 185, 129, 0.3); }
        .stat-card.blue .value { color: var(--accent-blue); text-shadow: 0 0 20px rgba(59, 130, 246, 0.3); }
        .stat-card.amber .value { color: var(--accent-amber); text-shadow: 0 0 20px rgba(245, 158, 11, 0.3); }

        .content-grid { display: flex; flex-direction: column; gap: 32px; }
        .glass-card { 
            background: var(--card-glass); border-radius: 24px; border: 1px solid var(--border-muted); 
            padding: 32px; backdrop-filter: blur(12px);
            box-shadow: var(--shadow-lg);
        }
        
        /* Charts */
        .charts-row { display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; }
        .chart-card { 
            background: var(--card-glass); border-radius: 20px; border: 1px solid var(--border-muted); 
            padding: 20px; min-height: 220px; backdrop-filter: blur(8px);
            transition: 0.3s;
        }
        .chart-card:hover { border-color: rgba(255,255,255,0.1); background: var(--card-glass-hover); }
        .chart-card p { font-size: 10px; text-transform: uppercase; color: var(--text-secondary); margin-bottom: 16px; letter-spacing: 1.5px; font-weight: 700; }
        .chart-container { position: relative; height: 160px; width: 100%; }
        
        /* Terminal & Feed */
        .terminal-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }
        .terminal-header h3 { font-size: 14px; text-transform: uppercase; letter-spacing: 2px; color: var(--text-secondary); }
        .terminal { 
            background: #08090c; border-radius: 16px; padding: 20px; height: 200px; overflow-y: auto;
            font-family: 'JetBrains Mono', monospace; font-size: 11px; line-height: 1.8;
            border: 1px solid var(--border-muted);
            scrollbar-width: thin;
            scrollbar-color: var(--accent-emerald) transparent;
            box-shadow: inset 0 2px 10px rgba(0,0,0,0.5);
        }
        .log-entry { margin-bottom: 6px; padding: 4px 12px; border-radius: 6px; transition: 0.2s; border-left: 2px solid transparent; }
        .log-entry:hover { background: rgba(255,255,255,0.03); border-left-color: var(--border-muted); }
        .log-time { color: var(--text-muted); font-size: 10px; min-width: 80px; }
        .log-src { color: var(--accent-blue); font-weight: 700; min-width: 90px; }
        .log-msg { color: #e2e8f0; }
        .log-msg.ERROR { color: var(--accent-red); font-weight: 600; }
        .log-msg.SUCCESS { color: var(--accent-emerald); font-weight: 600; }

        /* Controls */
        .controls-grid { display: grid; grid-template-columns: 1.2fr 1fr 1fr auto; gap: 20px; align-items: flex-end; margin-bottom: 32px; }
        .input-group label { display: block; font-size: 11px; text-transform: uppercase; color: var(--text-secondary); margin-bottom: 10px; letter-spacing: 1.5px; font-weight: 700; }
        .input-group input, .input-group select { 
            width: 100%; background: #08090c; border: 1px solid var(--border-muted); padding: 14px 18px; 
            border-radius: 14px; color: #fff; font-size: 14px; outline: none; transition: all 0.2s;
            box-shadow: inset 0 2px 4px rgba(0,0,0,0.2);
        }
        .input-group input:focus, .input-group select:focus { border-color: var(--accent-emerald); box-shadow: 0 0 0 4px rgba(16, 185, 129, 0.1); }
        .input-group select { appearance: none; cursor: pointer; background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='24' height='24' viewBox='0 0 24 24' fill='none' stroke='%2394a3b8' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'%3E%3C/polyline%3E%3C/svg%3E"); background-repeat: no-repeat; background-position: right 14px center; background-size: 18px; }

        .btn { 
            padding: 14px 28px; border-radius: 14px; font-weight: 700; cursor: pointer; border: none; font-size: 13px;
            text-transform: uppercase; letter-spacing: 1.5px; transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); 
            display: inline-flex; align-items: center; gap: 10px;
            box-shadow: var(--shadow-sm);
        }
        .btn-primary { background: var(--accent-emerald); color: #064e3b; position: relative; overflow: hidden; }
        .btn-primary:hover { transform: scale(1.02); box-shadow: 0 0 25px var(--glow-emerald); background: #10c991; }
        .btn-primary::after { content: ''; position: absolute; top: -50%; left: -50%; width: 200%; height: 200%; background: radial-gradient(circle, rgba(255,255,255,0.2) 0%, transparent 70%); opacity: 0; transition: 0.5s; }
        .btn-primary:hover::after { opacity: 1; }

        .btn-outline { background: rgba(255,255,255,0.03); border: 1px solid var(--border-muted); color: var(--text-primary); }
        .btn-outline:hover { background: rgba(255,255,255,0.06); border-color: var(--text-secondary); color: #fff; }
        .btn-sm { padding: 8px 16px; font-size: 11px; }

        /* HUD Table */
        .table-wrap { 
            background: rgba(8, 9, 12, 0.5); border-radius: 18px; overflow: hidden; 
            border: 1px solid var(--border-muted); box-shadow: inset 0 0 20px rgba(0,0,0,0.2);
        }
        table { width: 100%; border-collapse: separate; border-spacing: 0; }
        th { 
            background: rgba(15, 18, 25, 0.8); padding: 16px 20px; text-align: left; 
            font-size: 11px; text-transform: uppercase; color: var(--text-muted); 
            letter-spacing: 1.5px; font-weight: 800; border-bottom: 1px solid var(--border-muted);
            position: sticky; top: 0; z-index: 10; backdrop-filter: blur(8px);
        }
        td { padding: 18px 20px; border-bottom: 1px solid var(--border-muted); font-size: 13px; color: var(--text-primary); transition: 0.2s; }
        tr:last-child td { border-bottom: none; }
        tr:hover td { background: rgba(16, 185, 129, 0.03); color: #fff; }
        
        .badge { padding: 5px 10px; border-radius: 8px; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; }
        .badge-src { background: rgba(59, 130, 246, 0.1); color: var(--accent-blue); border: 1px solid rgba(59, 130, 246, 0.2); }
        
        /* Pagination */
        .pagination {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding-top: 24px;
            margin-top: 24px;
            border-top: 1px solid var(--border-muted);
            flex-wrap: wrap;
            gap: 16px;
        }
        .pagination-info {
            font-size: 13px;
            color: var(--text-muted);
            font-weight: 500;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .pagination-info span { color: var(--text-secondary); font-weight: 700; }
        .pagination-btns { display: flex; gap: 6px; flex-wrap: wrap; }
        .pagination-btn {
            min-width: 40px;
            height: 40px;
            padding: 0 16px;
            border-radius: 10px;
            border: 1px solid var(--border-muted);
            background: rgba(255,255,255,0.03);
            color: var(--text-secondary);
            font-size: 13px;
            cursor: pointer;
            transition: all 0.2s ease;
            font-weight: 600;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .pagination-btn:hover:not(:disabled) {
            border-color: var(--accent-emerald);
            color: var(--accent-emerald);
            background: rgba(16, 185, 129, 0.08);
            transform: translateY(-1px);
        }
        .pagination-btn.active {
            background: var(--accent-emerald);
            color: #000;
            border-color: var(--accent-emerald);
            box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);
        }
        .pagination-btn:disabled {
            opacity: 0.3;
            cursor: not-allowed;
            transform: none !important;
        }
        .pagination-btn.icon-btn {
            min-width: 40px;
            padding: 0;
        }

        .page-size-selector {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-left: 16px;
        }
        .page-size-selector label {
            font-size: 11px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .page-size-selector select {
            background: rgba(255,255,255,0.05);
            border: 1px solid var(--border-muted);
            border-radius: 8px;
            padding: 6px 12px;
            color: var(--text-primary);
            font-size: 12px;
            cursor: pointer;
        }

        .quick-jump {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-left: auto;
        }
        .quick-jump input {
            width: 60px;
            height: 36px;
            background: rgba(255,255,255,0.05);
            border: 1px solid var(--border-muted);
            border-radius: 8px;
            padding: 0 12px;
            color: var(--text-primary);
            font-size: 13px;
            text-align: center;
        }
        .quick-jump button {
            height: 36px;
            padding: 0 16px;
            background: var(--accent-blue);
            border: none;
            border-radius: 8px;
            color: #fff;
            font-size: 12px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
        }
        .quick-jump button:hover {
            background: #4f9ef7;
            transform: translateY(-1px);
        }

        /* Progress Animation */
        .progress-bar-container { height: 6px; background: rgba(255,255,255,0.05); border-radius: 3px; overflow: hidden; margin-top: 8px; }
        .progress-bar { height: 100%; background: linear-gradient(90deg, var(--accent-emerald), var(--accent-blue)); transition: width 0.5s cubic-bezier(0.4, 0, 0.2, 1); }

        /* Table Styling */
        .lead-row {
            transition: all 0.2s ease;
        }
        .lead-row:hover {
            background: rgba(16, 185, 129, 0.04);
        }
        .lead-row:hover td {
            color: #fff;
        }

        .score-wrapper {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .score-bar {
            width: 50px;
            height: 6px;
            background: rgba(255,255,255,0.08);
            border-radius: 3px;
            overflow: hidden;
        }
        .score-fill {
            height: 100%;
            border-radius: 3px;
            transition: width 0.3s ease;
        }
        .score-value {
            font-size: 11px;
            font-weight: 700;
            color: var(--text-secondary);
            min-width: 35px;
        }

        .action-btn {
            width: 32px;
            height: 32px;
            border-radius: 8px;
            border: 1px solid var(--border-muted);
            background: rgba(255,255,255,0.03);
            color: var(--text-muted);
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.2s;
        }
        .action-btn:hover {
            background: rgba(255,255,255,0.08);
            border-color: var(--accent-emerald);
            color: var(--accent-emerald);
        }

        /* Stats Cards Enhancement */
        .stats-hud {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 20px;
        }
        .stat-card {
            background: var(--card-glass);
            padding: 24px;
            border-radius: 20px;
            border: 1px solid var(--border-muted);
            transition: all 0.3s ease;
            backdrop-filter: blur(12px);
            position: relative;
            overflow: hidden;
        }
        .stat-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: linear-gradient(90deg, transparent, var(--accent-emerald), transparent);
            opacity: 0;
            transition: opacity 0.3s;
        }
        .stat-card:hover::before {
            opacity: 1;
        }
        .stat-card:hover {
            transform: translateY(-4px);
            border-color: rgba(16, 185, 129, 0.2);
            box-shadow: 0 20px 40px rgba(0,0,0,0.3);
        }

        /* Charts Enhancement */
        .charts-row {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 20px;
        }
        .chart-card {
            background: var(--card-glass);
            border-radius: 20px;
            border: 1px solid var(--border-muted);
            padding: 24px;
            min-height: 280px;
            backdrop-filter: blur(8px);
            transition: all 0.3s ease;
        }
        .chart-card:hover {
            border-color: rgba(255,255,255,0.1);
            transform: translateY(-2px);
        }
        .chart-card p {
            font-size: 11px;
            text-transform: uppercase;
            color: var(--text-secondary);
            margin-bottom: 20px;
            letter-spacing: 2px;
            font-weight: 700;
        }
        .chart-container {
            position: relative;
            height: 180px;
            width: 100%;
        }

        /* Notification Toast */
        .toast {
            position: fixed;
            bottom: 24px;
            right: 24px;
            padding: 16px 24px;
            border-radius: 12px;
            background: var(--card-glass);
            border: 1px solid var(--accent-emerald);
            color: var(--accent-emerald);
            font-weight: 600;
            font-size: 13px;
            display: none;
            align-items: center;
            gap: 12px;
            animation: slideUp 0.3s ease;
            z-index: 1000;
            backdrop-filter: blur(12px);
            box-shadow: 0 8px 32px rgba(0,0,0,0.4);
        }
        @keyframes slideUp {
            from { transform: translateY(20px); opacity: 0; }
            to { transform: translateY(0); opacity: 1; }
        }

        .pulse { animation: pulse 2s infinite; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }

        /* Custom Scrollbar */
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: var(--text-muted); border-radius: 10px; border: 2px solid var(--bg-obsidian); }
        ::-webkit-scrollbar-thumb:hover { background: var(--text-secondary); }

        /* Mobile Responsive */
        @media (max-width: 1200px) {
            .stats-hud { grid-template-columns: repeat(2, 1fr); }
            .charts-row { grid-template-columns: repeat(2, 1fr); }
        }
        @media (max-width: 768px) {
            .stats-hud { grid-template-columns: 1fr; }
            .charts-row { grid-template-columns: 1fr; }
            .sidebar { display: none; }
            .layout-wrapper { grid-template-columns: 1fr; }
        }

        /* Controls Card */
        .controls-card {
            background: var(--card-glass);
            border-radius: 20px;
            border: 1px solid var(--border-muted);
            padding: 24px;
            backdrop-filter: blur(12px);
        }
        .search-bar-wrapper {
            display: flex;
            align-items: center;
            gap: 12px;
            background: #08090c;
            border: 1px solid var(--border-muted);
            border-radius: 14px;
            padding: 12px 20px;
            margin-bottom: 20px;
        }
        .search-bar-wrapper svg {
            color: var(--text-muted);
            flex-shrink: 0;
        }
        .search-bar-wrapper input {
            flex: 1;
            background: none;
            border: none;
            color: #fff;
            font-size: 15px;
            outline: none;
        }
        .search-bar-wrapper input::placeholder {
            color: var(--text-muted);
        }
        .search-btn {
            padding: 10px 24px;
            background: var(--accent-emerald);
            border: none;
            border-radius: 10px;
            color: #000;
            font-weight: 700;
            font-size: 12px;
            cursor: pointer;
            transition: all 0.2s;
        }
        .search-btn:hover {
            background: #10c991;
            transform: translateY(-1px);
        }
        .filter-row {
            display: grid;
            grid-template-columns: 1.5fr 1fr 1fr 1.5fr;
            gap: 16px;
            align-items: flex-end;
            margin-bottom: 16px;
        }
        .filter-actions {
            display: flex;
            gap: 10px;
        }
        .input-group label {
            display: block;
            font-size: 10px;
            text-transform: uppercase;
            color: var(--text-muted);
            margin-bottom: 8px;
            letter-spacing: 1.5px;
            font-weight: 700;
        }
        .input-group input, .input-group select {
            width: 100%;
            background: #08090c;
            border: 1px solid var(--border-muted);
            padding: 12px 16px;
            border-radius: 12px;
            color: #fff;
            font-size: 13px;
            outline: none;
            transition: all 0.2s;
        }
        .input-group input:focus, .input-group select:focus {
            border-color: var(--accent-emerald);
            box-shadow: 0 0 0 3px rgba(16, 185, 129, 0.1);
        }
        .input-group select {
            appearance: none;
            cursor: pointer;
            background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='20' height='20' viewBox='0 0 24 24' fill='none' stroke='%2394a3b8' stroke-width='2'%3E%3Cpolyline points='6 9 12 15 18 9'%3E%3C/polyline%3E%3C/svg%3E");
            background-repeat: no-repeat;
            background-position: right 12px center;
            padding-right: 40px;
        }
        .quick-filters {
            display: flex;
            align-items: center;
            gap: 10px;
            flex-wrap: wrap;
        }
        .quick-label {
            font-size: 10px;
            text-transform: uppercase;
            color: var(--text-muted);
            letter-spacing: 1px;
            font-weight: 700;
        }
        .quick-btn {
            padding: 8px 16px;
            background: rgba(255,255,255,0.03);
            border: 1px solid var(--border-muted);
            border-radius: 8px;
            color: var(--text-secondary);
            font-size: 12px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
        }
        .quick-btn:hover {
            background: rgba(16, 185, 129, 0.08);
            border-color: var(--accent-emerald);
            color: var(--accent-emerald);
        }

        /* Buttons */
        .btn {
            padding: 12px 24px;
            border-radius: 12px;
            font-weight: 700;
            cursor: pointer;
            border: none;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 1px;
            transition: all 0.3s ease;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }
        .btn-primary {
            background: var(--accent-emerald);
            color: #000;
            position: relative;
            overflow: hidden;
        }
        .btn-primary:hover {
            transform: scale(1.02);
            box-shadow: 0 0 25px var(--glow-emerald);
            background: #10c991;
        }
        .btn-outline {
            background: rgba(255,255,255,0.03);
            border: 1px solid var(--border-muted);
            color: var(--text-primary);
        }
        .btn-outline:hover {
            background: rgba(255,255,255,0.06);
            border-color: var(--text-secondary);
            color: #fff;
        }

        /* Table Section */
        .table-section {
            display: flex;
            flex-direction: column;
            gap: 20px;
        }
        .table-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .table-header h3 {
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 2px;
            color: var(--text-secondary);
            font-weight: 700;
        }
        .table-actions {
            display: flex;
            align-items: center;
            gap: 16px;
        }
        .record-count {
            font-size: 12px;
            color: var(--text-muted);
            padding: 6px 14px;
            background: rgba(255,255,255,0.03);
            border-radius: 8px;
            border: 1px solid var(--border-muted);
        }
        .record-count span {
            color: var(--accent-emerald);
            font-weight: 700;
        }
    </style>
</head>
<body>
    <div id="notif" style="position:fixed; top:20px; right:20px; padding:16px 24px; border-radius:12px; background:var(--accent-emerald); color:#000; font-weight:800; z-index:1000; display:none; animation:slideIn 0.3s ease-out;"></div>

    <div class="layout-wrapper">
        <aside class="sidebar">
            <div class="brand-box">
                <p>Maysan Labs</p>
                <span>Data Platform</span>
            </div>
            
            <nav class="nav-group">
                <p class="nav-label">Menu</p>
                <a href="/" class="nav-item active">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path><polyline points="9 22 9 12 15 12 15 22"></polyline></svg>
                    Dashboard
                </a>
            </nav>

            <nav class="nav-group">
                <p class="nav-label">Export Intelligence</p>
                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px; padding: 4px;">
                    <button class="export-btn export-csv" onclick="exportData('csv')" title="Export as CSV">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg>
                        <span>CSV</span>
                    </button>
                    <button class="export-btn export-excel" onclick="exportData('excel')" title="Export as Excel">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg>
                        <span>Excel</span>
                    </button>
                    <button class="export-btn export-json" onclick="exportData('json')" title="Export as JSON">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg>
                        <span>JSON</span>
                    </button>
                </div>
            </nav>

            <nav class="nav-group">
                <p class="nav-label">Tools</p>
                <a href="#" class="nav-item" onclick="cleanup()">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"></path></svg>
                    Clean Data
                </a>
                <a href="#" class="nav-item" onclick="updateQuality()">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path><polyline points="22 4 12 14.01 9 11.01"></polyline></svg>
                    Quality Check
                </a>
                <a href="#" class="nav-item" onclick="openMaintenance()">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"></path></svg>
                    Maintenance
                </a>
            </nav>

            <div class="system-footer">
                <p>System Status</p>
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <div class="status-online">
                        <div class="status-dot"></div>
                        <span id="status-badge">ONLINE</span>
                    </div>
                    <span id="last-update-sidebar" style="font-size:10px; color:rgba(255,255,255,0.2);">--:--:--</span>
                </div>
            </div>
        </aside>

    <main class="main-view">
        <div class="header-row">
            <div class="page-title">
                <h2>Intelligence HUD</h2>
                <p>Data Extraction Engine & Analytics Node</p>
            </div>
            <div style="display:flex; align-items:center; gap:20px;">
                <div style="background:var(--card-glass); border:1px solid var(--border-muted); padding:10px 20px; border-radius:14px; font-size:12px; display:flex; align-items:center; gap:10px; backdrop-filter:blur(10px);">
                    <span style="color:var(--text-muted); font-weight:700;">SYSTEM STATUS:</span> 
                    <span id="live-status" style="font-weight:800; color:var(--text-secondary); letter-spacing:1px;">IDLE</span>
                </div>
                <div style="background:var(--card-glass); border:1px solid var(--border-muted); padding:10px 20px; border-radius:14px; font-size:12px; backdrop-filter:blur(10px);">
                    <span style="color:var(--text-muted); font-weight:700;">LAST UPDATED:</span> <span id="last-update" class="mono" style="color:var(--text-primary); margin-left:8px;">--:--:--</span>
                </div>
            </div>
        </div>

        <div id="prog-wrap" style="display:none; margin-top: -16px;">
            <div class="progress-bar-container">
                <div id="prog-bar" class="progress-bar" style="width: 0%;"></div>
            </div>
            <p style="font-size:10px; color:var(--accent-emerald); margin-top:8px; font-weight:700; letter-spacing:1px; text-align:right;">EXTRACTION IN PROGRESS...</p>
        </div>

        <div class="stats-hud">
            <div class="stat-card">
                <span class="label">Total Intelligence</span>
                <span class="value mono" id="stat-total">{{s.total}}</span>
            </div>
            <div class="stat-card emerald">
                <span class="label">Verified Contacts</span>
                <span class="value mono" id="stat-phone">{{s.phone}}</span>
            </div>
            <div class="stat-card blue">
                <span class="label">Digital Identity</span>
                <span class="value mono" id="stat-email">{{s.email}}</span>
            </div>
            <div class="stat-card amber">
                <span class="label">Engine Precision</span>
                <span class="value mono">98.4%</span>
            </div>
        </div>

        <div class="charts-row">
            <div class="chart-card">
                <p>Industry Distribution</p>
                <div class="chart-container"><canvas id="catChart"></canvas></div>
            </div>
            <div class="chart-card">
                <p>Intelligence Sources</p>
                <div class="chart-container"><canvas id="srcChart"></canvas></div>
            </div>
            <div class="chart-card">
                <p>Extraction Velocity</p>
                <div class="chart-container"><canvas id="trendChart"></canvas></div>
            </div>
        </div>

<div class="controls-card">
            <div class="search-bar-wrapper">
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
                <input type="text" id="t-cat" placeholder="Search leads, categories, sources..." list="cats-list" value="{{selected_category or search_query}}">
                <button class="search-btn" onclick="applyFilters()">Search</button>
            </div>

            <div class="filter-row">
                <div class="input-group">
                    <label>Location</label>
                    <input type="text" id="t-city" placeholder="City..." list="cities-list" value="{{selected_city}}">
                </div>
                <div class="input-group">
                    <label>Source</label>
                    <select id="t-source">
                        <option value="">All Sources</option>
                        <option value="BAR_COUNCIL" {% if selected_source == 'BAR_COUNCIL' %}selected{% endif %}>Bar Council</option>
                        <option value="ICAI" {% if selected_source == 'ICAI' %}selected{% endif %}>ICAI</option>
                        <option value="SEBI" {% if selected_source == 'SEBI' %}selected{% endif %}>SEBI</option>
                        <option value="SITEMAP" {% if selected_source == 'SITEMAP' %}selected{% endif %}>Sitemap</option>
                        <option value="YELLOWPAGES" {% if selected_source == 'YELLOWPAGES' %}selected{% endif %}>YellowPages</option>
                        <option value="JUSTDIAL" {% if selected_source == 'JUSTDIAL' %}selected{% endif %}>JustDial</option>
                        <option value="GMB" {% if selected_source == 'GMB' %}selected{% endif %}>Google Maps</option>
                    </select>
                </div>
                <div class="input-group">
                    <label>Sort By</label>
                    <select id="t-sort" onchange="applyFilters()">
                        <option value="date" {% if sort_by == 'date' %}selected{% endif %}>Recent First</option>
                        <option value="name" {% if sort_by == 'name' %}selected{% endif %}>Name A-Z</option>
                        <option value="score" {% if sort_by == 'score' %}selected{% endif %}>Quality Score</option>
                    </select>
                </div>
                <div class="filter-actions">
                    <button class="btn btn-primary" id="start-btn" onclick="startCollection()">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"></polygon></svg>
                        Start Collection
                    </button>
                </div>
            </div>

<div class="quick-filters">
                <span class="quick-label">Quick:</span>
                <button class="quick-btn" onclick="setFilter('Delhi', 'Lawyers')">Lawyers Delhi</button>
                <button class="quick-btn" onclick="setFilter('Mumbai', 'CAs')">CAs Mumbai</button>
                <button class="quick-btn" onclick="setFilter('Bangalore', 'Doctors')">Doctors Bangalore</button>
            </div>
        </div>

        <div class="charts-row">
            <div class="chart-card">
                <p>Leads by Source</p>
                <div class="chart-container"><canvas id="sourceChart"></canvas></div>
            </div>
            <div class="chart-card">
                <p>Lead Health & Completeness</p>
                <div style="margin-top: 10px;">
                    <div style="margin-bottom: 20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px; font-size:11px; font-weight:700; color:var(--text-secondary);">
                            <span>PHONE VERIFICATION</span>
                            <span class="mono">{{s.with_phone_pct}}%</span>
                        </div>
                        <div class="progress-bar-container"><div class="progress-bar" style="width:{{s.with_phone_pct}}%; background:var(--accent-emerald);"></div></div>
                    </div>
                    <div style="margin-bottom: 20px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px; font-size:11px; font-weight:700; color:var(--text-secondary);">
                            <span>DIGITAL REACH (EMAIL)</span>
                            <span class="mono">{{s.with_email_pct}}%</span>
                        </div>
                        <div class="progress-bar-container"><div class="progress-bar" style="width:{{s.with_email_pct}}%; background:var(--accent-blue);"></div></div>
                    </div>
                    <div>
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px; font-size:11px; font-weight:700; color:var(--text-secondary);">
                            <span>AVG DATA FIDELITY</span>
                            <span class="mono">{{s.avg_quality}}%</span>
                        </div>
                        <div class="progress-bar-container"><div class="progress-bar" style="width:{{s.avg_quality}}%; background:var(--accent-amber);"></div></div>
                    </div>
                </div>
            </div>
            <div class="chart-card">
                <p>Top Categories</p>
                <div class="chart-container"><canvas id="categoryChart"></canvas></div>
            </div>
        </div>

        <div class="chart-card" style="margin-top: -12px;">
            <p>Intelligence Growth Trend (Last 7 Days)</p>
            <div class="chart-container" style="height: 120px;"><canvas id="trendChart"></canvas></div>
        </div>

        <div class="content-grid">
            <div class="glass-card">
                <div class="table-section">
                    <div class="table-header">
                        <h3>Lead Records</h3>
                        <div class="table-actions">
                            <span class="record-count">{{ contacts|length }} of {{ s.total }} records</span>
                        </div>
                    </div>
                    <div class="table-wrap">
                        <table>
                            <thead>
                                <tr>
                                    <th style="width:40px;">#</th>
                                    <th>Lead Name</th>
                                    <th>Phone</th>
                                    <th>Email</th>
                                    <th>Category</th>
                                    <th>City</th>
                                    <th>Source</th>
                                    <th style="width:100px;">Score</th>
                                    <th style="width:60px;">Actions</th>
                                </tr>
                            </thead>
                            <tbody id="leads-tbody">
                                {% for c in contacts %}
                                <tr class="lead-row">
                                    <td style="color:var(--text-muted); font-size:11px;">{{ loop.index + (page - 1) * 50 }}</td>
                                    <td style="font-weight:700; font-family:'Outfit',sans-serif; color:#fff;">{{c.name}}</td>
                                    <td class="mono" style="font-size:12px;">{{c.phone or '---'}}</td>
                                    <td class="mono" style="color:var(--accent-blue); font-size:11px;">{{c.email or '---'}}</td>
                                    <td style="font-size:12px; font-weight:500;">{{c.category}}</td>
                                    <td style="font-size:12px; color:var(--text-secondary);">{{c.city or '---'}}</td>
                                    <td><span class="badge badge-src">{{c.source}}</span></td>
                                    <td>
                                        <div class="score-wrapper">
                                            <div class="score-bar"><div class="score-fill" style="width:{{c.quality_score}}%; background:{{ 'var(--accent-emerald)' if c.quality_score > 70 else 'var(--accent-blue)' if c.quality_score > 40 else 'var(--accent-red)' }};"></div></div>
                                            <span class="mono score-value">{{c.quality_score}}%</span>
                                        </div>
                                    </td>
                                    <td>
                                        <div style="display:flex; gap:4px;">
                                            <button class="action-btn" title="Copy" onclick="copyLead('{{c.phone or c.email}}')">
                                                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                                            </button>
                                        </div>
                                    </td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                    
                    <div class="pagination" id="pagination-wrapper">
                        <div class="pagination-info">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path><circle cx="9" cy="7" r="4"></circle><path d="M23 21v-2a4 4 0 0 0-3-3.87"></path><path d="M16 3.13a4 4 0 0 1 0 7.75"></path></svg>
                            <span>Showing <span>{{ contacts|length }}</span> results</span>
                            <span style="color:var(--border-muted);">|</span>
                            <span>Page <span>{{ page }}</span> of <span>{{ total_pages }}</span></span>
                        </div>
                        <div class="pagination-btns" id="pagination-btns-container">
                            <button class="pagination-btn icon-btn" onclick="goToPage(1)" {% if page <= 1 %}disabled{% endif %} title="First">
                                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="11 17 6 12 11 7"></polyline><polyline points="18 17 13 12 18 7"></polyline></svg>
                            </button>
                            <button class="pagination-btn" onclick="changePage(-1)" {% if page <= 1 %}disabled{% endif %}>Prev</button>
                            
                            {% set start_p = [1, page - 2]|max %}
                            {% set end_p = [total_pages, start_p + 4]|min %}
                            {% set start_p = [1, end_p - 4]|max %}
                            
                            {% for p in range(start_p, end_p + 1) %}
                            <button class="pagination-btn {% if p == page %}active{% endif %}" onclick="goToPage({{ p }})">{{ p }}</button>
                            {% endfor %}

                            <button class="pagination-btn" onclick="changePage(1)" {% if page >= total_pages %}disabled{% endif %}>Next</button>
                            <button class="pagination-btn icon-btn" onclick="goToPage({{ total_pages }})" {% if page >= total_pages %}disabled{% endif %} title="Last">
                                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="13 17 18 12 13 7"></polyline><polyline points="6 17 11 12 6 7"></polyline></svg>
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </main>
</div>

    <datalist id="cities-list">{% for c in cities_default %}<option value="{{c}}">{% endfor %}</datalist>
    <datalist id="cats-list">{% for c in categories_default %}<option value="{{c}}">{% endfor %}</datalist>    <script>
        // CORE NAVIGATION FUNCTIONS (Defined early)
        window.showNotif = function(msg, dur) {
            if (dur === undefined) dur = 3000;
            const n = document.getElementById('notif');
            if (n) {
                n.innerText = msg; n.style.display = 'block';
                setTimeout(function() { n.style.display = 'none'; }, dur);
            }
        };

        window.currentPage = parseInt("{{page}}") || 1;
        window.totalPages = parseInt("{{total_pages}}") || 1;

        window.changePage = function(delta) {
            window.goToPage(window.currentPage + delta);
        };
        
        window.goToPage = function(p) {
            if (p < 1 || p > window.totalPages) return;
            const url = new URL(window.location.href);
            url.searchParams.set('page', p);
            window.loadLeads(url.toString(), true);
        };

        window.applyFilters = function() {
            const city = document.getElementById('t-city').value;
            const cat = document.getElementById('t-cat').value;
            const source = document.getElementById('t-source').value;
            const sort = document.getElementById('t-sort')?.value || 'date';
            
            const url = new URL(window.location.origin + window.location.pathname);
            if (city) url.searchParams.set('city', city);
            if (cat) url.searchParams.set('category', cat);
            if (source) url.searchParams.set('source', source);
            if (sort) url.searchParams.set('sort', sort);
            url.searchParams.set('page', 1);
            window.loadLeads(url.toString(), true);
        };

        window.setFilter = function(city, cat) {
            document.getElementById('t-city').value = city;
            document.getElementById('t-cat').value = cat;
            window.applyFilters();
        };

        window.copyLead = function(text) {
            if (!text || text === '---') {
                window.showNotif('No data to copy');
                return;
            }
            navigator.clipboard.writeText(text).then(function() {
                window.showNotif('Copied to clipboard!');
            }).catch(function() {
                window.showNotif('Failed to copy');
            });
        };

        window.loadLeads = async function(url, pushState) {
            try {
                const res = await fetch(url, { headers: { 'X-Requested-With': 'XMLHttpRequest' } });
                const data = await res.json();
                
                window.currentPage = data.page;
                window.totalPages = data.total_pages;
                
                window.renderLeads(data.contacts);
                window.updatePaginationUI(data);
                
                if (pushState) {
                    history.pushState({page: data.page, url: url}, '', url);
                }
                
                // Scroll to top of table
                document.querySelector('.glass-card').scrollIntoView({ behavior: 'smooth', block: 'start' });
            } catch (e) {
                console.error("AJAX Load Error:", e);
                window.showNotif('Failed to load data', 3000);
            }
        };

        window.renderLeads = function(leads) {
            const tbody = document.getElementById('leads-tbody');
            if (!tbody) return;
            
            if (leads.length === 0) {
                tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; padding:40px; color:var(--text-secondary);">No records found matching filters.</td></tr>';
                return;
            }

            tbody.innerHTML = leads.map(function(c) {
                const scoreColor = c.quality_score > 70 ? 'var(--accent-emerald)' : (c.quality_score > 40 ? 'var(--accent-blue)' : 'var(--accent-red)');
                const isGmail = c.email && c.email.includes('@gmail.com');
                return '<tr>' +
                    '<td style="font-weight:700; font-family:\'Outfit\',sans-serif; color:#fff;">' + c.name + '</td>' +
                    '<td class="mono" style="font-size:12px;">' + c.phone + '</td>' +
                    '<td class="mono" style="color:var(--accent-blue); font-size:11px;">' + c.email + '</td>' +
                    '<td>' + (isGmail ? '<span class="badge" style="background:rgba(16,185,129,0.1); color:var(--accent-emerald); border:1px solid rgba(16,185,129,0.2);">PERSONAL</span>' : '<span style="color:var(--text-muted); font-size:10px;">---</span>') + '</td>' +
                    '<td style="font-size:12px; font-weight:500;">' + c.category + '</td>' +
                    '<td><span class="badge badge-src">' + c.source + '</span></td>' +
                    '<td>' +
                        '<div style="display:flex; align-items:center; gap:10px;">' +
                            '<div class="progress-bar-container" style="flex:1; width:60px; margin-top:0;">' +
                                '<div class="progress-bar" style="width:' + c.quality_score + '%; background:' + scoreColor + '; box-shadow:0 0 10px ' + (c.quality_score > 70 ? 'var(--glow-emerald)' : 'rgba(59,130,246,0.2)') + ';"></div>' +
                            '</div>' +
                            '<span class="mono" style="font-size:10px; font-weight:700;">' + c.quality_score + '%</span>' +
                        '</div>' +
                    '</td>' +
                '</tr>';
            }).join('');
        };

window.updatePaginationUI = function(data) {
            var btnContainer = document.getElementById('pagination-btns-container');
            if (!btnContainer) return;

            var html = '';
            var isFirst = data.page <= 1;
            var isLast = data.page >= data.total_pages;

            html += '<button class="pagination-btn icon-btn" onclick="goToPage(1)" ' + (isFirst ? 'disabled' : '') + ' title="First">';
            html += '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="11 17 6 12 11 7"></polyline><polyline points="18 17 13 12 18 7"></polyline></svg>';
            html += '</button>';
            html += '<button class="pagination-btn" onclick="changePage(-1)" ' + (isFirst ? 'disabled' : '') + '>Prev</button>';

            var start_p = Math.max(1, data.page - 2);
            var end_p = Math.min(data.total_pages, start_p + 4);
            start_p = Math.max(1, end_p - 4);

            for (var i = start_p; i <= end_p; i++) {
                html += '<button class="pagination-btn ' + (i === data.page ? 'active' : '') + '" onclick="goToPage(' + i + ')">' + i + '</button>';
            }

            html += '<button class="pagination-btn" onclick="changePage(1)" ' + (isLast ? 'disabled' : '') + '>Next</button>';
            html += '<button class="pagination-btn icon-btn" onclick="goToPage(' + data.total_pages + ')" ' + (isLast ? 'disabled' : '') + ' title="Last">';
            html += '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="13 17 18 12 13 7"></polyline><polyline points="6 17 11 12 6 7"></polyline></svg>';
            html += '</button>';

            btnContainer.innerHTML = html;

            // Update info
            var infoEl = document.querySelector('.pagination-info');
            if (infoEl) {
                infoEl.innerHTML = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path><circle cx="9" cy="7" r="4"></circle><path d="M23 21v-2a4 4 0 0 0-3-3.87"></path><path d="M16 3.13a4 4 0 0 1 0 7.75"></path></svg>' +
                    '<span>Showing <span>' + data.contacts.length + '</span> results</span>' +
                    '<span style="color:var(--border-muted);">|</span>' +
                    '<span>Page <span>' + data.page + '</span> of <span>' + data.total_pages + '</span></span>';
            }
        };

        // Handle Browser Back/Forward
        window.addEventListener('popstate', function(event) {
            if (event.state && event.state.url) {
                window.loadLeads(event.state.url, false);
            }
        });
        
        // Initialize history state on load
        if (typeof history.replaceState === 'function') {
            history.replaceState({page: window.currentPage, url: window.location.href}, '', window.location.href);
        }

        window.startCollection = async function() {
            const city = document.getElementById('t-city').value;
            const cat = document.getElementById('t-cat').value;
            const source = document.getElementById('t-source').value;
            const btn = document.getElementById('start-btn');
            
            if(!city || !cat) return window.showNotif('Please enter location and search term', 2000);
            
            btn.disabled = true;
            btn.innerHTML = '<span class="pulse">COLLECTING...</span>';
            
            try {
                const res = await fetch('/api/trigger/scrape', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({city, category: cat, source})
                });
                const data = await res.json();
                window.showNotif(data.message);
            } catch (e) {
                window.showNotif('Failed to trigger collection');
                btn.disabled = false;
                btn.innerText = 'Start Collection';
            }
        };

        window.setTemplate = function(city, cat, src) {
            document.getElementById('t-city').value = city;
            document.getElementById('t-cat').value = cat;
            document.getElementById('t-source').value = src;
            window.applyFilters();
        };

        window.exportData = function(fmt) {
            const search = document.getElementById('t-cat')?.value || "";
            const city = document.getElementById('t-city')?.value || "";
            const cat = search;
            const src = document.getElementById('t-source')?.value || "";
            
            const url = new URL(window.location.origin + "/export/" + fmt);
            if (search) url.searchParams.set('q', search);
            if (city) url.searchParams.set('city', city);
            if (cat) url.searchParams.set('category', cat);
            if (src) url.searchParams.set('source', src);
            
            window.location.href = url.toString();
        };

        window.cleanup = async function() {
            window.showNotif('Cleaning started...');
            try {
                const res = await fetch('/api/cleanup/deep', {method: 'POST'});
                const data = await res.json();
                window.showNotif('Done: ' + data.deleted + ' deleted');
            } catch(e) { window.showNotif('Cleanup failed'); }
        };

        window.updateQuality = async function() {
            window.showNotif('Quality audit started...');
            try {
                const res = await fetch('/api/cleanup/quality', {method: 'POST'});
                const data = await res.json();
                window.showNotif('Audited ' + data.updated + ' records');
            } catch(e) { window.showNotif('Audit failed'); }
        };

        // Live Telemetry Stream
        const evtSource = new EventSource("/api/stream/stats");
        evtSource.onmessage = function(event) {
            const data = JSON.parse(event.data);
            if (document.getElementById('stat-total')) document.getElementById('stat-total').innerText = data.total;
            if (document.getElementById('stat-phone')) document.getElementById('stat-phone').innerText = data.with_phone;
            if (document.getElementById('stat-email')) document.getElementById('stat-email').innerText = data.with_email;
            if (document.getElementById('last-update')) document.getElementById('last-update').innerText = new Date().toLocaleTimeString();
            
            const status = data.scraper_status;
            const statusEl = document.getElementById('live-status');
            const progWrap = document.getElementById('prog-wrap');
            const progBar = document.getElementById('prog-bar');
            const startBtn = document.getElementById('start-btn');

            if (status && status.running) {
                if(statusEl) { statusEl.innerText = status.message || 'RUNNING'; statusEl.style.color = 'var(--accent-emerald)'; }
                if(progWrap) progWrap.style.display = 'block';
                if(progBar) progBar.style.width = (status.stats && status.stats.progress ? status.stats.progress : 100) + '%';
                if (startBtn) {
                    startBtn.disabled = true;
                    startBtn.innerHTML = '<span class="pulse">COLLECTING...</span>';
                }
            } else {
                if(statusEl) { statusEl.innerText = 'ONLINE'; statusEl.style.color = 'var(--text-secondary)'; }
                if(progWrap) progWrap.style.display = 'none';
                if (startBtn) {
                    startBtn.disabled = false;
                    startBtn.innerText = 'Start Collection';
                }
            }

            // Activity logs hidden per user request
            
            const badge = document.getElementById('status-badge');
            if (badge) {
                if (status && status.running) {
                    badge.innerText = 'SCRAPING'; badge.style.color = 'var(--accent-emerald)';
                } else {
                    badge.innerText = 'ONLINE'; badge.style.color = 'var(--text-secondary)';
                }
        // Chart.js initialization
        let sourceChart, categoryChart, trendChart;
        async function initCharts() {
            const chartColors = ['#10b981', '#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899'];
            const fontOpt = { color: '#94a3b8', font: { family: 'Inter', size: 10 } };
            const baseOpt = { 
                responsive: true, 
                maintainAspectRatio: false, 
                animation: { duration: 800, easing: 'easeOutQuart' },
                plugins: {
                    legend: {
                        labels: {
                            color: '#94a3b8',
                            font: { family: 'Inter', size: 10, weight: '500' },
                            padding: 10,
                            usePointStyle: true
                        }
                    }
                }
            };

            const srcEl = document.getElementById('sourceChart');
            if (srcEl) {
                sourceChart = new Chart(srcEl, {
                    type: 'doughnut',
                    data: { labels: [], datasets: [{ data: [], backgroundColor: chartColors, borderColor: 'rgba(0,0,0,0.2)', borderWidth: 2 }] },
                    options: { 
                        ...baseOpt, 
                        plugins: { ...baseOpt.plugins, legend: { ...baseOpt.plugins.legend, position: 'bottom' } },
                        cutout: '65%' 
                    }
                });
            }

            const catEl = document.getElementById('categoryChart');
            if (catEl) {
                categoryChart = new Chart(catEl, {
                    type: 'bar',
                    data: { labels: [], datasets: [{ data: [], backgroundColor: 'rgba(16, 185, 129, 0.7)', borderRadius: 6 }] },
                    options: { 
                        ...baseOpt, 
                        plugins: { ...baseOpt.plugins, legend: { display: false } }, 
                        scales: { 
                            x: { ticks: fontOpt, grid: { display: false } }, 
                            y: { ticks: fontOpt, grid: { color: 'rgba(255,255,255,0.03)' } }
                        }
                    }
                });
            }

            const trendEl = document.getElementById('trendChart');
            if (trendEl) {
                trendChart = new Chart(trendEl, {
                    type: 'line',
                    data: { labels: [], datasets: [{ 
                        data: [], 
                        borderColor: '#3b82f6', 
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        fill: true,
                        tension: 0.4,
                        borderWidth: 3,
                        pointRadius: 0
                    }] },
                    options: { 
                        ...baseOpt, 
                        plugins: { ...baseOpt.plugins, legend: { display: false } }, 
                        scales: { 
                            x: { ticks: fontOpt, grid: { display: false } }, 
                            y: { ticks: fontOpt, grid: { color: 'rgba(255,255,255,0.03)' } }
                        }
                    }
                });
            }

            refreshCharts();
            setInterval(refreshCharts, 30000);
        }

        async function refreshCharts() {
            try {
                const response = await fetch('/api/stats/charts');
                const stats = await response.json();
                if (!stats.sources) return;
                
                if (sourceChart) {
                    sourceChart.data.labels = stats.sources.map(function(s) { return s.source; });
                    sourceChart.data.datasets[0].data = stats.sources.map(function(s) { return s.count; });
                    sourceChart.update();
                }
                
                if (categoryChart) {
                    categoryChart.data.labels = stats.categories.slice(0,5).map(function(c) { return c.category; });
                    categoryChart.data.datasets[0].data = stats.categories.slice(0,5).map(function(c) { return c.count; });
                    categoryChart.update();
                }
                
                if (trendChart) {
                    trendChart.data.labels = stats.trend.map(function(t) { return t.date; });
                    trendChart.data.datasets[0].data = stats.trend.map(function(t) { return t.count; });
                    trendChart.update();
                }
            } catch(e) { console.log('Chart error:', e); }
        }

        // Live Feed SSE
        const evtSource = new EventSource("/api/stream/stats");
        evtSource.onmessage = function(event) {
            const data = JSON.parse(event.data);
            
            // Update Stats
            if (data.total) document.getElementById('stat-total').innerText = data.total.toLocaleString();
            if (data.with_phone) document.getElementById('stat-phone').innerText = data.with_phone.toLocaleString();
            if (data.with_email) document.getElementById('stat-email').innerText = data.with_email.toLocaleString();
            
            // Update Status
            if (data.scraper_status) {
                const s = data.scraper_status;
                document.getElementById('live-status').innerText = s.running ? 'ACTIVE' : 'IDLE';
                document.getElementById('live-status').style.color = s.running ? 'var(--accent-emerald)' : 'var(--text-secondary)';
                if (s.running) {
                    document.getElementById('prog-wrap').style.display = 'block';
                    document.getElementById('prog-bar').style.width = '100%'; // Pulsing is done via CSS
                } else {
                    document.getElementById('prog-wrap').style.display = 'none';
                }
            }
            
            // Update Terminal Feed
            if (data.activity_logs) {
                const term = document.getElementById('terminal');
                const currentContent = term.innerHTML;
                let newLogs = '';
                data.activity_logs.forEach(log => {
                    const levelClass = log.level || 'INFO';
                    newLogs += `<div class="log-entry">
                        <span class="log-time">${log.time}</span>
                        <span class="log-src">${log.source || 'SYS'}</span>
                        <span class="log-msg ${levelClass}">${log.message}</span>
                    </div>`;
                });
                term.innerHTML = newLogs;
            }
            
            document.getElementById('last-update').innerText = new Date().toLocaleTimeString();
            document.getElementById('last-update-sidebar').innerText = new Date().toLocaleTimeString();
        };

        async function openMaintenance() {
            if (!confirm("Run system-wide category normalization? This will fix duplicate charts by merging similar categories.")) return;
            
            showNotif("Starting database maintenance...");
            try {
                const resp = await fetch('/api/maintenance/normalize', { method: 'POST' });
                const res = await resp.json();
                if (res.success) {
                    showNotif(`Success! Normalized ${res.category_normalized} entries.`);
                    refreshCharts();
                } else {
                    showNotif("Error: " + res.error);
                }
            } catch(e) {
                showNotif("Maintenance failed: " + e);
            }
        }

        if (document.getElementById('sourceChart')) {
            initCharts();
        }
    </script>
</body>
</html>
"""
@app.route("/")
def index():
    try:
        config = load_config()
        scraper_cfg = config.get("scraper", {})
        page_size = int(
            os.environ.get(
                "DASHBOARD_PAGE_SIZE", scraper_cfg.get("dashboard_page_size", 50)
            )
        )

        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", page_size, type=int)

        search_query = request.args.get("q", "")
        selected_city = request.args.get("city", "")
        selected_category = request.args.get("category", "")
        selected_source = request.args.get("source", "")
        selected_quality = request.args.get("quality", "")
        sort_by = request.args.get("sort", "date")

        conn = get_db()
        cur = conn.cursor()

        # Sort mapping
        sort_map = {
            "date": "scraped_at DESC",
            "name": "name ASC",
            "city": "city ASC",
            "source": "source ASC",
            "score": "quality_score DESC"
        }
        order_by = sort_map.get(sort_by, "scraped_at DESC")

        like_op = "LIKE" if USE_SQLITE else "ILIKE"
        
        # Build WHERE clause for filters
        where_clauses = []
        params = []
        if search_query:
            where_clauses.append(f"(name {like_op} %s OR phone {like_op} %s OR email {like_op} %s)")
            search_pattern = f"%{search_query}%"
            params.extend([search_pattern, search_pattern, search_pattern])
        if selected_city:
            where_clauses.append(f"city {like_op} %s")
            params.append(f"%{selected_city}%")
        if selected_category:
            where_clauses.append(f"category {like_op} %s")
            params.append(f"%{selected_category}%")
        if selected_source:
            where_clauses.append(f"source {like_op} %s")
            params.append(f"%{selected_source}%")
        if selected_quality:
            where_clauses.append("(quality_tier = %s OR quality_tier IS NULL)")
            params.append(selected_quality)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Get total count (unfiltered)
        cur.execute("SELECT COUNT(*) as cnt FROM contacts")
        total = cur.fetchone()["cnt"]

        # Get filtered count
        count_sql = f"SELECT COUNT(*) as cnt FROM contacts WHERE {where_sql}"
        cur.execute(count_sql, params)
        filtered_total = cur.fetchone()["cnt"]

        # Final Page Calculation
        total_pages = (filtered_total + limit - 1) // limit if filtered_total > 0 else 1
        
        # Clamp and validate current page
        if page < 1: page = 1
        if page > total_pages: page = total_pages
        
        offset = (page - 1) * limit

        if USE_SQLITE:
            query_sql = f"SELECT id, name, phone, email, city, source, category, quality_tier, quality_score, scraped_at FROM contacts WHERE {where_sql} ORDER BY {order_by} LIMIT ? OFFSET ?"
            cur.execute(query_sql, params + [limit, offset])
        else:
            query_sql = f"SELECT id, name, phone, email, city, source, category, quality_tier, quality_score, scraped_at FROM contacts WHERE {where_sql} ORDER BY {order_by} LIMIT %s OFFSET %s"
            cur.execute(query_sql, params + [limit, offset])
        contacts = cur.fetchall()

        # Get unique values for filter dropdowns (CACHED)
        cities = get_cached_filter(
            "cities",
            "SELECT DISTINCT city FROM contacts WHERE city IS NOT NULL AND city <> '' ORDER BY city",
            cur
        )
        categories = get_cached_filter(
            "categories",
            "SELECT DISTINCT category FROM contacts WHERE category IS NOT NULL AND category <> '' ORDER BY category",
            cur
        )
        sources = get_cached_filter(
            "sources",
            "SELECT DISTINCT source FROM contacts WHERE source IS NOT NULL AND source <> '' ORDER BY source",
            cur
        )

        # Optimized Stats
        if USE_SQLITE:
            cur.execute("""
                SELECT 
                    SUM(CASE WHEN phone_clean IS NOT NULL AND phone_clean <> '' THEN 1 ELSE 0 END) as with_phone,
                    SUM(CASE WHEN email IS NOT NULL AND email <> '' THEN 1 ELSE 0 END) as with_email,
                    COUNT(DISTINCT city) as city_count,
                    SUM(CASE WHEN LOWER(quality_tier) = 'high' THEN 1 ELSE 0 END) as q_high,
                    SUM(CASE WHEN LOWER(quality_tier) = 'medium' THEN 1 ELSE 0 END) as q_medium,
                    SUM(CASE WHEN LOWER(quality_tier) = 'low' THEN 1 ELSE 0 END) as q_low,
                    AVG(quality_score) as avg_score
                FROM contacts
            """)
        else:
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE phone_clean IS NOT NULL AND phone_clean <> '') as with_phone,
                    COUNT(*) FILTER (WHERE email IS NOT NULL AND email <> '') as with_email,
                    COUNT(DISTINCT city) as city_count,
                    COUNT(*) FILTER (WHERE LOWER(quality_tier) = 'high') as q_high,
                    COUNT(*) FILTER (WHERE LOWER(quality_tier) = 'medium') as q_medium,
                    COUNT(*) FILTER (WHERE LOWER(quality_tier) = 'low') as q_low,
                    AVG(quality_score) as avg_score
                FROM contacts
            """)
        stats_row = cur.fetchone()
        if stats_row:
            stats_row = dict(stats_row)
        
        
        cur.execute("SELECT source, COUNT(*) as c FROM contacts GROUP BY source")
        by_source = {r["source"]: r["c"] for r in cur.fetchall()}
        cur.execute("SELECT category, COUNT(*) as c FROM contacts GROUP BY category")
        by_cat = {r["category"]: r["c"] for r in cur.fetchall()}
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Database error: {e}")
        contacts, total, filtered_total, stats_row = [], 0, 0, {}
        by_source, by_cat, total_pages, page = {}, {}, 1, 1
        cities, categories, sources = [], [], []
        selected_city = selected_category = selected_source = ""
        selected_quality = ""
        search_query = ""
        sort_by = "date"
        limit = page_size

    # Support AJAX / JSON response for flicker-free pagination
    if request.args.get("format") == "json" or request.headers.get("X-Requested-With") == "XMLHttpRequest":
        leads_list = []
        for c in contacts:
            leads_list.append({
                "id": c["id"],
                "name": c["name"],
                "phone": c["phone"] or "---",
                "email": c["email"] or "---",
                "city": c["city"] or "---",
                "source": c["source"] or "---",
                "category": c["category"] or "---",
                "quality_score": c["quality_score"] or 0
            })
        return jsonify({
            "contacts": leads_list,
            "page": page,
            "total_pages": total_pages,
            "filtered_total": filtered_total,
            "stats": {
                "total": total,
                "phone": stats_row.get("with_phone", 0) if stats_row else 0,
                "email": stats_row.get("with_email", 0) if stats_row else 0,
            }
        })

    return render_template_string(
        HTML,
        contacts=contacts,
        s={
            "total": total,
            "phone": stats_row.get("with_phone", 0) if stats_row else 0,
            "email": stats_row.get("with_email", 0) if stats_row else 0,
            "cities": stats_row.get("city_count", 0) if stats_row else 0,
            "filtered_total": filtered_total,
            "quality_high": stats_row.get("q_high", 0) if stats_row else 0,
            "quality_medium": stats_row.get("q_medium", 0) if stats_row else 0,
            "quality_low": stats_row.get("q_low", 0) if stats_row else 0,
            "avg_quality": round(stats_row.get("avg_score", 0) or 0, 1) if stats_row else 0,
            "with_phone_pct": round((stats_row.get("with_phone", 0) / total * 100) if total > 0 else 0, 1),
            "with_email_pct": round((stats_row.get("with_email", 0) / total * 100) if total > 0 else 0, 1),
        },
        by_source=by_source,
        by_cat=by_cat,
        page=page,
        total_pages=total_pages,
        cities_default=config.get("cities", []),
        categories_default=config.get("categories", []),
        cities=cities,
        categories=categories,
        sources=sources,
        selected_city=selected_city,
        selected_category=selected_category,
        selected_source=selected_source,
        selected_quality=selected_quality,
        search_query=search_query,
        sort_by=sort_by,
        limit=limit,
    )


@app.route("/api/status")
def get_status():
    def db_status():
        try:
            conn = get_db()
            cur = conn.cursor()
            placeholder = "?" if USE_SQLITE else "%s"
            cur.execute(
                f"SELECT value FROM system_status WHERE key = {placeholder}",
                ("scraper_status",),
            )
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                return json.loads(row["value"])
        except Exception:
            pass
        return None

    try:
        if redis_client:
            status = redis_client.get("scraper_status")
            if status:
                return Response(status, mimetype="application/json")
    except Exception:
        pass

    fallback = db_status()
    if fallback:
        return jsonify(fallback)

    return jsonify({"message": "Idle", "running": False})


@app.route("/api/cleanup/deep", methods=["POST"])
def api_deep_clean():
    """Trigger the deep logic-based cleanup"""
    try:
        from tasks import set_status
        set_status("🧹 Deep cleaning database...", True)
        
        def run_clean():
            try:
                conn = get_db()
                cur = conn.cursor()
                cur.execute("SELECT * FROM contacts")
                rows = cur.fetchall()
                
                deleted = 0
                updated = 0
                from processing import ProcessingHandler
                for row in rows:
                    contact = dict(row)
                    contact_id = contact['id']
                    
                    cleaned = ProcessingHandler.process_contact(contact)
                    
                    if not cleaned.get('phone_clean') and not (cleaned.get('email') and cleaned.get('email_valid')):
                        placeholder = "?" if USE_SQLITE else "%s"
                        cur.execute(f"DELETE FROM contacts WHERE id = {placeholder}", (contact_id,))
                        deleted += 1
                        continue
                        
                    if cleaned.get('phone') != row['phone'] or cleaned.get('email') != row['email']:
                        placeholder = "?" if USE_SQLITE else "%s"
                        cur.execute(
                            f"UPDATE contacts SET phone = {placeholder}, phone_clean = {placeholder}, email = {placeholder}, email_valid = {placeholder} WHERE id = {placeholder}",
                            (cleaned.get('phone'), cleaned.get('phone_clean'), cleaned.get('email'), cleaned.get('email_valid'), contact_id)
                        )
                        updated += 1
                
                conn.commit()
                cur.close()
                conn.close()
                set_status("Idle", False)
                return deleted, updated
            except Exception as e:
                set_status("Idle", False)
                raise e

        deleted, updated = run_clean()
        return jsonify({"success": True, "deleted": deleted, "updated": updated})
    except Exception as e:
        logger.error(f"Deep clean failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/contact/<int:contact_id>")
def get_contact(contact_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        placeholder = "?" if USE_SQLITE else "%s"
        cur.execute(f"SELECT * FROM contacts WHERE id = {placeholder}", (contact_id,))
        contact = cur.fetchone()
        cur.close()
        conn.close()
        if contact:
            return jsonify(dict(contact))
        return jsonify({"error": "Contact not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/logs")
def view_logs():
    try:
        log_files = []
        if LOGS_DIR.exists():
            for f in LOGS_DIR.glob("*.log"):
                log_files.append(
                    {
                        "name": f.name,
                        "size": f.stat().st_size,
                        "modified": f.stat().st_mtime,
                    }
                )
        log_files.sort(key=lambda x: x["modified"], reverse=True)
        return render_template_string(LOGS_HTML, logs=log_files[:30])
    except Exception as e:
        return f"Error reading logs: {e}"


LOG_DETAIL_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Log Detail - {{name}}</title>
    <style>
        body { background: #050508; color: #c9d1d9; font-family: 'JetBrains Mono', monospace; margin: 0; padding: 20px; line-height: 1.5; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #2d3148; padding-bottom: 10px; margin-bottom: 20px; }
        .terminal { background: #0a0a0f; padding: 20px; border-radius: 12px; border: 1px solid #2d3148; overflow-x: auto; font-size: 12px; }
        .back-btn { color: #10b981; text-decoration: none; font-size: 14px; font-weight: bold; }
        .back-btn:hover { text-decoration: underline; }
        .line { margin-bottom: 4px; padding-left: 8px; border-left: 2px solid transparent; }
        .ERROR { color: #ef4444; border-left-color: #ef4444; background: rgba(239, 68, 68, 0.05); }
        .SUCCESS { color: #10b981; border-left-color: #10b981; }
        .INFO { color: #94a3b8; }
        .WARNING { color: #f59e0b; border-left-color: #f59e0b; }
        h2 { margin:0; font-size:16px; color: #fff; }
    </style>
</head>
<body>
    <div class="header">
        <a href="/logs" class="back-btn">← Back to Logs</a>
        <h2>{{name}}</h2>
        <div style="font-size: 10px; color: #64748b;">LIVE TAIL (LAST 1000 LINES)</div>
    </div>
    <div class="terminal">
        {% for line in lines %}
        <div class="line {{ 'ERROR' if 'ERROR' in line else 'SUCCESS' if 'SUCCESS' in line else 'WARNING' if 'WARNING' in line else 'INFO' }}">{{ line }}</div>
        {% endfor %}
    </div>
    <script>
        window.scrollTo(0, document.body.scrollHeight);
    </script>
</body>
</html>
"""

@app.route("/logs/<name>")
def get_log(name):
    try:
        log_file = LOGS_DIR / name
        if log_file.exists():
            content = log_file.read_text(errors='replace')
            lines = content.split("\n")
            return render_template_string(LOG_DETAIL_HTML, name=name, lines=lines[-1000:])
        return "Log file not found", 404
    except Exception as e:
        return f"Error reading log detail: {e}", 500


@app.route("/api/trigger/scrape", methods=["POST", "GET"])
def trigger_scrape():
    """Trigger scraping tasks. Supports single (POST JSON) or batch (default)."""
    os.environ.setdefault("CELERY_HEALTH_SERVER_STARTED", "1")
    from tasks import fast_scrape_task, scrape_category_task, set_status
    
    data = {}
    if request.method == "POST":
        try:
            data = request.get_json() or {}
        except:
            data = {}
    
    city = data.get("city") or request.args.get("city")
    category = data.get("category") or request.args.get("category")
    source = data.get("source") or request.args.get("source")
    use_business = data.get("use_business", False)
    
    if not use_business:
        use_business = request.args.get("business", "false").lower() == "true"

    if city and category:
        log_msg = f"Dashboard triggered manual scrape: {category} in {city} (Source: {source or 'Auto'})"
        set_status(
            f"Queued: {category} in {city}...",
            True,
            {"city": city, "category": category, "source": source or "QUEUE"},
        )
        task_result = scrape_category_task.delay(city=city, category=category, source=source, use_business=use_business)
        msg = f"🚀 Scrape queued for {category} in {city}!"
        logger.info(log_msg)
    else:
        set_status(
            "Queued batch fast-scrape for all configured targets...",
            True,
            {"source": source or "QUEUE"},
        )
        task_result = fast_scrape_task.delay(source=source)
        msg = f"🚀 Batch fast-scrape queued for all Official sources!"
    
    return jsonify({"message": msg, "task_id": getattr(task_result, "id", None)})

@app.route("/api/trigger/fast-scrape", methods=["POST"])
def trigger_fast_scrape():
    """Trigger fast parallel scraping with higher concurrency"""
    os.environ.setdefault("CELERY_HEALTH_SERVER_STARTED", "1")
    from tasks import fast_scrape_task, set_status
    try:
        max_concurrent = request.args.get("concurrency", 5, type=int)
        set_status(
            f"Queued fast scrape with concurrency={max_concurrent}...",
            True,
            {"source": "QUEUE", "concurrency": max_concurrent},
        )
        task_result = fast_scrape_task.delay(max_concurrent=max_concurrent)
        return jsonify({
            "message": f"⚡ Fast scrape queued with concurrency={max_concurrent}!",
            "task_id": getattr(task_result, "id", None),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/contacts")
def api_contacts():
    try:
        conn = get_db()
        cur = conn.cursor()

        page = request.args.get("page", 1, type=int)
        limit = min(request.args.get("limit", 100, type=int), 1000)
        offset = (page - 1) * limit

        search_query = request.args.get("q", "")
        filter_city = request.args.get("city", "")
        filter_category = request.args.get("category", "")
        filter_source = request.args.get("source", "")

        like_op = "LIKE" if USE_SQLITE else "ILIKE"
        where_clauses = []
        params = []
        if search_query:
            where_clauses.append(f"(name {like_op} %s OR phone {like_op} %s OR email {like_op} %s)")
            search_pattern = f"%{search_query}%"
            params.extend([search_pattern, search_pattern, search_pattern])
        if filter_city:
            where_clauses.append(f"city {like_op} %s")
            params.append(filter_city)
        if filter_category:
            where_clauses.append(f"category {like_op} %s")
            params.append(filter_category)
        if filter_source:
            where_clauses.append(f"source {like_op} %s")
            params.append(filter_source)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        if USE_SQLITE:
            where_sql = where_sql.replace("%s", "?")
        
        placeholder = "?" if USE_SQLITE else "%s"
        query = f"SELECT name, phone, email, city, category, source FROM contacts WHERE {where_sql} ORDER BY scraped_at DESC LIMIT {placeholder} OFFSET {placeholder}"
        count_query = f"SELECT COUNT(*) as cnt FROM contacts WHERE {where_sql}"

        cur.execute(query, params + [limit, offset])
        contacts = cur.fetchall()
        cur.execute(count_query, params)
        total = cur.fetchone()["cnt"]
        cur.close()
        conn.close()
        return jsonify(
            {
                "total": total,
                "page": page,
                "limit": limit,
                "total_pages": (total + limit - 1) // limit,
                "data": [dict(c) for c in contacts],
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/export/<fmt>")
def export(fmt):
    try:
        search_query = request.args.get("q", "")
        filter_city = request.args.get("city", "")
        filter_category = request.args.get("category", "")
        filter_source = request.args.get("source", "")

        conn = get_db()
        cur = conn.cursor()

        where_clauses = []
        params = []
        if search_query:
            where_clauses.append("(name LIKE ? OR phone LIKE ? OR email LIKE ?)")
            params.extend([f"%{search_query}%", f"%{search_query}%", f"%{search_query}%"])
        if filter_city:
            where_clauses.append("city LIKE ?")
            params.append(f"%{filter_city}%")
        if filter_category:
            where_clauses.append("category LIKE ?")
            params.append(f"%{filter_category}%")
        if filter_source:
            where_clauses.append("source LIKE ?")
            params.append(f"%{filter_source}%")

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        cur.execute(f"SELECT * FROM contacts WHERE {where_sql}", params)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Export error: {e}")
        return jsonify({"error": str(e)}), 500

    if not rows:
        rows = []

    if fmt == "csv":
        import csv
        out = io.StringIO()
        fields = ["name", "phone", "email", "address", "category", "city", "area", "state", "source", "scraped_at"]
        if rows:
            fields = list(rows[0].keys())
        w = csv.DictWriter(out, fieldnames=fields)
        w.writeheader()
        for r in rows:
            row = {}
            for k, v in r.items():
                if isinstance(v, (datetime, date)):
                    row[k] = v.isoformat()
                else:
                    row[k] = v
            w.writerow(row)
        return Response(out.getvalue(), mimetype="text/csv", headers={"Content-Disposition": f"attachment;filename=leads_export_{int(time.time())}.csv"})

    if fmt == "json":
        return jsonify({"status": "success", "count": len(rows), "timestamp": datetime.now().isoformat(), "data": rows})

    if fmt == "excel":
        wb = Workbook()
        ws = wb.active
        ws.title = "Intelligence Data"
        if rows:
            ws.append(list(rows[0].keys()))
            for r in rows:
                ws.append([r.get(k, "") for k in rows[0].keys()])
        else:
            ws.append(["No data found for selected filters"])
        out = io.BytesIO()
        wb.save(out)
        out.seek(0)
        return send_file(out, download_name=f"leads_export_{int(time.time())}.xlsx", as_attachment=True, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return "Invalid format", 400


LOGS_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Scraper Logs</title>
    <style>
        body { background: #0d1117; color: #c9d1d9; font-family: monospace; padding: 20px; }
        h1 { color: #fff; }
        .log-list { list-style: none; padding: 0; }
        .log-list li { padding: 10px; border-bottom: 1px solid #2d3148; }
        .log-list a { color: #58a6ff; text-decoration: none; }
        .log-list a:hover { text-decoration: underline; }
        .log-content { background: #161824; padding: 20px; border-radius: 8px; overflow-x: auto; white-space: pre-wrap; font-size: 12px; max-height: 70vh; }
        .back { color: #8b8fa3; margin-bottom: 20px; }
    </style>
</head>
<body>
    <h1>Scraper Logs</h1>
    <a class="back" href="/">← Back to Dashboard</a>
    {% if logs %}
    <ul class="log-list">
    {% for log in logs %}
        <li><a href="/logs/{{log.name}}">{{log.name}}</a> - {{(log.size/1024)|round(1)}} KB</li>
    {% endfor %}
    </ul>
    {% else %}
    <p>No logs found.</p>
    {% endif %}
</body>
</html>
"""


@app.route("/api/cleanup/empty", methods=["DELETE"])
def cleanup_empty_contacts():
    """Delete contacts that have neither phone nor email"""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM contacts 
            WHERE (phone IS NULL OR TRIM(phone) = '') 
            AND (email IS NULL OR TRIM(email) = '')
        """)
        deleted_count = cur.rowcount
        conn.commit()
        cur.execute("SELECT COUNT(*) as cnt FROM contacts")
        remaining = cur.fetchone()["cnt"]
        cur.close()
        conn.close()
        return jsonify({
            "success": True,
            "deleted": deleted_count,
            "remaining": remaining,
            "message": f"Deleted {deleted_count} contacts with no phone or email",
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/cleanup/quality", methods=["POST"])
def cleanup_low_quality():
    """Recalculate and update quality scores for all contacts"""
    try:
        from processing import ProcessingHandler
        from tasks import set_status
        set_status("🔍 Auditing lead quality...", True)
        
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM contacts")
        contacts = cur.fetchall()
        if not contacts:
            set_status("Idle", False)
            return jsonify({"success": True, "updated": 0, "message": "No contacts to update"})
            
        updated = 0
        for contact in contacts:
            try:
                processed = ProcessingHandler.process_contact(dict(contact))
                placeholder = "?" if USE_SQLITE else "%s"
                cur.execute(f"""
                    UPDATE contacts 
                    SET phone_clean = {placeholder}, 
                        email_valid = {placeholder}, 
                        quality_score = {placeholder}, 
                        quality_tier = {placeholder}
                    WHERE id = {placeholder}
                """, (processed.get("phone_clean"), processed.get("email_valid", False), processed.get("quality_score", 0), processed.get("quality_tier", "low"), contact["id"]))
                updated += 1
                
                # Periodically commit and update status for very large sets
                if updated % 500 == 0:
                    conn.commit()
                    set_status(f"🔍 Audited {updated} leads...", True)
                    
            except Exception:
                continue
                
        conn.commit()
        cur.close()
        conn.close()
        set_status("Idle", False)
        return jsonify({"success": True, "updated": updated})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
@app.route("/api/maintenance/normalize", methods=["POST"])
def api_maintenance_normalize():
    """Trigger system-wide category normalization"""
    try:
        from processing import ProcessingHandler
        from tasks import set_status
        set_status("🧹 Normalizing all categories...", True)
        
        conn = get_db()
        stats = ProcessingHandler.clean_database_logic(conn)
        conn.close()
        
        set_status("Idle", False)
        return jsonify({"success": True, **stats})
    except Exception as e:
        logger.error(f"Maintenance failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/stream/stats")
def stream_stats():
    """Server-Sent Events endpoint for live stats updates"""
    def generate():
        while True:
            try:
                conn = get_db()
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) as cnt FROM contacts")
                total = cur.fetchone()["cnt"]
                placeholder = "?" if USE_SQLITE else "%s"
                cur.execute(f"SELECT COUNT(*) as cnt FROM contacts WHERE phone IS NOT NULL AND phone <> {placeholder}", ("",))
                with_phone = cur.fetchone()["cnt"]
                cur.execute(f"SELECT COUNT(*) as cnt FROM contacts WHERE email IS NOT NULL AND email <> {placeholder}", ("",))
                with_email = cur.fetchone()["cnt"]
                status_data = {}
                if redis_client:
                    try:
                        raw_status = redis_client.get("scraper_status")
                        if raw_status: status_data = json.loads(raw_status)
                    except: pass
                if not status_data:
                    try:
                        cur.execute("SELECT value FROM system_status WHERE key = 'scraper_status'")
                        row = cur.fetchone()
                        if row: status_data = json.loads(row["value"])
                    except: pass
                cur.execute("SELECT * FROM scraper_logs ORDER BY created_at DESC LIMIT 15")
                logs = [dict(r) for r in cur.fetchall()]
                for l in logs:
                    if isinstance(l['created_at'], datetime):
                        l['time'] = l['created_at'].strftime("%H:%M:%S")
                    else:
                        l['time'] = str(l['created_at'])[-8:]
                yield f"data: {json.dumps({'total': total, 'with_phone': with_phone, 'with_email': with_email, 'scraper_status': status_data, 'activity_logs': logs}, cls=CustomJSONEncoder)}\n\n"
                cur.close()
                conn.close()
            except Exception as e:
                logger.error(f"Stream error: {e}")
            time.sleep(2)
    return Response(generate(), mimetype="text/event-stream")


@app.route("/api/stats/charts")
def api_chart_stats():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT source, COUNT(*) as count FROM contacts GROUP BY source")
        sources = [dict(r) for r in cur.fetchall()]
        # Normalize categories in Python to ensure perfect grouping regardless of DB state
        cur.execute("SELECT category, COUNT(*) as count FROM contacts GROUP BY category")
        raw_cats = cur.fetchall()
        from processing import ProcessingHandler
        cat_map = {}
        for r in raw_cats:
            norm = ProcessingHandler.normalize_category(r["category"])
            cat_map[norm] = cat_map.get(norm, 0) + r["count"]
        
        categories = [{"category": k, "count": v} for k, v in sorted(cat_map.items(), key=lambda x: x[1], reverse=True)[:10]]

        if USE_SQLITE:
            cur.execute("SELECT strftime('%Y-%m-%d', scraped_at) as date, COUNT(*) as count FROM contacts GROUP BY date ORDER BY date DESC LIMIT 7")
        else:
            cur.execute("SELECT scraped_at::date as date, COUNT(*) as count FROM contacts GROUP BY date ORDER BY date DESC LIMIT 7")
        trend = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify({"sources": sources, "categories": categories, "trend": trend[::-1]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(PORT), debug=True)
