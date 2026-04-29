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

# Start Watchdog
watchdog = ScraperWatchdog()
watchdog.start()


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
    <title>Aurora Obsidian | Registry HUD</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-obsidian: #08090d;
            --card-glass: rgba(13, 14, 21, 0.8);
            --accent-emerald: #10b981;
            --accent-blue: #3b82f6;
            --accent-red: #ef4444;
            --text-primary: #f1f5f9;
            --text-secondary: #64748b;
            --border-muted: rgba(255,255,255,0.05);
            --border-glow: rgba(16, 185, 129, 0.3);
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { 
            font-family: 'Inter', sans-serif; 
            background: var(--bg-obsidian); 
            color: var(--text-primary); 
            min-height: 100vh;
            display: grid;
            grid-template-columns: 280px 1fr;
            background-image: radial-gradient(circle at 0% 0%, rgba(16, 185, 129, 0.05) 0%, transparent 50%);
        }

        /* Sidebar */
        .sidebar {
            background: rgba(0,0,0,0.3);
            border-right: 1px solid var(--border-muted);
            padding: 24px;
            display: flex;
            flex-direction: column;
            gap: 32px;
        }
        .brand-box { margin-bottom: 16px; }
        .brand-box p { font-size: 16px; font-weight: 800; color: var(--text-primary); letter-spacing: -0.5px; }
        .brand-box span { font-size: 9px; text-transform: uppercase; letter-spacing: 2px; color: var(--accent-emerald); display: block; margin-top: 4px; }

        .nav-group { display: flex; flex-direction: column; gap: 4px; }
        .nav-label { font-size: 9px; text-transform: uppercase; color: var(--text-secondary); letter-spacing: 1px; margin-bottom: 8px; }
        .nav-item { 
            padding: 10px 12px; border-radius: 8px; color: var(--text-secondary); 
            text-decoration: none; font-size: 13px; font-weight: 500; transition: 0.2s;
            display: flex; align-items: center; gap: 10px;
        }
        .nav-item:hover { background: rgba(255,255,255,0.03); color: #fff; }
        .nav-item.active { background: rgba(16, 185, 129, 0.1); color: var(--accent-emerald); }

        /* Layout Wrapper */
        .layout-wrapper { display: flex; height: 100vh; overflow: hidden; }
        .sidebar { width: 260px; background: #050508; border-right: 1px solid var(--border-muted); padding: 24px; display: flex; flex-direction: column; flex-shrink: 0; }
        .main-view { flex: 1; padding: 24px; overflow-y: auto; background: #050508; }
        .logs-sidebar { width: 320px; background: #050508; border-left: 1px solid var(--border-muted); padding: 24px; display: flex; flex-direction: column; flex-shrink: 0; }
        .header-row { display: flex; justify-content: space-between; align-items: flex-end; margin-bottom: 24px; }
        
        /* HUD Components */
        .stats-hud { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }
        .stat-card { 
            background: var(--card-glass); padding: 20px; border-radius: 16px; border: 1px solid var(--border-muted);
            transition: 0.2s;
        }
        .stat-card:hover { border-color: var(--border-glow); }
        .stat-card .label { font-size: 10px; text-transform: uppercase; color: var(--text-secondary); letter-spacing: 1px; margin-bottom: 8px; display: block; }
        .stat-card .value { font-size: 28px; font-weight: 800; font-family: 'JetBrains Mono', monospace; }
        .stat-card.emerald .value { color: var(--accent-emerald); }
        .stat-card.blue .value { color: var(--accent-blue); }

        .content-grid { display: flex; flex-direction: column; gap: 32px; }
        .glass-card { background: var(--card-glass); border-radius: 20px; border: 1px solid var(--border-muted); padding: 24px; }
        
        /* Charts */
        .charts-row { display: flex; gap: 16px; margin-bottom: 24px; }
        .chart-card { flex: 1; background: var(--card-glass); border-radius: 12px; border: 1px solid var(--border-muted); padding: 12px; min-height: 180px; }
        .chart-card p { font-size: 9px; text-transform: uppercase; color: var(--text-secondary); margin-bottom: 8px; letter-spacing: 1px; }
        .chart-container { position: relative; height: 150px; width: 100%; }
        
        /* Terminal & Feed */
        .terminal-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
        .terminal { 
            background: #0a0a0f; border-radius: 12px; padding: 16px; flex: 1; overflow-y: auto;
            font-family: 'JetBrains Mono', monospace; font-size: 10px; line-height: 1.6;
            border: 1px solid var(--border-muted);
            scrollbar-width: thin;
            scrollbar-color: var(--accent-emerald) transparent;
        }
        .log-entry { margin-bottom: 4px; border-left: 2px solid var(--border-muted); padding-left: 10px; display: flex; gap: 10px; }
        .log-time { color: #475569; min-width: 60px; }
        .log-src { color: var(--accent-blue); font-weight: 800; min-width: 70px; }
        .log-msg { color: #cbd5e1; }
        .log-msg.ERROR { color: var(--accent-red); }
        .log-msg.SUCCESS { color: var(--accent-emerald); }

        /* Controls */
        .controls-grid { display: grid; grid-template-columns: 1fr 1fr auto; gap: 16px; align-items: flex-end; margin-bottom: 32px; }
        .input-group label { display: block; font-size: 10px; text-transform: uppercase; color: var(--text-secondary); margin-bottom: 8px; letter-spacing: 1px; }
        .input-group input, .input-group select { 
            width: 100%; background: #0a0a0f; border: 1px solid var(--border-muted); padding: 12px 16px; 
            border-radius: 12px; color: #fff; font-size: 14px; outline: none; transition: 0.2s;
        }
        .input-group input:focus, .input-group select:focus { border-color: var(--accent-emerald); }
        .input-group select { appearance: none; cursor: pointer; }

        .btn { 
            padding: 12px 24px; border-radius: 12px; font-weight: 800; cursor: pointer; border: none; font-size: 12px;
            text-transform: uppercase; letter-spacing: 1px; transition: 0.2s; display: inline-flex; align-items: center; gap: 8px;
        }
        .btn-primary { background: var(--accent-emerald); color: #000; }
        .btn-primary:hover { transform: scale(1.02); box-shadow: 0 0 20px rgba(16, 185, 129, 0.4); }
        .btn-outline { background: transparent; border: 1px solid var(--border-muted); color: var(--text-primary); }
        .btn-outline:hover { border-color: #fff; }
        .btn-sm { padding: 6px 12px; font-size: 10px; }

        /* HUD Table */
        .table-wrap { background: #0a0a0f; border-radius: 12px; overflow: hidden; border: 1px solid var(--border-muted); }
        table { width: 100%; border-collapse: collapse; }
        th { background: rgba(255,255,255,0.02); padding: 12px 16px; text-align: left; font-size: 10px; text-transform: uppercase; color: var(--text-secondary); letter-spacing: 1px; }
        td { padding: 12px 16px; border-bottom: 1px solid var(--border-muted); font-size: 12px; }
        tr:hover td { background: rgba(255,255,255,0.01); }
        .badge { padding: 4px 8px; border-radius: 6px; font-size: 9px; font-weight: 800; }
        .badge-src { background: rgba(59, 130, 246, 0.15); color: var(--accent-blue); }
        
        /* Pagination */
        .pagination { display: flex; align-items: center; justify-content: space-between; padding: 16px 0; }
        .pagination-info { font-size: 11px; color: var(--text-secondary); }
        .pagination-btns { display: flex; gap: 6px; }
        .pagination-btn { padding: 6px 12px; border-radius: 6px; border: 1px solid var(--border-muted); background: transparent; color: var(--text-secondary); font-size: 11px; cursor: pointer; transition: 0.2s; }
        .pagination-btn:hover { border-color: var(--accent-emerald); color: var(--accent-emerald); }
        .pagination-btn.active { background: var(--accent-emerald); color: #000; border-color: var(--accent-emerald); }
        .pagination-btn:disabled { opacity: 0.3; cursor: not-allowed; }
        
        .pulse { animation: pulse 2s infinite; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.3; } 100% { opacity: 1; } }

        .progress-container { background: rgba(255,255,255,0.05); height: 4px; border-radius: 2px; margin-top: 12px; overflow: hidden; display: none; }
        .progress-bar { height: 100%; background: var(--accent-emerald); transition: width 0.3s; }
    </style>
</head>
<body>
    <div id="notif" style="position:fixed; top:20px; right:20px; padding:16px 24px; border-radius:12px; background:var(--accent-emerald); color:#000; font-weight:800; z-index:1000; display:none; animation:slideIn 0.3s ease-out;"></div>

    <div class="layout-wrapper">
        <div class="brand-box">
            <p>Maysan Labs</p>
            <span>Data Platform</span>
        </div>
        
        <nav class="nav-group">
            <p class="nav-label">Menu</p>
            <a href="/" class="nav-item active">Dashboard</a>
            <a href="/logs" class="nav-item">Activity Logs</a>
            <a href="#" class="nav-item" onclick="exportData('csv')">Export Data</a>
        </nav>

        <nav class="nav-group">
            <p class="nav-label">Tools</p>
            <a href="#" class="nav-item" onclick="cleanup()">Clean Data</a>
            <a href="#" class="nav-item" onclick="updateQuality()">Quality Check</a>
        </nav>

        <div style="margin-top:auto; padding:16px; background:rgba(0,0,0,0.2); border-radius:12px;">
            <p style="font-size:10px; color:var(--text-secondary);">System Status</p>
            <p style="font-size:11px; font-weight:600; color:var(--accent-emerald);">Running</p>
        </div>
    </aside>

    <main class="main-view">
        <div class="header-row">
            <div class="page-title">
                <h2 style="font-size:24px; font-weight:800;">Data Dashboard</h2>
                <p style="font-size:12px; color:var(--text-secondary);">Manage and explore your collected leads</p>
            </div>
            <div style="display:flex; align-items:center; gap:16px;">
                <div style="background:rgba(255,255,255,0.03); border:1px solid var(--border-muted); padding:8px 16px; border-radius:12px; font-size:12px; display:flex; align-items:center; gap:8px;">
                    <span style="color:var(--text-secondary);">STATUS:</span> 
                    <span id="live-status" style="font-weight:800; color:var(--text-secondary);">IDLE</span>
                </div>
                <div style="background:rgba(255,255,255,0.03); border:1px solid var(--border-muted); padding:8px 16px; border-radius:12px; font-size:12px;">
                    <span style="color:var(--text-secondary);">Updated:</span> <span id="last-update">--:--:--</span>
                </div>
            </div>
        </div>

        <div id="prog-wrap" class="progress-container" style="margin-top: -16px; margin-bottom: 24px;">
            <div id="prog-bar" class="progress-bar" style="width: 0%;"></div>
        </div>

        <div class="stats-hud">
            <div class="stat-card">
                <span class="label">Total Leads</span>
                <span class="value" id="stat-total">{{s.total}}</span>
            </div>
            <div class="stat-card emerald">
                <span class="label">Verified Phones</span>
                <span class="value" id="stat-phone">{{s.phone}}</span>
            </div>
            <div class="stat-card blue">
                <span class="label">Valid Emails</span>
                <span class="value" id="stat-email">{{s.email}}</span>
            </div>
            <div class="stat-card">
                <span class="label">Status</span>
                <span id="live-status" style="font-size:16px; font-weight:800; color:var(--text-secondary);">IDLE</span>
                <div class="progress-container" id="prog-wrap"><div class="progress-bar" id="prog-bar" style="width:0%"></div></div>
            </div>
        </div>

        <!-- Charts Section -->
        <div class="charts-row">
            <div class="chart-card">
                <p>Leads by Source</p>
                <div class="chart-container"><canvas id="sourceChart"></canvas></div>
            </div>
            <div class="chart-card">
                <p>Top Categories</p>
                <div class="chart-container"><canvas id="categoryChart"></canvas></div>
            </div>
            <div class="chart-card">
                <p>Growth Trend</p>
                <div class="chart-container"><canvas id="trendChart"></canvas></div>
            </div>
        </div>

        <div class="content-grid">
            <div class="glass-card">
                <div class="controls-grid">
                    <div class="input-group">
                        <label>Search For</label>
                        <input type="text" id="t-cat" placeholder="e.g. Lawyers" list="cats-list" value="{{selected_category or search_query}}">
                    </div>
                    <div class="input-group">
                        <label>Location</label>
                        <input type="text" id="t-city" placeholder="e.g. Delhi" list="cities-list" value="{{selected_city}}">
                    </div>
                    <div class="input-group">
                        <label>Data Source</label>
                        <select id="t-source">
                            <option value="">Auto-Select</option>
                            <option value="BAR_COUNCIL" {% if selected_source == 'BAR_COUNCIL' %}selected{% endif %}>Bar Council (Lawyers)</option>
                            <option value="ICAI" {% if selected_source == 'ICAI' %}selected{% endif %}>ICAI (CAs)</option>
                            <option value="SEBI" {% if selected_source == 'SEBI' %}selected{% endif %}>SEBI (Advisors)</option>
                            <option value="SITEMAP" {% if selected_source == 'SITEMAP' %}selected{% endif %}>Sitemap</option>
                            <option value="YELLOWPAGES" {% if selected_source == 'YELLOWPAGES' %}selected{% endif %}>YellowPages</option>
                            <option value="JUSTDIAL" {% if selected_source == 'JUSTDIAL' %}selected{% endif %}>JustDial</option>
                            <option value="GMB" {% if selected_source == 'GMB' %}selected{% endif %}>Google Maps</option>
                        </select>
                    </div>
                    <div class="input-group" style="display:flex; align-items:flex-end; gap:10px;">
                        <button class="btn btn-outline" style="flex:1;" onclick="applyFilters()">Apply Filters</button>
                        <button class="btn btn-primary" id="start-btn" style="flex:1;" onclick="startCollection()">Start Collection</button>
                    </div>
                </div>

                <div style="margin-bottom: 24px; display: flex; gap: 12px; flex-wrap: wrap;">
                    <span style="font-size: 10px; color: var(--text-secondary); align-self: center;">Quick Filters:</span>
                    <button class="btn btn-outline btn-sm" onclick="setTemplate('Delhi', 'Lawyers', 'BAR_COUNCIL')">Lawyers in Delhi</button>
                    <button class="btn btn-outline btn-sm" onclick="setTemplate('Mumbai', 'Chartered Accountants', 'ICAI')">CAs in Mumbai</button>
                    <button class="btn btn-outline btn-sm" onclick="setTemplate('Bangalore', 'Software Companies', 'YELLOWPAGES')">Tech in Bangalore</button>
                </div>

                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>Lead Name</th>
                                <th>Phone</th>
                                <th>Category</th>
                                <th>Source</th>
                                <th>Score</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for c in contacts %}
                            <tr>
                                <td style="font-weight:700;">{{c.name}}</td>
                                <td>{{c.phone or '---'}}</td>
                                <td>{{c.category}}</td>
                                <td><span class="badge badge-src">{{c.source}}</span></td>
                                <td>
                                    <div style="display:flex; align-items:center; gap:8px;">
                                        <div style="flex:1; background:rgba(255,255,255,0.05); height:4px; width:40px; border-radius:2px;">
                                            <div style="height:100%; background:{{ 'var(--accent-emerald)' if c.quality_score > 70 else 'var(--accent-blue)' if c.quality_score > 40 else 'var(--accent-red)' }}; width:{{c.quality_score}}%;"></div>
                                        </div>
                                        <span style="font-size:10px;">{{c.quality_score}}%</span>
                                    </div>
                                </td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
                
                <div class="pagination">
                    <div class="pagination-info">
                        Showing {{ contacts|length }} of {{ s.filtered_total }} leads
                        <span style="margin-left: 10px; color: rgba(255,255,255,0.2);">|</span>
                        <span style="margin-left: 10px;">Page {{ page }} of {{ total_pages }}</span>
                    </div>
                    <div class="pagination-btns">
                        <button class="pagination-btn" onclick="goToPage(1)" {% if page <= 1 %}disabled{% endif %}>First</button>
                        <button class="pagination-btn" onclick="changePage(-1)" {% if page <= 1 %}disabled{% endif %}>Prev</button>
                        
                        {% set start_p = [1, page - 2]|max %}
                        {% set end_p = [total_pages, start_p + 4]|min %}
                        {% set start_p = [1, end_p - 4]|max %}
                        
                        {% for p in range(start_p, end_p + 1) %}
                        <button class="pagination-btn {% if p == page %}active{% endif %}" onclick="goToPage({{ p }})">{{ p }}</button>
                        {% endfor %}

                        <button class="pagination-btn" onclick="changePage(1)" {% if page >= total_pages %}disabled{% endif %}>Next</button>
                        <button class="pagination-btn" onclick="goToPage({{ total_pages }})" {% if page >= total_pages %}disabled{% endif %}>Last</button>
                    </div>
                </div>
            </div>

            </div>
        </div>
    </main>

    <aside class="logs-sidebar">
        <div class="terminal-header">
            <div>
                <p style="font-size:10px; font-weight:800; color:var(--text-secondary); text-transform:uppercase; letter-spacing:1px;">Live Activity</p>
                <p style="font-size:9px; color:rgba(255,255,255,0.2);">Real-time stream</p>
            </div>
            <div class="pulse" style="width:8px; height:8px; background:var(--accent-emerald); border-radius:50%;"></div>
        </div>
        <div class="terminal" id="activity-logs">
            <!-- Logs will stream here -->
        </div>
        
        <div style="margin-top:24px; padding:16px; background:rgba(255,255,255,0.02); border-radius:12px; border:1px solid var(--border-muted);">
            <p style="font-size:10px; color:var(--text-secondary); margin-bottom:8px;">LAST TELEMETRY</p>
            <div style="display:flex; justify-content:space-between; font-size:11px;">
                <span id="last-update-sidebar">--:--:--</span>
                <span id="status-badge" style="color:var(--accent-emerald); font-weight:800;">ONLINE</span>
            </div>
        </div>
    </aside>
</div>

    <datalist id="cities-list">{% for c in cities_default %}<option value="{{c}}">{% endfor %}</datalist>
    <datalist id="cats-list">{% for c in categories_default %}<option value="{{c}}">{% endfor %}</datalist>

    <script>
        function showNotif(msg, dur=3000) {
            const n = document.getElementById('notif');
            n.innerText = msg; n.style.display = 'block';
            setTimeout(() => { n.style.display = 'none'; }, dur);
        }

        let currentPage = {{page}};
        let totalPages = {{total_pages}};

        function changePage(delta) {
            const newPage = currentPage + delta;
            if (newPage < 1 || newPage > totalPages) return;
            const url = new URL(window.location);
            url.searchParams.set('page', newPage);
            window.location.href = url.toString();
        }
        
        function goToPage(p) {
            if (p < 1 || p > totalPages) return;
            const url = new URL(window.location);
            url.searchParams.set('page', p);
            window.location.href = url.toString();
        }

        function applyFilters() {
            const city = document.getElementById('t-city').value;
            const cat = document.getElementById('t-cat').value;
            const source = document.getElementById('t-source').value;
            
            const url = new URL(window.location);
            if (city) url.searchParams.set('city', city); else url.searchParams.delete('city');
            if (cat) url.searchParams.set('category', cat); else url.searchParams.delete('category');
            if (source) url.searchParams.set('source', source); else url.searchParams.delete('source');
            url.searchParams.set('page', 1); // Reset to first page on new filter
            window.location.href = url.toString();
        }

        async function startCollection() {
            const city = document.getElementById('t-city').value;
            const cat = document.getElementById('t-cat').value;
            const source = document.getElementById('t-source').value;
            const btn = document.getElementById('start-btn');
            
            if(!city || !cat) return showNotif('Please enter location and search term', 2000);
            
            btn.disabled = true;
            btn.innerHTML = '<span class="pulse">COLLECTING...</span>';
            
            try {
                const res = await fetch('/api/trigger/scrape', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({city, category: cat, source})
                });
                const data = await res.json();
                showNotif(data.message);
            } catch (e) {
                showNotif('Failed to trigger collection');
                btn.disabled = false;
                btn.innerText = 'Start Collection';
            }
        }

        function setTemplate(city, cat, src) {
            document.getElementById('t-city').value = city;
            document.getElementById('t-cat').value = cat;
            document.getElementById('t-source').value = src;
            applyFilters();
        }

        async function triggerFast() {
            const res = await fetch('/api/trigger/fast-scrape', {method: 'POST'});
            const data = await res.json();
            showNotif(data.message);
        }

        async function cleanup() {
            showNotif('Cleaning started...');
            const res = await fetch('/api/cleanup/deep', {method: 'POST'});
            const data = await res.json();
            showNotif(`Done: ${data.deleted} deleted, ${data.updated} updated`);
        }

        async function updateQuality() {
            showNotif('Quality audit started...');
            const res = await fetch('/api/cleanup/quality', {method: 'POST'});
            const data = await res.json();
            showNotif(`Audited ${data.updated} records`);
        }

        function exportData(fmt) {
            window.location.href = `/export/${fmt}`;
        }

        // Live Telemetry Stream
        const evtSource = new EventSource("/api/stream/stats");
        evtSource.onmessage = function(event) {
            const data = JSON.parse(event.data);
            document.getElementById('stat-total').innerText = data.total;
            document.getElementById('stat-phone').innerText = data.with_phone;
            document.getElementById('stat-email').innerText = data.with_email;
            document.getElementById('last-update').innerText = new Date().toLocaleTimeString();
            
            const status = data.scraper_status;
            const statusEl = document.getElementById('live-status');
            const progWrap = document.getElementById('prog-wrap');
            const progBar = document.getElementById('prog-bar');

            const startBtn = document.getElementById('start-btn');

            if (status && status.running) {
                statusEl.innerText = status.message || 'RUNNING';
                statusEl.style.color = 'var(--accent-emerald)';
                progWrap.style.display = 'block';
                if(status.stats && status.stats.progress) {
                    progBar.style.width = status.stats.progress + '%';
                } else {
                    progBar.style.width = '100%';
                }
                
                if (startBtn) {
                    startBtn.disabled = true;
                    startBtn.innerHTML = '<span class="pulse">COLLECTING...</span>';
                }
            } else {
                statusEl.innerText = 'ONLINE'; // Online but not currently scraping
                statusEl.style.color = 'var(--text-secondary)';
                progWrap.style.display = 'none';
                
                if (startBtn) {
                    startBtn.disabled = false;
                    startBtn.innerText = 'Start Collection';
                }
            }

            // Stream Logs
            if (data.activity_logs) {
                const logContainer = document.getElementById('activity-logs');
                logContainer.innerHTML = data.activity_logs.map(log => `
                    <div class="log-entry">
                        <span class="log-time">${log.time}</span>
                        <span class="log-src">[${log.source}]</span>
                        <span class="log-msg ${log.level}">${log.message}</span>
                    </div>
                `).join('');
            }
            
            // Sidebar Telemetry
            const sidebarTime = document.getElementById('last-update-sidebar');
            if (sidebarTime) sidebarTime.innerText = new Date().toLocaleTimeString();
            
            const badge = document.getElementById('status-badge');
            if (badge) {
                if (status && status.running) {
                    badge.innerText = 'SCRAPING';
                    badge.style.color = 'var(--accent-emerald)';
                } else {
                    badge.innerText = 'ONLINE';
                    badge.style.color = 'var(--text-secondary)';
                }
            }
        };

        // Chart.js initialization
        let sourceChart, categoryChart, trendChart;
        
        async function initCharts() {
            const chartColors = ['#10b981', '#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899'];
            const baseOpt = { responsive: true, maintainAspectRatio: false, animation: false };

            sourceChart = new Chart(document.getElementById('sourceChart'), {
                type: 'doughnut',
                data: { labels: [], datasets: [{ data: [], backgroundColor: chartColors }] },
                options: { ...baseOpt, plugins: { legend: { position: 'bottom', labels: { color: '#64748b', font: { size: 8 }, padding: 4 } } }, cutout: '50%' }
            });

            categoryChart = new Chart(document.getElementById('categoryChart'), {
                type: 'bar',
                data: { labels: [], datasets: [{ data: [], backgroundColor: '#10b981' }] },
                options: { ...baseOpt, plugins: { legend: { display: false } }, scales: { x: { ticks: { color: '#64748b', font: { size: 7 } }, grid: { display: false } }, y: { ticks: { color: '#64748b', font: { size: 8 } }, grid: { color: 'rgba(255,255,255,0.05)' } } }
            });

            trendChart = new Chart(document.getElementById('trendChart'), {
                type: 'line',
                data: { labels: [], datasets: [{ data: [], borderColor: '#3b82f6', backgroundColor: 'rgba(59,130,246,0.1)', fill: true, tension: 0.3, pointRadius: 2 }] },
                options: { ...baseOpt, plugins: { legend: { display: false } }, scales: { x: { ticks: { color: '#64748b', font: { size: 8 } }, grid: { display: false } }, y: { ticks: { color: '#64748b', font: { size: 8 } }, grid: { color: 'rgba(255,255,255,0.05)' } } }
            });

            refreshCharts();
            setInterval(refreshCharts, 30000);
        }

        async function refreshCharts() {
            try {
                const stats = await fetch('/api/stats/charts').then(r => r.json());
                if (!stats.sources) return;
                sourceChart.data.labels = stats.sources.map(s => s.source);
                sourceChart.data.datasets[0].data = stats.sources.map(s => s.count);
                categoryChart.data.labels = stats.categories.slice(0,5).map(c => c.category);
                categoryChart.data.datasets[0].data = stats.categories.slice(0,5).map(c => c.count);
                trendChart.data.labels = stats.trend.map(t => t.date);
                trendChart.data.datasets[0].data = stats.trend.map(t => t.count);
                sourceChart.update(); categoryChart.update(); trendChart.update();
            } catch(e) { console.log('Chart error:', e); }
        }

        initCharts();
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
            params.append(selected_city)
        if selected_category:
            where_clauses.append(f"category {like_op} %s")
            params.append(selected_category)
        if selected_source:
            where_clauses.append(f"source {like_op} %s")
            params.append(selected_source)
        if selected_quality:
            where_clauses.append("(quality_tier = %s OR quality_tier IS NULL)")
            params.append(selected_quality)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        if USE_SQLITE:
            where_sql = where_sql.replace("%s", "?")

        # Get total count (unfiltered)
        cur.execute("SELECT COUNT(*) as cnt FROM contacts")
        total = cur.fetchone()["cnt"]

        # Get filtered count
        count_sql = f"SELECT COUNT(*) as cnt FROM contacts WHERE {where_sql}"
        cur.execute(count_sql, params)
        filtered_total = cur.fetchone()["cnt"]

        total_pages = (filtered_total + limit - 1) // limit if filtered_total > 0 else 1

        # Clamp page
        if page > total_pages:
            page = total_pages
        if page < 1:
            page = 1
        offset = (page - 1) * limit

        placeholder = "?" if USE_SQLITE else "%s"
        query_sql = f"SELECT id, name, phone, email, city, source, category, quality_tier, quality_score, scraped_at FROM contacts WHERE {where_sql} ORDER BY {order_by} LIMIT {placeholder} OFFSET {placeholder}"
        if USE_SQLITE:
            query_sql = query_sql.replace("%s", "?")
            
        cur.execute(query_sql, params + [limit, offset])
        contacts = cur.fetchall()

        placeholder = "?" if USE_SQLITE else "%s"
        # Get unique values for filter dropdowns (CACHED)
        cities = get_cached_filter(
            "cities",
            f"SELECT DISTINCT city FROM contacts WHERE city IS NOT NULL AND city <> {placeholder} ORDER BY city",
            cur
        )
        categories = get_cached_filter(
            "categories",
            f"SELECT DISTINCT category FROM contacts WHERE category IS NOT NULL AND category <> {placeholder} ORDER BY category",
            cur
        )
        sources = get_cached_filter(
            "sources",
            f"SELECT DISTINCT source FROM contacts WHERE source IS NOT NULL AND source <> {placeholder} ORDER BY source",
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
        set_status({"running": True, "message": "🧹 Deep cleaning database..."})
        
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
                set_status({"running": False, "message": "Idle"})
                return deleted, updated
            except Exception as e:
                set_status({"running": False, "message": "Idle"})
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
        return render_template_string(LOGS_HTML, logs=log_files[:20])
    except Exception as e:
        return f"Error reading logs: {e}"


@app.route("/logs/<name>")
def get_log(name):
    try:
        log_file = LOGS_DIR / name
        if log_file.exists():
            content = log_file.read_text()
            lines = content.split("\n")
            return jsonify({"name": name, "lines": lines[-500:]})
        return jsonify({"error": "Log not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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

        conn = get_db()
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM contacts WHERE {where_sql}", params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if fmt == "csv":
        import csv
        out = io.StringIO()
        if rows:
            w = csv.DictWriter(out, fieldnames=rows[0].keys())
            w.writeheader()
            for r in rows:
                w.writerow(dict(r))
        return Response(
            out.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment;filename=contacts.csv"},
        )
    if fmt == "json":
        return jsonify({"total": len(rows), "data": [dict(r) for r in rows]})
    if fmt == "excel":
        wb = Workbook()
        ws = wb.active
        ws.title = "Contacts"
        if rows:
            ws.append(list(rows[0].keys()))
            for r in rows:
                ws.append(list(r.values()))
        out = io.BytesIO()
        wb.save(out)
        out.seek(0)
        return send_file(out, download_name="contacts.xlsx", as_attachment=True)
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
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM contacts LIMIT 1000")
        contacts = cur.fetchall()
        if not contacts:
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
            except Exception:
                continue
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"success": True, "updated": updated})
    except Exception as e:
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
        cur.execute("SELECT category, COUNT(*) as count FROM contacts GROUP BY category ORDER BY count DESC LIMIT 10")
        categories = [dict(r) for r in cur.fetchall()]
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
