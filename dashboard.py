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
from datetime import datetime
from openpyxl import Workbook
from pathlib import Path
import sqlite3

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
        "database": "ready" if DB_INIT_READY else "pending",
        "timestamp": int(time.time()),
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
DB_INIT_READY = False
DB_INIT_IN_PROGRESS = False
DB_INIT_LAST_ATTEMPT = 0.0
DB_INIT_LAST_ERROR = None
DB_INIT_RETRY_SECONDS = int(os.environ.get("DATABASE_INIT_RETRY_SECONDS", "15"))
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


def _connect_db():
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
    conn = psycopg2.connect(
        url,
        cursor_factory=psycopg2.extras.RealDictCursor,
        connect_timeout=connect_timeout,
        application_name="dashboard",
    )
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
    return _connect_db()


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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id SERIAL PRIMARY KEY,
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
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            "scraped_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }

        for column_name, column_type in required_columns.items():
            try:
                cur.execute(
                    f"ALTER TABLE contacts ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                )
            except Exception as col_err:
                # Ignore column errors
                pass

        # Optimization: Only run heavy cleanup if the unique index is missing
        cur.execute("""
            SELECT count(*) FROM pg_indexes 
            WHERE indexname = 'idx_contacts_unique_phone'
        """)
        index_exists = cur.fetchone()['count'] > 0

        if not index_exists:
            logger.info("🧹 Deduplication Index missing. Running one-time cleanup...")
            
            # 1. Ensure phone_clean has a basic index to speed up the join
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tmp_phone_clean ON contacts(phone_clean)")
            
            # 2. Faster JOIN-based deduplication (keeps record with highest ID/latest)
            # This is significantly faster than NOT IN (SELECT MAX...)
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
            logger.info("✅ Cleanup completed.")

        # Constraints for Deduplication (UPSERT support)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_unique_phone ON contacts(phone_clean) WHERE phone_clean IS NOT NULL")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_unique_email ON contacts(email) WHERE email IS NOT NULL")
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_source ON contacts(source)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_category ON contacts(category)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_city ON contacts(city)")
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
    logger.info(f"💾 BOOTSTRAP: Managed mode (Railway {RAILWAY_SERVICE}). Awaiting first request for local state sync.")
elif not DB_INIT_READY:
    logger.info("💾 BOOTSTRAP: Local/Lazy mode (RAILWAY_SERVICE=Unknown).")


HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Financial Services Contact Scraper</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f1117; color: #e1e4e8; padding: 20px; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 24px 28px; border-radius: 14px; margin-bottom: 24px; display: flex; justify-content: space-between; align-items: center; }
        .header h1 { font-size: 22px; font-weight: 700; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 24px; }
        .stat { background: #1c1f2e; padding: 20px; border-radius: 12px; border: 1px solid #2d3148; }
        .stat h3 { color: #8b8fa3; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
        .stat .val { font-size: 32px; font-weight: 700; color: #fff; }
        .card { background: #1c1f2e; padding: 24px; border-radius: 12px; border: 1px solid #2d3148; margin-bottom: 24px; }
        .card h3 { color: #8b8fa3; font-size: 14px; margin-bottom: 12px; }
        .actions { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 24px; }
        .btn { padding: 10px 20px; color: white; border: none; border-radius: 8px; cursor: pointer; font-weight: 600; font-size: 14px; transition: all 0.2s; }
        .btn:hover { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(0,0,0,0.3); }
        .btn-export { background: #238636; }
        .btn-scrape { background: #f0883e; }
        .btn-scrape:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }
        table { width: 100%; border-collapse: collapse; background: #1c1f2e; border-radius: 12px; overflow: hidden; border: 1px solid #2d3148; }
        th, td { padding: 14px 16px; text-align: left; border-bottom: 1px solid #2d3148; font-size: 14px; }
        th { background: #161824; font-weight: 600; color: #8b8fa3; text-transform: uppercase; font-size: 11px; letter-spacing: 0.5px; }
        td { color: #c9d1d9; }
        .tag { padding: 4px 10px; border-radius: 20px; font-size: 11px; font-weight: 600; }
        .tag-source { background: rgba(56,139,253,0.15); color: #58a6ff; }
        .tag-cat { background: rgba(63,185,80,0.15); color: #3fb950; }
        .badge { padding: 3px 8px; border-radius: 4px; font-size: 12px; font-weight: 600; }
        .badge-valid { background: rgba(63,185,80,0.15); color: #3fb950; }
        .badge-invalid { background: rgba(248,81,73,0.15); color: #f85149; }
        .badge-empty { background: rgba(139,143,163,0.15); color: #8b8fa3; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 16px; margin-bottom: 24px; }
        .stat-card { background: #1c1f2e; padding: 16px; border-radius: 10px; border: 1px solid #2d3148; text-align: center; }
        .stat-card .value { font-size: 24px; font-weight: 700; color: #c9d1d9; }
        .stat-card .label { font-size: 12px; color: #8b8fa3; text-transform: uppercase; margin-top: 4px; }
        .modal-overlay { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.7); z-index: 1000; }
        .modal-overlay.active { display: flex; align-items: center; justify-content: center; }
        .modal { background: #1c1f2e; border-radius: 12px; padding: 24px; max-width: 600px; width: 90%; max-height: 80vh; overflow-y: auto; border: 1px solid #2d3148; }
        .modal-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
        .modal-header h2 { color: #c9d1d9; margin: 0; }
        .modal-close { background: none; border: none; color: #8b8fa3; font-size: 24px; cursor: pointer; }
        .detail-row { display: flex; padding: 12px 0; border-bottom: 1px solid #2d3148; }
        .detail-row:last-child { border-bottom: none; }
        .detail-label { width: 120px; color: #8b8fa3; font-size: 13px; }
        .detail-value { color: #c9d1d9; font-size: 14px; }
        .sort-select, .limit-select { padding: 8px 12px; background: #1c1f2e; border: 1px solid #2d3148; border-radius: 6px; color: #c9d1d9; font-size: 14px; }
        tr.clickable { cursor: pointer; }
        tr.clickable:hover { background: rgba(102,126,234,0.1); }
        .status-card { border: 2px solid #667eea; }
        .status-idle { color: #3fb950; }
        .status-running { color: #f0883e; }
        @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }
        @keyframes slideIn { from { transform: translateX(100%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
        .pulse { animation: pulse 1.5s ease-in-out infinite; }
        .empty { text-align: center; padding: 60px; color: #8b8fa3; }
        .empty h2 { font-size: 18px; margin-bottom: 8px; color: #c9d1d9; }
        .pagination { display: flex; justify-content: center; gap: 8px; margin: 30px 0; align-items: center; }
        .page-link { padding: 8px 16px; background: #1c1f2e; border: 1px solid #2d3148; border-radius: 8px; color: #8b8fa3; text-decoration: none; font-size: 14px; transition: all 0.2s; }
        .page-link:hover { background: #2d3148; color: #fff; }
        .page-link.active { background: #667eea; color: #fff; border-color: #667eea; }
        .page-link.disabled { opacity: 0.4; cursor: not-allowed; pointer-events: none; }
        .page-info { color: #8b8fa3; font-size: 14px; }
        .filters { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; align-items: center; }
        .filters label { font-size: 12px; color: #8b8fa3; text-transform: uppercase; letter-spacing: 0.5px; }
        .filters select { padding: 8px 12px; background: #1c1f2e; border: 1px solid #2d3148; border-radius: 6px; color: #c9d1d9; font-size: 14px; min-width: 140px; cursor: pointer; }
        .filters select:hover { border-color: #667eea; }
        .filters .btn-filter { padding: 8px 16px; background: #667eea; color: white; border: none; border-radius: 6px; font-size: 13px; cursor: pointer; }
        .filters .btn-filter:hover { background: #764ba2; }
        .filters .btn-clear { padding: 8px 16px; background: transparent; color: #8b8fa3; border: 1px solid #2d3148; border-radius: 6px; font-size: 13px; cursor: pointer; }
        .filters .btn-clear:hover { border-color: #ff7b72; color: #ff7b72; }
        .filter-stats { margin-left: auto; font-size: 13px; color: #8b8fa3; }
        .notification { position: fixed; bottom: 20px; right: 20px; padding: 12px 24px; background: #238636; color: white; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); z-index: 2000; animation: slideIn 0.3s ease-out; display: none; font-size: 14px; font-weight: 600; }
        .notification.error { background: #da3633; }
        .mode-badge { padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 700; text-transform: uppercase; margin-left: 12px; }
        .mode-local { background: #d29922; color: #000; }
        .mode-cloud { background: #238636; color: #fff; }
    </style>
</head>
<body>
    <div class="header">
        <div style="display:flex; align-items:center;">
            <h1>Financial Services Contact Scraper</h1>
            {% if use_sqlite %}
            <span class="mode-badge mode-local">Local Mode (SQLite)</span>
            {% else %}
            <span class="mode-badge mode-cloud">Cloud Mode (Postgres)</span>
            {% endif %}
        </div>
        <span style="font-size:13px; opacity:0.8;">{{s.total}} contacts collected</span>
    </div>

    <div class="stats">
        <div class="stat"><h3>Total Contacts</h3><div class="val">{{s.total}}</div></div>
        <div class="stat"><h3>Phone Numbers</h3><div class="val">{{s.phone}}</div></div>
        <div class="stat"><h3>Emails Found</h3><div class="val">{{s.email}}</div></div>
        <div class="stat"><h3>Cities Covered</h3><div class="val">{{s.cities}}</div></div>
        <div class="stat status-card">
            <h3>Live Scraper Status</h3>
            <div id="live-status" class="val status-idle" style="font-size:16px;">Idle</div>
        </div>
    </div>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="value">{{s.filtered_total}}</div>
            <div class="label">Filtered Results</div>
        </div>
        <div class="stat-card">
            <div class="value">{{s.quality_high}}</div>
            <div class="label" style="color:#3fb950;">High Quality</div>
        </div>
        <div class="stat-card">
            <div class="value">{{s.quality_medium}}</div>
            <div class="label" style="color:#d29922;">Medium Quality</div>
        </div>
        <div class="stat-card">
            <div class="value">{{s.quality_low}}</div>
            <div class="label" style="color:#f85149;">Low Quality</div>
        </div>
        <div class="stat-card">
            <div class="value">{{s.avg_quality}}</div>
            <div class="label">Avg Quality Score</div>
        </div>
    </div>

    <div class="card">
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:24px;">
            <div><h3>By Source</h3><canvas id="c1"></canvas></div>
            <div><h3>By Category</h3><canvas id="c2"></canvas></div>
        </div>
    </div>

    <div class="actions">
        <button class="btn btn-export" onclick="exportWithFilters('csv')">📥 Export CSV</button>
        <button class="btn btn-export" onclick="exportWithFilters('json')">📥 Export JSON</button>
        <button class="btn btn-scrape" id="scrape-btn" onclick="startScrape()">🚀 Start Scrape</button>
        <button class="btn" style="background:#238636;" onclick="startFastScrape()">⚡ Fast Scrape</button>
        <button class="btn" style="background:#8250df;" onclick="window.location.href='/logs'">📋 View Logs</button>
        <button class="btn" style="background:#da3633;" onclick="cleanupEmpty()">🗑️ Clean Empty</button>
        <button class="btn" style="background:#d29922;" onclick="updateQuality()">📊 Update Quality</button>
    </div>

    <div id="notification" class="notification"></div>

    <script>
        function showNotification(msg, isError = false) {
            const el = document.getElementById('notification');
            el.innerText = msg;
            el.style.display = 'block';
            if (isError) el.classList.add('error');
            else el.classList.remove('error');
            setTimeout(() => { el.style.display = 'none'; }, 4000);
        }
        // Update SSE logic to handle status and stats
        const source = new EventSource('/api/stream/stats');
        source.onmessage = function(event) {
            const data = JSON.parse(event.data);
            
            // Update quick stats
            const stats = document.querySelectorAll('.stat .val');
            if (stats.length >= 3) {
                stats[0].innerText = data.total;
                stats[1].innerText = data.with_phone;
                stats[2].innerText = data.with_email;
            }

            // Update status card
            const statusEl = document.getElementById('live-status');
            const statusData = data.scraper_status || {};
            
            if (statusData.running) {
                let msg = statusData.message || 'Scraping...';
                if (statusData.stats && statusData.stats.leads !== undefined) {
                    msg += `<br><span style="font-size:12px;opacity:0.8;color:#58a6ff;">✨ ${statusData.stats.leads} leads found on this page</span>`;
                }
                statusEl.innerHTML = msg;
                statusEl.className = 'val status-running pulse';
                document.getElementById('scrape-btn').disabled = true;
                document.getElementById('scrape-btn').innerText = '🚧 Scraping...';
            } else {
                statusEl.innerText = 'Idle';
                statusEl.className = 'val status-idle';
                document.getElementById('scrape-btn').disabled = false;
                document.getElementById('scrape-btn').innerText = '🚀 Start Scrape';
            }
        };

        function startFastScrape(){
            const btn = document.getElementById('scrape-btn');
            const fastBtn = document.querySelector('button[onclick="startFastScrape()"]');
            fastBtn.disabled = true;
            fastBtn.innerText = '⏳ Running...';
            
            fetch('/api/trigger/fast-scrape', {method: 'POST'}).then(r=>r.json()).then(d=>{
                showNotification(d.message || d.error);
                fastBtn.innerText = '⚡ Fast Scrape';
                fastBtn.disabled = false;
            }).catch(()=>{ 
                fastBtn.innerText = '⚡ Fast Scrape';
                fastBtn.disabled = false;
            });
        }

        function cleanupEmpty(){
            if(confirm('Delete all contacts with no phone AND no email? This cannot be undone.')){
                fetch('/api/cleanup/empty', {method: 'DELETE'}).then(r=>r.json()).then(d=>{
                    showNotification(d.message || d.error);
                    if(d.success) setTimeout(() => location.reload(), 2000);
                });
            }
        }

        function updateQuality(){
            fetch('/api/cleanup/quality', {method: 'POST'}).then(r=>r.json()).then(d=>{
                showNotification(d.message || d.error);
                if(d.success) setTimeout(() => location.reload(), 2000);
            });
        }
    </script>

    <div class="filters">
        <div>
            <label>Search</label><br>
            <input type="text" id="filter-search" placeholder="Name, phone, email..." value="{{search_query}}" style="padding:8px 12px;background:#1c1f2e;border:1px solid #2d3148;border-radius:6px;color:#c9d1d9;font-size:14px;min-width:180px;">
        </div>
        <div>
            <label>Sort By</label><br>
            <select id="sort-by" class="sort-select" onchange="applyFilters()">
                <option value="date" {% if sort_by=='date' %}selected{% endif %}>Date Scraped</option>
                <option value="name" {% if sort_by=='name' %}selected{% endif %}>Name</option>
                <option value="city" {% if sort_by=='city' %}selected{% endif %}>City</option>
                <option value="source" {% if sort_by=='source' %}selected{% endif %}>Source</option>
            </select>
        </div>
        <div>
            <label>City</label><br>
            <select id="filter-city" onchange="applyFilters()">
                <option value="">All Cities</option>
                {% for c in cities %}<option value="{{c}}" {% if selected_city==c %}selected{% endif %}>{{c}}</option>{% endfor %}
            </select>
        </div>
        <div>
            <label>Category</label><br>
            <select id="filter-category" onchange="applyFilters()">
                <option value="">All Categories</option>
                {% for cat in categories %}<option value="{{cat}}" {% if selected_category==cat %}selected{% endif %}>{{cat}}</option>{% endfor %}
            </select>
        </div>
        <div>
            <label>Source</label><br>
            <select id="filter-source" onchange="applyFilters()">
                <option value="">All Sources</option>
                {% for src in sources %}<option value="{{src}}" {% if selected_source==src %}selected{% endif %}>{{src}}</option>{% endfor %}
            </select>
        </div>
        <div>
            <label>Per Page</label><br>
            <select id="limit" class="limit-select" onchange="applyFilters()">
                <option value="25" {% if limit==25 %}selected{% endif %}>25</option>
                <option value="50" {% if limit==50 %}selected{% endif %}>50</option>
                <option value="100" {% if limit==100 %}selected{% endif %}>100</option>
            </select>
        </div>
        <div>
            <label>Quality</label><br>
            <select id="filter-quality" onchange="applyFilters()">
                <option value="">All Quality</option>
                <option value="high" {% if selected_quality=='high' %}selected{% endif %}>High</option>
                <option value="medium" {% if selected_quality=='medium' %}selected{% endif %}>Medium</option>
                <option value="low" {% if selected_quality=='low' %}selected{% endif %}>Low</option>
            </select>
        </div>
        <div style="display:flex;gap:8px;align-items:flex-end;">
            <button class="btn btn-filter" onclick="applyFilters()">Apply</button>
            <button class="btn btn-clear" onclick="clearFilters()">Clear</button>
        </div>
        <div class="filter-stats">Showing {{contacts|length}} of {{s.filtered_total}} results</div>
    </div>

    {% if contacts %}
    <table>
        <thead><tr><th>Name</th><th>Phone</th><th>Email</th><th>City</th><th>Source</th><th>Category</th></tr></thead>
        <tbody>{% for c in contacts %}<tr class="clickable" onclick="showContactDetail({{c.id}})">
            <td>{{c.name or '-'}}</td>
            <td>{{c.phone or '-'}}</td>
            <td>{{c.email or '-'}}</td>
            <td>{{c.city or '-'}}</td>
            <td><span class="tag tag-source">{{c.source or '-'}}</span></td>
            <td><span class="tag tag-cat">{{c.category or '-'}}</span></td>
        </tr>{% endfor %}</tbody>
    </table>
    {% else %}
    <div class="empty">
        {% if search_query or selected_city or selected_category or selected_source %}
        <h2>No matching contacts found</h2>
        <p>Try adjusting your filters or search query.</p>
        {% else %}
        <h2>No contacts yet</h2>
        <p>Click "Start Scrape" to begin collecting leads from your configured sources.</p>
        {% endif %}
    </div>
    {% endif %}

    {% if total_pages > 1 %}
    <div class="pagination">
        <a href="/?page=1{% if search_query %}&q={{search_query}}{% endif %}{% if selected_city %}&city={{selected_city}}{% endif %}{% if selected_category %}&category={{selected_category}}{% endif %}{% if selected_source %}&source={{selected_source}}{% endif %}{% if selected_quality %}&quality={{selected_quality}}{% endif %}{% if sort_by %}&sort={{sort_by}}{% endif %}{% if limit %}&limit={{limit}}{% endif %}" class="page-link {% if page == 1 %}disabled{% endif %}">« First</a>
        <a href="/?page={{ page - 1 }}{% if search_query %}&q={{search_query}}{% endif %}{% if selected_city %}&city={{selected_city}}{% endif %}{% if selected_category %}&category={{selected_category}}{% endif %}{% if selected_source %}&source={{selected_source}}{% endif %}{% if selected_quality %}&quality={{selected_quality}}{% endif %}{% if sort_by %}&sort={{sort_by}}{% endif %}{% if limit %}&limit={{limit}}{% endif %}" class="page-link {% if page == 1 %}disabled{% endif %}">‹ Prev</a>
        
        <span class="page-info">Page <b>{{ page }}</b> of <b>{{ total_pages }}</b></span>

        <a href="/?page={{ page + 1 }}{% if search_query %}&q={{search_query}}{% endif %}{% if selected_city %}&city={{selected_city}}{% endif %}{% if selected_category %}&category={{selected_category}}{% endif %}{% if selected_source %}&source={{selected_source}}{% endif %}{% if selected_quality %}&quality={{selected_quality}}{% endif %}{% if sort_by %}&sort={{sort_by}}{% endif %}{% if limit %}&limit={{limit}}{% endif %}" class="page-link {% if page == total_pages %}disabled{% endif %}">Next ›</a>
        <a href="/?page={{ total_pages }}{% if search_query %}&q={{search_query}}{% endif %}{% if selected_city %}&city={{selected_city}}{% endif %}{% if selected_category %}&category={{selected_category}}{% endif %}{% if selected_source %}&source={{selected_source}}{% endif %}{% if selected_quality %}&quality={{selected_quality}}{% endif %}{% if sort_by %}&sort={{sort_by}}{% endif %}{% if limit %}&limit={{limit}}{% endif %}" class="page-link {% if page == total_pages %}disabled{% endif %}">Last »</a>
    </div>
    {% endif %}

    <div class="modal-overlay" id="modal" onclick="closeModal(event)">
        <div class="modal" onclick="event.stopPropagation()">
            <div class="modal-header">
                <h2>Contact Details</h2>
                <button class="modal-close" onclick="closeModal()">&times;</button>
            </div>
            <div id="modal-content"></div>
        </div>
    </div>

    <script>
        Chart.defaults.color = '#8b8fa3';
        Chart.defaults.borderColor = '#2d3148';
        const srcData = {{by_source|tojson}};
        const catData = {{by_cat|tojson}};
        const colors = ['#667eea','#764ba2','#3fb950','#f0883e','#58a6ff','#d2a8ff','#ff7b72'];

        if (Object.keys(srcData).length > 0) {
            new Chart(document.getElementById('c1'),{type:'doughnut',data:{labels:Object.keys(srcData),datasets:[{data:Object.values(srcData),backgroundColor:colors}]},options:{plugins:{legend:{labels:{color:'#c9d1d9'}}}}});
        }
        if (Object.keys(catData).length > 0) {
            new Chart(document.getElementById('c2'),{type:'bar',data:{labels:Object.keys(catData),datasets:[{data:Object.values(catData),backgroundColor:'#667eea',borderRadius:6}]},options:{plugins:{legend:{display:false}},scales:{y:{ticks:{color:'#8b8fa3'}},x:{ticks:{color:'#8b8fa3'}}}}});
        }

        function startScrape(){
            const btn = document.getElementById('scrape-btn');
            btn.disabled = true;
            btn.innerText = '🚧 Starting...';
            fetch('/api/trigger/scrape').then(r=>r.json()).then(d=>{
                showNotification(d.message || d.error);
                btn.innerText = '🚧 Scraping...';
            }).catch(()=>{ btn.disabled=false; btn.innerText='🚀 Start Scrape'; });
        }

        function applyFilters(){
            const search = document.getElementById('filter-search').value;
            const city = document.getElementById('filter-city').value;
            const category = document.getElementById('filter-category').value;
            const source = document.getElementById('filter-source').value;
            const sortBy = document.getElementById('sort-by').value;
            const limit = document.getElementById('limit').value;
            
            let params = new URLSearchParams();
            if(search) params.set('q', search);
            if(city) params.set('city', city);
            if(category) params.set('category', category);
            if(source) params.set('source', source);
            if(sortBy) params.set('sort', sortBy);
            if(limit && limit != 50) params.set('limit', limit);
            const quality = document.getElementById('filter-quality').value;
            if(quality) params.set('quality', quality);
            
            const url = params.toString() ? '?' + params.toString() : '/';
            window.location.href = url;
        }

        function clearFilters(){
            window.location.href = '/';
        }

        function showContactDetail(id){
            fetch('/api/contact/' + id).then(r=>r.json()).then(data=>{
                if(data.error){ showNotification(data.error, true); return; }
                const c = data;
                document.getElementById('modal-content').innerHTML = `
                    <div class="detail-row"><span class="detail-label">Name</span><span class="detail-value">${c.name || '-'}</span></div>
                    <div class="detail-row"><span class="detail-label">Phone</span><span class="detail-value">${c.phone || '-'}</span></div>
                    <div class="detail-row"><span class="detail-label">Email</span><span class="detail-value">${c.email || '-'}</span></div>
                    <div class="detail-row"><span class="detail-label">City</span><span class="detail-value">${c.city || '-'}</span></div>
                    <div class="detail-row"><span class="detail-label">Area</span><span class="detail-value">${c.area || '-'}</span></div>
                    <div class="detail-row"><span class="detail-label">Address</span><span class="detail-value">${c.address || '-'}</span></div>
                    <div class="detail-row"><span class="detail-label">Category</span><span class="detail-value">${c.category || '-'}</span></div>
                    <div class="detail-row"><span class="detail-label">Source</span><span class="detail-value">${c.source || '-'}</span></div>
                    <div class="detail-row"><span class="detail-label">ARN/License</span><span class="detail-value">${c.arn || c.license_no || c.membership_no || '-'}</span></div>
                    <div class="detail-row"><span class="detail-label">Scraped At</span><span class="detail-value">${c.scraped_at || '-'}</span></div>
                `;
                document.getElementById('modal').classList.add('active');
            });
        }

        function closeModal(e){
            if(!e || e.target.id === 'modal'){
                document.getElementById('modal').classList.remove('active');
            }
        }

        function exportWithFilters(fmt){
            const search = document.getElementById('filter-search').value;
            const city = document.getElementById('filter-city').value;
            const category = document.getElementById('filter-category').value;
            const source = document.getElementById('filter-source').value;
            
            let params = new URLSearchParams();
            if(search) params.set('q', search);
            if(city) params.set('city', city);
            if(category) params.set('category', category);
            if(source) params.set('source', source);
            
            const url = '/export/' + fmt + (params.toString() ? '?' + params.toString() : '');
            window.location.href = url;
        }

        let wasRunning = false;
        function pollStatus() {
            fetch('/api/status').then(r=>r.json()).then(data=>{
                const el = document.getElementById('live-status');
                const btn = document.getElementById('scrape-btn');
                
                if (data.running) {
                    let msg = data.message || 'Scraping...';
                    if (data.stats && data.stats.leads !== undefined) {
                        msg += `<br><span style="font-size:12px;opacity:0.8;color:#58a6ff;">✨ ${data.stats.leads} leads found on this page</span>`;
                    }
                    el.innerHTML = msg;
                    el.className = 'val status-running pulse';
                    btn.disabled = true;
                    btn.innerText = '🚧 Scraping...';
                    wasRunning = true;
                } else {
                    el.innerText = 'Idle';
                    el.className = 'val status-idle';
                    btn.disabled = false;
                    btn.innerText = '🚀 Start Scrape';
                    if (wasRunning) { wasRunning = false; location.reload(); }
                }
            }).catch(()=>{});
        }
        
        // Live stats via Server-Sent Events
        let lastTotal = {{s.total}};
        try {
            const evtSource = new EventSource('/api/stream/stats');
            evtSource.onmessage = function(e) {
                const data = JSON.parse(e.data);
                
                // Update stat cards if values changed
                const totalEl = document.querySelector('.stat:nth-child(1) .val');
                const phoneEl = document.querySelector('.stat:nth-child(2) .val');
                const emailEl = document.querySelector('.stat:nth-child(3) .val');
                
                if(totalEl) {
                    const newTotal = parseInt(data.total);
                    if(newTotal !== lastTotal) {
                        // Animate change
                        totalEl.style.color = '#3fb950';
                        totalEl.style.transform = 'scale(1.2)';
                        setTimeout(() => {
                            totalEl.style.color = '';
                            totalEl.style.transform = '';
                        }, 500);
                        
                        // Show notification
                        const diff = newTotal - lastTotal;
                        if(diff > 0) showNotification(`+${diff} new contacts!`);
                        lastTotal = newTotal;
                    }
                    totalEl.textContent = data.total;
                }
                if(phoneEl) phoneEl.textContent = data.with_phone;
                if(emailEl) emailEl.textContent = data.with_email;
                
                // Update header count
                document.querySelector('.header span').textContent = data.total + ' contacts collected';
            };
            evtSource.onerror = function() {
                evtSource.close();
                // Fallback to polling
                setInterval(pollStats, 10000);
            };
        } catch(e) {
            console.log('SSE not supported, using polling');
        }
        
        function pollStats() {
            fetch('/api/stats').then(r=>r.json()).then(data=>{
                if(data.total !== lastTotal) {
                    lastTotal = data.total;
                    showNotification('New data available! <a href="/">Refresh</a>');
                }
            }).catch(()=>{});
        }
        
        function showNotification(msg) {
            const existing = document.getElementById('live-notification');
            if(existing) existing.remove();
            
            const notif = document.createElement('div');
            notif.id = 'live-notification';
            notif.innerHTML = msg;
            notif.style.cssText = 'position:fixed;top:20px;right:20px;background:#238636;color:white;padding:12px 20px;border-radius:8px;z-index:9999;animation:slideIn 0.3s ease';
            document.body.appendChild(notif);
            setTimeout(() => notif.remove(), 5000);
        }
        
        setInterval(pollStatus, 3000);
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

        # Build WHERE clause for filters (case-insensitive)
        where_clauses = []
        params = []
        if search_query:
            where_clauses.append("(name ILIKE %s OR phone ILIKE %s OR email ILIKE %s)")
            search_pattern = f"%{search_query}%"
            params.extend([search_pattern, search_pattern, search_pattern])
        if selected_city:
            where_clauses.append("city ILIKE %s")
            params.append(selected_city)
        if selected_category:
            where_clauses.append("category ILIKE %s")
            params.append(selected_category)
        if selected_source:
            where_clauses.append("source ILIKE %s")
            params.append(selected_source)
        if selected_quality:
            where_clauses.append("(quality_tier = %s OR quality_tier IS NULL)")
            params.append(selected_quality)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Get total count (unfiltered)
        cur.execute("SELECT COUNT(*) as cnt FROM contacts")
        total = cur.fetchone()["cnt"]

        # Get filtered count
        cur.execute(f"SELECT COUNT(*) as cnt FROM contacts WHERE {where_sql}", params)
        filtered_total = cur.fetchone()["cnt"]

        total_pages = (filtered_total + limit - 1) // limit if filtered_total > 0 else 1

        # Clamp page
        if page > total_pages:
            page = total_pages
        if page < 1:
            page = 1
        offset = (page - 1) * limit

        cur.execute(
            f"SELECT id, name, phone, email, city, source, category, quality_tier, quality_score, scraped_at FROM contacts WHERE {where_sql} ORDER BY {order_by} LIMIT %s OFFSET %s",
            params + [limit, offset],
        )
        contacts = cur.fetchall()

        # Get unique values for filter dropdowns (CACHED)
        cities = get_cached_filter(
            "cities",
            "SELECT DISTINCT city FROM contacts WHERE city IS NOT NULL AND city <> %s ORDER BY city",
            cur
        )
        categories = get_cached_filter(
            "categories",
            "SELECT DISTINCT category FROM contacts WHERE category IS NOT NULL AND category <> %s ORDER BY category",
            cur
        )
        sources = get_cached_filter(
            "sources",
            "SELECT DISTINCT source FROM contacts WHERE source IS NOT NULL AND source <> %s ORDER BY source",
            cur
        )

        # Optimized Stats: Combine all 7+ counts into a single efficient database pass
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE phone_clean IS NOT NULL AND phone_clean <> '') as with_phone,
                COUNT(*) FILTER (WHERE email IS NOT NULL AND email <> '') as with_email,
                COUNT(DISTINCT city) as city_count,
                COUNT(*) FILTER (WHERE quality_tier = 'high') as q_high,
                COUNT(*) FILTER (WHERE quality_tier = 'medium') as q_medium,
                COUNT(*) FILTER (WHERE quality_tier = 'low') as q_low,
                AVG(quality_score) as avg_score
            FROM contacts
        """)
        stats_row = cur.fetchone()
        with_phone = stats_row["with_phone"]
        with_email = stats_row["with_email"]
        city_count = stats_row["city_count"]
        quality_high = stats_row["q_high"]
        quality_medium = stats_row["q_medium"]
        quality_low = stats_row["q_low"]
        avg_quality = round(stats_row["avg_score"] or 0, 1)

        cur.execute("SELECT source, COUNT(*) as c FROM contacts GROUP BY source")
        by_source = {r["source"]: r["c"] for r in cur.fetchall()}
        cur.execute("SELECT category, COUNT(*) as c FROM contacts GROUP BY category")
        by_cat = {r["category"]: r["c"] for r in cur.fetchall()}
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Database error: {e}")
        contacts, total, filtered_total, with_phone, with_email, city_count = (
            [],
            0,
            0,
            0,
            0,
            0,
        )
        by_source, by_cat, total_pages, page = {}, {}, 1, 1
        cities, categories, sources = [], [], []
        selected_city = selected_category = selected_source = ""
        selected_quality = ""
        search_query = ""
        sort_by = "date"
        limit = page_size
        quality_high = quality_medium = quality_low = 0
        avg_quality = 0

    return render_template_string(
        HTML,
        contacts=contacts,
        s={
            "total": total,
            "phone": with_phone,
            "email": with_email,
            "cities": city_count,
            "filtered_total": filtered_total,
            "quality_high": quality_high,
            "quality_medium": quality_medium,
            "quality_low": quality_low,
            "avg_quality": avg_quality,
        },
        by_source=by_source,
        by_cat=by_cat,
        page=page,
        total_pages=total_pages,
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
    if not redis_client:
        return jsonify({"message": "Idle", "running": False})
    try:
        status = redis_client.get("scraper_status")
        if status:
            return Response(status, mimetype="application/json")
    except Exception:
        pass
    return jsonify({"message": "Idle", "running": False})


@app.route("/api/contact/<int:contact_id>")
def get_contact(contact_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM contacts WHERE id = %s", (contact_id,))
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


@app.route("/api/trigger/scrape")
def trigger_scrape():
    """Trigger scraping tasks. Default: official sources only (AMFI, IRDAI, ICAI)."""
    from tasks import scrape_all_task

    use_business = request.args.get("business", "false").lower() == "true"

    config = load_config()
    cities = config.get("cities", [])
    categories = config.get("categories", [])
    pair_count = len(cities) * len(categories)
    scrape_all_task.delay(source=None, use_business=use_business)

    source_type = (
        "Business Directories"
        if use_business
        else "Official Sources (AMFI, IRDAI, ICAI)"
    )
    return jsonify(
        {
            "message": f"🚀 Batch scrape queued for {source_type} across {pair_count} city/category combinations!",
            "tasks": 1,
            "pairs": pair_count,
            "source_type": source_type,
            "use_business": use_business,
        }
    )


@app.route("/api/trigger/fast-scrape", methods=["POST"])
def trigger_fast_scrape():
    """Trigger fast parallel scraping with higher concurrency"""
    from tasks import fast_scrape_task
    from tasks import _load_runtime_config

    config = _load_runtime_config()
    cities = config.get("cities", [])
    categories = config.get("categories", [])
    pair_count = len(cities) * len(categories)

    max_concurrent = request.args.get("concurrency", 3, type=int)

    fast_scrape_task.delay(
        source=None, use_business=False, max_concurrent=max_concurrent
    )

    return jsonify(
        {
            "message": f"⚡ Fast scrape queued! {pair_count} jobs with concurrency={max_concurrent}",
            "type": "fast_parallel",
            "jobs": pair_count,
            "concurrency": max_concurrent,
        }
    )


@app.route("/api/contacts")
def api_contacts():
    try:
        conn = get_db()
        cur = conn.cursor()

        page = request.args.get("page", 1, type=int)
        limit = min(request.args.get("limit", 100, type=int), 1000)
        offset = (page - 1) * limit

        # Filter params
        search_query = request.args.get("q", "")
        filter_city = request.args.get("city", "")
        filter_category = request.args.get("category", "")
        filter_source = request.args.get("source", "")

        where_clauses = []
        params = []
        if search_query:
            where_clauses.append("(name ILIKE %s OR phone ILIKE %s OR email ILIKE %s)")
            search_pattern = f"%{search_query}%"
            params.extend([search_pattern, search_pattern, search_pattern])
        if filter_city:
            where_clauses.append("city ILIKE %s")
            params.append(filter_city)
        if filter_category:
            where_clauses.append("category ILIKE %s")
            params.append(filter_category)
        if filter_source:
            where_clauses.append("source ILIKE %s")
            params.append(filter_source)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        # SQLite vs Postgres compatibility
        if USE_SQLITE:
            where_sql = where_sql.replace("ILIKE", "LIKE")
            query = f"SELECT name, phone, email, city, category, source FROM contacts WHERE {where_sql} ORDER BY scraped_at DESC LIMIT ? OFFSET ?"
            query = query.replace("%s", "?")
            count_query = f"SELECT COUNT(*) as cnt FROM contacts WHERE {where_sql}".replace("%s", "?")
        else:
            query = f"SELECT name, phone, email, city, category, source FROM contacts WHERE {where_sql} ORDER BY scraped_at DESC LIMIT %s OFFSET %s"
            count_query = f"SELECT COUNT(*) as cnt FROM contacts WHERE {where_sql}"

        cur.execute(query, params + [limit, offset])
        contacts = cur.fetchall()
        cur.execute(count_query, params)
        total = cur.fetchone()["cnt"] if USE_SQLITE else cur.fetchone()["cnt"]
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

        where_clauses = []
        params = []
        if search_query:
            where_clauses.append("(name ILIKE %s OR phone ILIKE %s OR email ILIKE %s)")
            search_pattern = f"%{search_query}%"
            params.extend([search_pattern, search_pattern, search_pattern])
        if filter_city:
            where_clauses.append("city ILIKE %s")
            params.append(filter_city)
        if filter_category:
            where_clauses.append("category ILIKE %s")
            params.append(filter_category)
        if filter_source:
            where_clauses.append("source ILIKE %s")
            params.append(filter_source)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

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

        # Delete contacts with no phone AND no email
        cur.execute("""
            DELETE FROM contacts 
            WHERE (phone IS NULL OR TRIM(phone) = '') 
            AND (email IS NULL OR TRIM(email) = '')
        """)
        deleted_count = cur.rowcount

        conn.commit()

        # Get remaining count
        cur.execute("SELECT COUNT(*) as cnt FROM contacts")
        remaining = cur.fetchone()["cnt"]

        cur.close()
        conn.close()

        logger.info(
            f"Cleaned up {deleted_count} empty contacts. Remaining: {remaining}"
        )
        return jsonify(
            {
                "success": True,
                "deleted": deleted_count,
                "remaining": remaining,
                "message": f"Deleted {deleted_count} contacts with no phone or email",
            }
        )
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/cleanup/quality", methods=["POST"])
def cleanup_low_quality():
    """Recalculate and update quality scores for all contacts"""
    try:
        from processing import ProcessingHandler

        conn = get_db()
        cur = conn.cursor()

        # Get all contacts (batch processing)
        cur.execute("SELECT * FROM contacts LIMIT 1000")
        contacts = cur.fetchall()

        if not contacts:
            return jsonify(
                {"success": True, "updated": 0, "message": "No contacts to update"}
            )

        updated = 0
        for contact in contacts:
            try:
                # Process through quality handler
                processed = ProcessingHandler.process_contact(dict(contact))

                # Update quality fields
                cur.execute(
                    """
                    UPDATE contacts 
                    SET phone_clean = %s, 
                        email_valid = %s, 
                        quality_score = %s, 
                        quality_tier = %s
                    WHERE id = %s
                """,
                    (
                        processed.get("phone_clean"),
                        processed.get("email_valid", False),
                        processed.get("quality_score", 0),
                        processed.get("quality_tier", "low"),
                        contact["id"],
                    ),
                )
                updated += 1
            except Exception as e:
                logger.warning(f"Failed to update contact {contact.get('id')}: {e}")
                continue

        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"Updated quality scores for {updated} contacts")
        return jsonify(
            {
                "success": True,
                "updated": updated,
                "message": f"Updated quality scores for {updated} contacts",
            }
        )
    except Exception as e:
        logger.error(f"Quality update failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/stream/stats")
def stream_stats():
    """Server-Sent Events endpoint for live stats updates"""
    import time

    def generate():
        last_total = 0
        last_phone = 0
        last_email = 0
        while True:
            try:
                conn = get_db()
                cur = conn.cursor()

                cur.execute("SELECT COUNT(*) as cnt FROM contacts")
                total = cur.fetchone()["cnt"]

                cur.execute(
                    "SELECT COUNT(*) as cnt FROM contacts WHERE phone IS NOT NULL AND phone <> %s",
                    ("",),
                )
                with_phone = cur.fetchone()["cnt"]

                cur.execute(
                    "SELECT COUNT(*) as cnt FROM contacts WHERE email IS NOT NULL AND email <> %s",
                    ("",),
                )
                with_email = cur.fetchone()["cnt"]

                cur.close()
                conn.close()

                # Get scraper status from Redis
                status_data = {}
                if redis_client:
                    raw_status = redis_client.get("scraper_status")
                    if raw_status:
                        status_data = json.loads(raw_status)

                data = {
                    "total": total,
                    "with_phone": with_phone,
                    "with_email": with_email,
                    "timestamp": int(time.time()),
                    "scraper_status": status_data
                }

                # Send if data changed OR if scraper is running (to update progress)
                is_running = status_data.get("running", False)
                if (
                    total != last_total
                    or with_phone != last_phone
                    or with_email != last_email
                    or is_running
                ):
                    yield f"data: {json.dumps(data)}\n\n"
                    last_total = total
                    last_phone = with_phone
                    last_email = with_email
            except Exception as e:
                logger.error(f"SSE Error: {e}")
                yield f"data: {json.dumps({'error': str(e), 'running': False})}\n\n"
            
            time.sleep(2)
            
            time.sleep(2)  # Check every 2 seconds

    return Response(generate(), mimetype="text/event-stream")


@app.route("/api/stats")
def get_stats():
    """Get current stats for polling fallback"""
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) as cnt FROM contacts")
        total = cur.fetchone()["cnt"]

        cur.execute(
            "SELECT COUNT(*) as cnt FROM contacts WHERE phone IS NOT NULL AND phone <> %s",
            ("",),
        )
        with_phone = cur.fetchone()["cnt"]

        cur.execute(
            "SELECT COUNT(*) as cnt FROM contacts WHERE email IS NOT NULL AND email <> %s",
            ("",),
        )
        with_email = cur.fetchone()["cnt"]

        cur.close()
        conn.close()

        return jsonify(
            {"total": total, "with_phone": with_phone, "with_email": with_email}
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
