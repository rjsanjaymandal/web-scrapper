from flask import Flask, render_template, request, jsonify, Response, send_file
import psycopg2
import psycopg2.extras
import yaml
import io
import re
import os
import logging
import json
from datetime import datetime
from openpyxl import Workbook
from pathlib import Path
import redis

# Redis for Real-time Stats & Caching
r = redis.from_url(os.environ.get('REDIS_URL', 'redis://localhost:6379/0'))

class DashboardStats:
    """Calculates and caches premium dashboard metrics."""
    
    @staticmethod
    def get_velocity() -> float:
        """Calculate Leads Per Minute (LPM) from the last 5 minutes."""
        try:
            keys = r.keys("lead_count:*")
            return len(keys) / 5.0 if keys else 0.0
        except: return 0.0

    @staticmethod
    def get_cached_stats(cur):
        """Get or calculate stats with short-term caching."""
        cache_key = "dashboard:stats"
        cached = r.get(cache_key)
        if cached:
            return json.loads(cached)
            
        # 1. Total Leads
        cur.execute('SELECT COUNT(*) as cnt FROM contacts')
        total = cur.fetchone()['cnt']
        
        # 2. Quality Breakdown
        cur.execute('SELECT quality_tier, COUNT(*) as cnt FROM contacts GROUP BY quality_tier')
        quality = {row['quality_tier']: row['cnt'] for row in cur.fetchall()}
        
        # 3. Source Breakdown (Heatmap)
        cur.execute('SELECT source, AVG(quality_score) as avg_score, COUNT(*) as cnt FROM contacts GROUP BY source')
        source_heatmap = {}
        for row in cur.fetchall():
            src_name = row['source'] if row['source'] else 'Unknown'
            # Robust None-guard for avg_score
            avg_score = float(row['avg_score']) if row['avg_score'] is not None else 0.0
            source_heatmap[src_name] = {'score': avg_score, 'count': row['cnt']}
        
        stats = {
            'total': total,
            'quality': quality,
            'heatmap': source_heatmap,
            'velocity': DashboardStats.get_velocity(),
            'updated_at': datetime.now().strftime('%H:%M:%S')
        }
        
        r.setex(cache_key, 60, json.dumps(stats)) # Cache for 60s
        return stats

app = Flask(__name__)

PROJ_DIR = Path(__file__).parent
LOGS_DIR = PROJ_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / 'dashboard.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Redis for live status (optional)
try:
    import redis
    REDIS_URL = os.environ.get('REDIS_URL')
    redis_client = redis.Redis.from_url(REDIS_URL) if REDIS_URL else None
except Exception:
    redis_client = None


def get_db_url():
    """Build the database URL from environment variables."""
    db_url = os.environ.get('DATABASE_URL')
    if db_url:
        return db_url
    config = load_config()
    db_cfg = config.get('database', {}) if isinstance(config, dict) else {}
    host = os.environ.get('DATABASE_HOST', db_cfg.get('host', 'localhost'))
    port = os.environ.get('DATABASE_PORT', db_cfg.get('port', 5432))
    name = os.environ.get('DATABASE_NAME', db_cfg.get('name', 'scraper_db'))
    user = os.environ.get('DATABASE_USER', db_cfg.get('user', 'postgres'))
    pw = os.environ.get('DATABASE_PASSWORD', db_cfg.get('password', ''))
    return f"postgresql://{user}:{pw}@{host}:{port}/{name}"


def get_db():
    """Get a fresh database connection. Simple, reliable, no pool issues."""
    url = get_db_url()
    # Railway may use postgres:// — psycopg2 needs postgresql://
    if url and url.startswith('postgres://'):
        url = url.replace('postgres://', 'postgresql://', 1)
    conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = True
    return conn


def init_tables():
    """Create tables if they don't exist."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
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
                phone_clean VARCHAR(50) UNIQUE,
                email_valid BOOLEAN,
                enriched BOOLEAN,
                arn VARCHAR(50),
                license_no VARCHAR(100),
                membership_no VARCHAR(100),
                quality_score INTEGER DEFAULT 0,
                quality_tier VARCHAR(20) DEFAULT 'low',
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Individual column checks for existing tables
        required_columns = {
            'phone_clean': 'VARCHAR(50) UNIQUE',
            'quality_score': 'INTEGER DEFAULT 0',
            'quality_tier': 'VARCHAR(20) DEFAULT \'low\'',
            'enriched': 'BOOLEAN DEFAULT FALSE',
            'email_valid': 'BOOLEAN DEFAULT FALSE'
        }

        for column_name, column_type in required_columns.items():
            try:
                # Add columns one by one
                cur.execute(f'ALTER TABLE contacts ADD COLUMN IF NOT EXISTS {column_name} {column_type.replace(" UNIQUE", "")}')
                
                # If it's phone_clean, try adding the unique constraint separately
                if column_name == 'phone_clean':
                    try:
                        # Check for duplicates first to avoid hard crash
                        cur.execute("SELECT phone_clean, COUNT(*) FROM contacts WHERE phone_clean IS NOT NULL AND phone_clean != '' GROUP BY phone_clean HAVING COUNT(*) > 1 LIMIT 5")
                        dupes = cur.fetchall()
                        if dupes:
                            logger.warning(f"⚠️ Duplicate contacts detected: {dupes}. Skipping unique constraint to prevent crash.")
                        else:
                            # Standard unique index instead of constraint for better performance & flexibility
                            cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_phone_unique ON contacts(phone_clean) WHERE phone_clean IS NOT NULL AND phone_clean != \'\'')
                    except Exception as idx_err:
                        logger.warning(f"Could not enforce uniqueness on phone_clean: {idx_err}")
            except Exception as col_err:
                logger.error(f"Error adding column {column_name}: {col_err}")

        cur.execute('CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone_clean)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_contacts_source ON contacts(source)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_contacts_category ON contacts(category)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_contacts_city ON contacts(city)')
        cur.close()
        conn.close()
        logger.info("Database tables ready!")
        return True
    except Exception as e:
        logger.error(f"Database init failed: {e}")
        return False


def load_config():
    try:
        with open('config.yaml', 'r') as f:
            return yaml.safe_load(f)
    except Exception:
        return {}


# Initialize tables on startup
try:
    init_tables()
    logger.info("Connected to PostgreSQL!")
except Exception as e:
    logger.warning(f"Could not connect to DB on startup: {e}")


HTML = '''
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
    </style>
</head>
<body>
    <div class="header">
        <h1>Financial Services Contact Scraper</h1>
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

    <script>
        function startFastScrape(){
            if(confirm('Start FAST parallel scrape? This will run multiple cities concurrently.')){
                const btn = document.getElementById('scrape-btn');
                const fastBtn = document.querySelector('button[onclick="startFastScrape()"]');
                fastBtn.disabled = true;
                fastBtn.innerText = '⏳ Running...';
                
                fetch('/api/trigger/fast-scrape', {method: 'POST'}).then(r=>r.json()).then(d=>{
                    alert(d.message || d.error);
                    fastBtn.innerText = '⚡ Fast Scrape';
                    fastBtn.disabled = false;
                }).catch(()=>{ 
                    fastBtn.innerText = '⚡ Fast Scrape';
                    fastBtn.disabled = false;
                });
            }
        }

        function cleanupEmpty(){
            if(confirm('Delete all contacts with no phone AND no email? This cannot be undone.')){
                fetch('/api/cleanup/empty', {method: 'DELETE'}).then(r=>r.json()).then(d=>{
                    alert(d.message || d.error);
                    if(d.success) location.reload();
                });
            }
        }

        function updateQuality(){
            if(confirm('Update quality scores for all contacts?')){
                fetch('/api/cleanup/quality', {method: 'POST'}).then(r=>r.json()).then(d=>{
                    alert(d.message || d.error);
                    if(d.success) location.reload();
                });
            }
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
            if(confirm('Start scraping for all configured cities and categories?')){
                const btn = document.getElementById('scrape-btn');
                btn.disabled = true;
                btn.innerText = '🚧 Starting...';
                fetch('/api/trigger/scrape').then(r=>r.json()).then(d=>{
                    alert(d.message || d.error);
                    btn.innerText = '🚧 Scraping...';
                }).catch(()=>{ btn.disabled=false; btn.innerText='🚀 Start Scrape'; });
            }
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
                if(data.error){ alert(data.error); return; }
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
                el.innerText = data.message || 'Idle';

                if (data.running) {
                    el.className = 'val status-running pulse';
                    btn.disabled = true;
                    btn.innerText = '🚧 Scraping...';
                    wasRunning = true;
                } else {
                    el.className = 'val status-idle';
                    btn.disabled = false;
                    btn.innerText = '🚀 Start Scrape';
                    if (wasRunning) { wasRunning = false; location.reload(); }
                }
            }).catch(()=>{});
        }
        setInterval(pollStatus, 3000);
    </script>
</body>
</html>
'''


@app.route('/')
def index():
    try:
        config = load_config()
        scraper_cfg = config.get('scraper', {})
        page_size = int(os.environ.get('DASHBOARD_PAGE_SIZE', scraper_cfg.get('dashboard_page_size', 50)))
        
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', page_size, type=int)
        
        search_query = request.args.get('q', '')
        selected_city = request.args.get('city', '')
        selected_category = request.args.get('category', '')
        selected_source = request.args.get('source', '')
        selected_quality = request.args.get('quality', '')
        sort_by = request.args.get('sort', 'date')
        
        conn = get_db()
        cur = conn.cursor()
        
        # Sort mapping
        sort_map = {
            'date': 'scraped_at DESC',
            'name': 'name ASC',
            'city': 'city ASC',
            'source': 'source ASC'
        }
        order_by = sort_map.get(sort_by, 'scraped_at DESC')
        
        # Build WHERE clause for filters (case-insensitive)
        where_clauses = []
        params = []
        if search_query:
            where_clauses.append('(name ILIKE %s OR phone ILIKE %s OR email ILIKE %s)')
            search_pattern = f'%{search_query}%'
            params.extend([search_pattern, search_pattern, search_pattern])
        if selected_city:
            where_clauses.append('city ILIKE %s')
            params.append(selected_city)
        if selected_category:
            where_clauses.append('category ILIKE %s')
            params.append(selected_category)
        if selected_source:
            where_clauses.append('source ILIKE %s')
            params.append(selected_source)
        if selected_quality:
            where_clauses.append('(quality_tier = %s OR quality_tier IS NULL)')
            params.append(selected_quality)
        
        where_sql = ' AND '.join(where_clauses) if where_clauses else '1=1'
        
        # Get total count (unfiltered)
        cur.execute('SELECT COUNT(*) as cnt FROM contacts')
        total = cur.fetchone()['cnt']
        
        # Get filtered count
        cur.execute(f'SELECT COUNT(*) as cnt FROM contacts WHERE {where_sql}', params)
        filtered_total = cur.fetchone()['cnt']
        
        total_pages = (filtered_total + limit - 1) // limit if filtered_total > 0 else 1
        
        # Clamp page
        if page > total_pages: page = total_pages
        if page < 1: page = 1
        offset = (page - 1) * limit

        # 1. Fetch Current Page of Contacts
        cur.execute(f"SELECT * FROM contacts WHERE {where_sql} ORDER BY {order_by} LIMIT %s OFFSET %s", (*params, limit, offset))
        contacts = cur.fetchall()

        # 2. Fetch Filter Options (Unique values)
        cur.execute("SELECT DISTINCT city FROM contacts WHERE city IS NOT NULL ORDER BY city")
        cities = [r['city'] for r in cur.fetchall()]
        
        cur.execute("SELECT DISTINCT category FROM contacts WHERE category IS NOT NULL ORDER BY category")
        categories = [r['category'] for r in cur.fetchall()]
        
        cur.execute("SELECT DISTINCT source FROM contacts WHERE source IS NOT NULL ORDER BY source")
        sources = [r['source'] for r in cur.fetchall()]

        # Premium Analytics & Stats Caching (after data fetch to reuse cursor if needed)
        stats = DashboardStats.get_cached_stats(cur)
        
        cur.close()
        conn.close()
        
        return render_template('dash.html', 
            contacts=contacts,
            total=total,
            page=page,
            total_pages=total_pages,
            search_query=search_query,
            selected_city=selected_city,
            selected_category=selected_category,
            selected_source=selected_source,
            selected_quality=selected_quality,
            sort_by=sort_by,
            limit=limit,
            cities=cities,
            categories=categories,
            sources=sources,
            stats=stats,
            s={'filtered_total': filtered_total}
        )
    except Exception as e:
        logger.error(f"Database error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/status')
def get_status():
    if not redis_client:
        return jsonify({"message": "Idle", "running": False})
    try:
        status = redis_client.get("scraper_status")
        if status:
            return Response(status, mimetype='application/json')
    except Exception:
        pass
    return jsonify({"message": "Idle", "running": False})


@app.route('/api/contact/<int:contact_id>')
def get_contact(contact_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT * FROM contacts WHERE id = %s', (contact_id,))
        contact = cur.fetchone()
        cur.close()
        conn.close()
        if contact:
            return jsonify(dict(contact))
        return jsonify({'error': 'Contact not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/logs')
def view_logs():
    try:
        log_files = []
        if LOGS_DIR.exists():
            for f in LOGS_DIR.glob('*.log'):
                mtime = f.stat().st_mtime
                log_files.append({
                    'name': f.name, 
                    'size': f.stat().st_size, 
                    'modified': mtime,
                    'modified_str': datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
                })
        log_files.sort(key=lambda x: x['modified'], reverse=True)
        return render_template('logs.html', logs=log_files[:20])
    except Exception as e:
        return f"Error reading logs: {e}"


@app.route('/logs/<name>')
def get_log(name):
    try:
        log_file = LOGS_DIR / name
        if log_file.exists():
            content = log_file.read_text()
            lines = content.split('\n')
            return jsonify({'name': name, 'lines': lines[-500:]})
        return jsonify({'error': 'Log not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/trigger/scrape')
def trigger_scrape():
    """Trigger scraping tasks. Default: official sources only (AMFI, IRDAI, ICAI)."""
    from tasks import scrape_all_task
    use_business = request.args.get('business', 'false').lower() == 'true'
    
    config = load_config()
    cities = config.get('cities', [])
    categories = config.get('categories', [])
    pair_count = len(cities) * len(categories)
    scrape_all_task.delay(source=None, use_business=use_business)

    source_type = "Business Directories" if use_business else "Official Sources (AMFI, IRDAI, ICAI)"
    return jsonify({
        'message': f'🚀 Batch scrape queued for {source_type} across {pair_count} city/category combinations!',
        'tasks': 1,
        'pairs': pair_count,
        'source_type': source_type,
        'use_business': use_business
    })


@app.route('/api/trigger/fast-scrape', methods=['POST'])
def trigger_fast_scrape():
    """Trigger fast parallel scraping with higher concurrency"""
    from tasks import fast_scrape_task
    from tasks import _load_runtime_config
    
    config = _load_runtime_config()
    cities = config.get('cities', [])
    categories = config.get('categories', [])
    pair_count = len(cities) * len(categories)
    
    max_concurrent = request.args.get('concurrency', 3, type=int)
    
    fast_scrape_task.delay(source=None, use_business=False, max_concurrent=max_concurrent)
    
    return jsonify({
        'message': f'⚡ Fast scrape queued! {pair_count} jobs with concurrency={max_concurrent}',
        'type': 'fast_parallel',
        'jobs': pair_count,
        'concurrency': max_concurrent
    })


@app.route('/api/contacts')
def api_contacts():
    try:
        conn = get_db()
        cur = conn.cursor()
        
        page = request.args.get('page', 1, type=int)
        limit = min(request.args.get('limit', 100, type=int), 1000)
        offset = (page - 1) * limit
        
        # Filter params
        search_query = request.args.get('q', '')
        filter_city = request.args.get('city', '')
        filter_category = request.args.get('category', '')
        filter_source = request.args.get('source', '')
        
        where_clauses = []
        params = []
        if search_query:
            where_clauses.append('(name ILIKE %s OR phone ILIKE %s OR email ILIKE %s)')
            search_pattern = f'%{search_query}%'
            params.extend([search_pattern, search_pattern, search_pattern])
        if filter_city:
            where_clauses.append('city ILIKE %s')
            params.append(filter_city)
        if filter_category:
            where_clauses.append('category ILIKE %s')
            params.append(filter_category)
        if filter_source:
            where_clauses.append('source ILIKE %s')
            params.append(filter_source)
        
        where_sql = ' AND '.join(where_clauses) if where_clauses else '1=1'
        
        cur.execute(f'SELECT name, phone, email, city, category, source FROM contacts WHERE {where_sql} ORDER BY scraped_at DESC LIMIT %s OFFSET %s', params + [limit, offset])
        contacts = cur.fetchall()
        cur.execute(f'SELECT COUNT(*) as cnt FROM contacts WHERE {where_sql}', params)
        total = cur.fetchone()['cnt']
        cur.close()
        conn.close()
        return jsonify({
            'total': total, 
            'page': page,
            'limit': limit,
            'total_pages': (total + limit - 1) // limit,
            'data': [dict(c) for c in contacts]
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/export/<fmt>')
def export(fmt):
    try:
        search_query = request.args.get('q', '')
        filter_city = request.args.get('city', '')
        filter_category = request.args.get('category', '')
        filter_source = request.args.get('source', '')
        
        where_clauses = []
        params = []
        if search_query:
            where_clauses.append('(name ILIKE %s OR phone ILIKE %s OR email ILIKE %s)')
            search_pattern = f'%{search_query}%'
            params.extend([search_pattern, search_pattern, search_pattern])
        if filter_city:
            where_clauses.append('city ILIKE %s')
            params.append(filter_city)
        if filter_category:
            where_clauses.append('category ILIKE %s')
            params.append(filter_category)
        if filter_source:
            where_clauses.append('source ILIKE %s')
            params.append(filter_source)
        
        where_sql = ' AND '.join(where_clauses) if where_clauses else '1=1'
        
        conn = get_db()
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM contacts WHERE {where_sql}', params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if fmt == 'csv':
        import csv
        out = io.StringIO()
        if rows:
            w = csv.DictWriter(out, fieldnames=rows[0].keys())
            w.writeheader()
            for r in rows:
                w.writerow(dict(r))
        return Response(out.getvalue(), mimetype='text/csv',
                        headers={'Content-Disposition': 'attachment;filename=contacts.csv'})
    if fmt == 'json':
        return jsonify({'total': len(rows), 'data': [dict(r) for r in rows]})
    if fmt == 'excel':
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
        return send_file(out, download_name='contacts.xlsx', as_attachment=True)
    return 'Invalid format', 400


# Routes for cleanup and quality updates continue below...

@app.route('/api/cleanup/empty', methods=['DELETE'])
def cleanup_empty_contacts():
    """Delete contacts that have neither phone nor email"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # Delete contacts with no phone AND no email
        cur.execute('''
            DELETE FROM contacts 
            WHERE (phone IS NULL OR TRIM(phone) = '') 
            AND (email IS NULL OR TRIM(email) = '')
        ''')
        deleted_count = cur.rowcount
        
        conn.commit()
        
        # Get remaining count
        cur.execute('SELECT COUNT(*) as cnt FROM contacts')
        remaining = cur.fetchone()['cnt']
        
        cur.close()
        conn.close()
        
        logger.info(f"Cleaned up {deleted_count} empty contacts. Remaining: {remaining}")
        return jsonify({
            'success': True, 
            'deleted': deleted_count,
            'remaining': remaining,
            'message': f'Deleted {deleted_count} contacts with no phone or email'
        })
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/cleanup/quality', methods=['POST'])
def cleanup_low_quality():
    """Recalculate and update quality scores for all contacts"""
    try:
        from quality_pipeline import DataQualityPipeline
        
        conn = get_db()
        cur = conn.cursor()
        
        # Get all contacts
        cur.execute('SELECT * FROM contacts')
        contacts = cur.fetchall()
        
        updated = 0
        for contact in contacts:
            # Process through unified quality pipeline
            processed = DataQualityPipeline.enrich_contact(dict(contact))
            
            # Update quality fields
            cur.execute('''
                UPDATE contacts 
                SET phone_clean = %s, 
                    email_valid = %s, 
                    quality_score = %s, 
                    quality_tier = %s
                WHERE id = %s
            ''', (
                processed.get('phone_clean'),
                processed.get('email_valid', False),
                processed.get('quality_score', 0),
                processed.get('quality_tier', 'low'),
                contact['id']
            ))
            updated += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Updated quality scores for {updated} contacts")
        return jsonify({
            'success': True,
            'updated': updated,
            'message': f'Updated quality scores for {updated} contacts'
        })
    except Exception as e:
        logger.error(f"Quality update failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/health')
def health():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT 1')
        cur.close()
        conn.close()
        return jsonify({'status': 'ok', 'db': 'connected'})
    except Exception as e:
        return jsonify({'status': 'error', 'db': str(e)})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
