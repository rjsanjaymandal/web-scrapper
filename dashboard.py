from flask import Flask, render_template_string, request, jsonify, Response, send_file
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
                phone_clean VARCHAR(50),
                email_valid BOOLEAN,
                enriched BOOLEAN,
                arn VARCHAR(50),
                license_no VARCHAR(100),
                membership_no VARCHAR(100),
                quality_score INTEGER DEFAULT 0,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        required_columns = {
            'name': 'VARCHAR(255)',
            'phone': 'VARCHAR(50)',
            'email': 'VARCHAR(255)',
            'address': 'TEXT',
            'category': 'VARCHAR(100)',
            'city': 'VARCHAR(100)',
            'area': 'VARCHAR(100)',
            'state': 'VARCHAR(100)',
            'source': 'VARCHAR(100)',
            'source_url': 'TEXT',
            'phone_clean': 'VARCHAR(50)',
            'email_valid': 'BOOLEAN',
            'enriched': 'BOOLEAN',
            'arn': 'VARCHAR(50)',
            'license_no': 'VARCHAR(100)',
            'membership_no': 'VARCHAR(100)',
            'quality_score': 'INTEGER DEFAULT 0',
            'scraped_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        }

        for column_name, column_type in required_columns.items():
            cur.execute(f'ALTER TABLE contacts ADD COLUMN IF NOT EXISTS {column_name} {column_type}')

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
    </div>

    <div class="filters">
        <div>
            <label>Search</label><br>
            <input type="text" id="filter-search" placeholder="Name, phone, email..." value="{{search_query}}" style="padding:8px 12px;background:#1c1f2e;border:1px solid #2d3148;border-radius:6px;color:#c9d1d9;font-size:14px;min-width:180px;">
        </div>
        <div>
            <label>City</label><br>
            <select id="filter-city" onchange="applyFilters()">
                <option value="">All Cities</option>
                {% for c in cities %}<option value="{{c}}" {% if selected_city==c %}selected{% endif %}>{{c}}</option>{% endfor %}
            </select>
        </div>
        <div>
            <label>State</label><br>
            <select id="filter-state" onchange="applyFilters()">
                <option value="">All States</option>
                {% for s in states %}<option value="{{s}}" {% if selected_state==s %}selected{% endif %}>{{s}}</option>{% endfor %}
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
        <div style="display:flex;gap:8px;align-items:flex-end;">
            <button class="btn btn-filter" onclick="applyFilters()">Apply</button>
            <button class="btn btn-clear" onclick="clearFilters()">Clear</button>
        </div>
        <div class="filter-stats">Showing {{contacts|length}} of {{s.filtered_total}} results</div>
    </div>

    {% if contacts %}
    <table>
        <thead><tr><th>Name</th><th>Phone</th><th>Email</th><th>City</th><th>Source</th><th>Category</th></tr></thead>
        <tbody>{% for c in contacts %}<tr>
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
        {% if search_query or selected_city or selected_state or selected_category or selected_source %}
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
        <a href="/?page=1{% if search_query %}&q={{search_query}}{% endif %}{% if selected_city %}&city={{selected_city}}{% endif %}{% if selected_state %}&state={{selected_state}}{% endif %}{% if selected_category %}&category={{selected_category}}{% endif %}{% if selected_source %}&source={{selected_source}}{% endif %}" class="page-link {% if page == 1 %}disabled{% endif %}">« First</a>
        <a href="/?page={{ page - 1 }}{% if search_query %}&q={{search_query}}{% endif %}{% if selected_city %}&city={{selected_city}}{% endif %}{% if selected_state %}&state={{selected_state}}{% endif %}{% if selected_category %}&category={{selected_category}}{% endif %}{% if selected_source %}&source={{selected_source}}{% endif %}" class="page-link {% if page == 1 %}disabled{% endif %}">‹ Prev</a>
        
        <span class="page-info">Page <b>{{ page }}</b> of <b>{{ total_pages }}</b></span>

        <a href="/?page={{ page + 1 }}{% if search_query %}&q={{search_query}}{% endif %}{% if selected_city %}&city={{selected_city}}{% endif %}{% if selected_state %}&state={{selected_state}}{% endif %}{% if selected_category %}&category={{selected_category}}{% endif %}{% if selected_source %}&source={{selected_source}}{% endif %}" class="page-link {% if page == total_pages %}disabled{% endif %}">Next ›</a>
        <a href="/?page={{ total_pages }}{% if search_query %}&q={{search_query}}{% endif %}{% if selected_city %}&city={{selected_city}}{% endif %}{% if selected_state %}&state={{selected_state}}{% endif %}{% if selected_category %}&category={{selected_category}}{% endif %}{% if selected_source %}&source={{selected_source}}{% endif %}" class="page-link {% if page == total_pages %}disabled{% endif %}">Last »</a>
    </div>
    {% endif %}

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
            const state = document.getElementById('filter-state').value;
            const category = document.getElementById('filter-category').value;
            const source = document.getElementById('filter-source').value;
            
            let params = new URLSearchParams();
            if(search) params.set('q', search);
            if(city) params.set('city', city);
            if(state) params.set('state', state);
            if(category) params.set('category', category);
            if(source) params.set('source', source);
            
            const url = params.toString() ? '?' + params.toString() : '/';
            window.location.href = url;
        }

        function clearFilters(){
            window.location.href = '/';
        }

        function exportWithFilters(fmt){
            const search = document.getElementById('filter-search').value;
            const city = document.getElementById('filter-city').value;
            const state = document.getElementById('filter-state').value;
            const category = document.getElementById('filter-category').value;
            const source = document.getElementById('filter-source').value;
            
            let params = new URLSearchParams();
            if(search) params.set('q', search);
            if(city) params.set('city', city);
            if(state) params.set('state', state);
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
        selected_state = request.args.get('state', '')
        selected_category = request.args.get('category', '')
        selected_source = request.args.get('source', '')
        
        conn = get_db()
        cur = conn.cursor()
        
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
        if selected_state:
            where_clauses.append('state ILIKE %s')
            params.append(selected_state)
        if selected_category:
            where_clauses.append('category ILIKE %s')
            params.append(selected_category)
        if selected_source:
            where_clauses.append('source ILIKE %s')
            params.append(selected_source)
        
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

        cur.execute(f'SELECT * FROM contacts WHERE {where_sql} ORDER BY scraped_at DESC LIMIT %s OFFSET %s', params + [limit, offset])
        contacts = cur.fetchall()
        
        # Get unique values for filter dropdowns
        cur.execute('SELECT DISTINCT city FROM contacts WHERE city IS NOT NULL AND city <> %s ORDER BY city', ('',))
        cities = [r['city'] for r in cur.fetchall()]
        
        cur.execute('SELECT DISTINCT state FROM contacts WHERE state IS NOT NULL AND state <> %s ORDER BY state', ('',))
        states = [r['state'] for r in cur.fetchall()]
        
        cur.execute('SELECT DISTINCT category FROM contacts WHERE category IS NOT NULL AND category <> %s ORDER BY category', ('',))
        categories = [r['category'] for r in cur.fetchall()]
        
        cur.execute('SELECT DISTINCT source FROM contacts WHERE source IS NOT NULL AND source <> %s ORDER BY source', ('',))
        sources = [r['source'] for r in cur.fetchall()]
        
        # Stats (unfiltered)
        cur.execute("SELECT COUNT(*) as cnt FROM contacts WHERE phone IS NOT NULL AND phone <> %s", ('',))
        with_phone = cur.fetchone()['cnt']
        cur.execute("SELECT COUNT(*) as cnt FROM contacts WHERE email IS NOT NULL AND email <> %s", ('',))
        with_email = cur.fetchone()['cnt']
        cur.execute('SELECT COUNT(DISTINCT city) as cnt FROM contacts')
        city_count = cur.fetchone()['cnt']
        cur.execute('SELECT source, COUNT(*) as c FROM contacts GROUP BY source')
        by_source = {r['source']: r['c'] for r in cur.fetchall()}
        cur.execute('SELECT category, COUNT(*) as c FROM contacts GROUP BY category')
        by_cat = {r['category']: r['c'] for r in cur.fetchall()}
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Database error: {e}")
        contacts, total, filtered_total, with_phone, with_email, city_count = [], 0, 0, 0, 0, 0
        by_source, by_cat, total_pages, page = {}, {}, 1, 1
        cities, states, categories, sources = [], [], [], []
        selected_city = selected_state = selected_category = selected_source = ''
        search_query = ''

    return render_template_string(HTML,
        contacts=contacts,
        s={'total': total, 'phone': with_phone, 'email': with_email, 'cities': city_count, 'filtered_total': filtered_total},
        by_source=by_source, by_cat=by_cat,
        page=page, total_pages=total_pages,
        cities=cities, states=states, categories=categories, sources=sources,
        selected_city=selected_city, selected_state=selected_state, selected_category=selected_category, selected_source=selected_source,
        search_query=search_query)


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
        filter_state = request.args.get('state', '')
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
        if filter_state:
            where_clauses.append('state ILIKE %s')
            params.append(filter_state)
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
        filter_state = request.args.get('state', '')
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
        if filter_state:
            where_clauses.append('state ILIKE %s')
            params.append(filter_state)
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
