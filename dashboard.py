from flask import Flask, render_template_string, request, jsonify, Response, send_file
import asyncpg
import asyncio
import yaml
import io
import re
import os
import logging
import json
import redis
from datetime import datetime
from functools import wraps
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

# Redis Client for Status Tracking
REDIS_URL = os.environ.get('REDIS_URL')
redis_client = redis.Redis.from_url(REDIS_URL) if REDIS_URL else None

_db_pool = None

async def init_db_pool():
    global _db_pool
    if _db_pool is not None and _db_pool != 'sqlite':
        return _db_pool
    
    db_url = os.environ.get('DATABASE_URL')
    
    if db_url:
        try:
            _db_pool = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5, command_timeout=60)
            async with _db_pool.acquire() as conn:
                await conn.execute('SELECT 1')
                await create_tables(conn)
            logger.info("Connected to Railway PostgreSQL!")
            return _db_pool
        except Exception as e:
            logger.warning(f"Railway DB connection failed: {e}")
    
    config = load_config().get('database', {}) if load_config else {}
    try:
        _db_pool = await asyncpg.create_pool(
            host=os.environ.get('DATABASE_HOST', config.get('host', 'localhost')),
            port=int(os.environ.get('DATABASE_PORT', config.get('port', 5432))),
            database=os.environ.get('DATABASE_NAME', config.get('name', 'scraper_db')),
            user=os.environ.get('DATABASE_USER', config.get('user', 'postgres')),
            password=os.environ.get('DATABASE_PASSWORD', config.get('password', '')),
            min_size=1, max_size=5, command_timeout=60
        )
        async with _db_pool.acquire() as conn:
            await conn.execute('SELECT 1')
            await create_tables(conn)
        logger.info("Connected to PostgreSQL!")
        return _db_pool
    except Exception as e:
        logger.warning(f"PostgreSQL connection failed: {e}")
        _db_pool = 'sqlite'
        return 'sqlite'

async def create_tables(conn):
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS contacts (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            phone VARCHAR(50),
            email VARCHAR(255),
            address TEXT,
            category VARCHAR(100),
            city VARCHAR(100),
            area VARCHAR(100),
            source VARCHAR(100),
            source_url TEXT,
            phone_clean VARCHAR(50),
            email_valid BOOLEAN,
            enriched BOOLEAN,
            quality_score INTEGER DEFAULT 0,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    await conn.execute('CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone_clean)')
    await conn.execute('CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email)')
    await conn.execute('CREATE INDEX IF NOT EXISTS idx_contacts_source ON contacts(source)')
    await conn.execute('CREATE INDEX IF NOT EXISTS idx_contacts_category ON contacts(category)')
    await conn.execute('CREATE INDEX IF NOT EXISTS idx_contacts_city ON contacts(city)')
    logger.info("Database tables ready!")

def load_config():
    try:
        with open('config.yaml', 'r') as f:
            return yaml.safe_load(f)
    except:
        return {}

def validate_phone(phone):
    if not phone:
        return False
    digits = re.sub(r'[^\d]', '', phone)
    return len(digits) >= 10

def validate_email(email):
    if not email:
        return False
    return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email))

HTML = '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Contact Scraper Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, sans-serif; background: #f5f5f5; padding: 20px; }
        .header { background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 20px; border-radius: 12px; margin-bottom: 20px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 20px; }
        .stat { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .stat h3 { color: #666; font-size: 12px; }
        .stat .val { font-size: 28px; font-weight: bold; }
        .card { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }
        .filters { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 20px; }
        .filters input, .filters select { padding: 10px; border: 1px solid #ddd; border-radius: 6px; }
        .btn { padding: 10px 20px; background: #667eea; color: white; border: none; border-radius: 6px; cursor: pointer; }
        .btn-green { background: #4caf50; }
        table { width: 100%; border-collapse: collapse; background: white; border-radius: 10px; overflow: hidden; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; font-weight: 600; }
        .tag { padding: 4px 8px; border-radius: 12px; font-size: 11px; }
        .tag-source { background: #e3f2fd; color: #1976d2; }
        .tag-cat { background: #e8f5e9; color: #388e3c; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.5; } 100% { opacity: 1; } }
        .pulse { animation: pulse 1.5s infinite; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Financial Services Contact Scraper</h1>
    </div>
    <div class="stats">
        <div class="stat"><h3>Total Contacts</h3><div class="val">{{s.total}}</div></div>
        <div class="stat"><h3>Phone Numbers</h3><div class="val">{{s.phone}}</div></div>
        <div class="stat"><h3>Verified Emails</h3><div class="val">{{s.email}}</div></div>
        <div class="stat" style="border: 2px solid #667eea;">
            <h3>Live Scraper Status</h3>
            <div id="live-status" class="val" style="font-size:16px; color:#667eea;">Idle</div>
        </div>
    </div>
    <div class="card">
        <div class="charts" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:20px;">
            <div><h3>By Source</h3><canvas id="c1"></canvas></div>
            <div><h3>By Category</h3><canvas id="c2"></canvas></div>
        </div>
    </div>
    <div class="filters">
        <button class="btn btn-green" onclick="exportCSV()">Export CSV</button>
        <button class="btn btn-green" onclick="exportJSON()">Export JSON</button>
        <button class="btn" style="background:#ff9800" id="scrape-btn" onclick="startScrape()">🚀 Start Scrape</button>
    </div>
    <table>
        <thead><tr><th>Name</th><th>Phone</th><th>Email</th><th>City</th><th>Source</th><th>Category</th></tr></thead>
        <tbody>{% for c in contacts %}<tr>
            <td>{{c.name}}</td>
            <td>{{c.phone or '-'}}</td>
            <td>{{c.email or '-'}}</td>
            <td>{{c.city}}</td>
            <td><span class="tag tag-source">{{c.source}}</span></td>
            <td><span class="tag tag-cat">{{c.category}}</span></td>
        </tr>{% endfor %}</tbody>
    </table>
    <script>
        new Chart(document.getElementById('c1'),{type:'doughnut',data:{labels:Object.keys({{by_source|tojson}}),datasets:[{data:Object.values({{by_source|tojson}}),backgroundColor:['#667eea','#764ba2','#4caf50']}]}});
        new Chart(document.getElementById('c2'),{type:'bar',data:{labels:Object.keys({{by_cat|tojson}}),datasets:[{data:Object.values({{by_cat|tojson}}),backgroundColor:'#667eea'}]}});
        
        function exportCSV(){window.location.href='/export/csv'}
        function exportJSON(){window.location.href='/export/json'}
        
        function startScrape(){
            if(confirm('Start new scraping job for all cities/categories in config.yaml?')){
                fetch('/api/trigger/scrape').then(r => r.json()).then(d => alert(d.message || d.error));
            }
        }

        let lastRunningState = false;
        function updateStatus() {
            fetch('/api/status').then(r => r.json()).then(data => {
                const el = document.getElementById('live-status');
                const btn = document.getElementById('scrape-btn');
                el.innerText = data.message || "Idle";
                
                if (data.running) {
                    el.style.color = "#ff9800";
                    el.classList.add('pulse');
                    btn.disabled = true;
                    btn.style.opacity = "0.5";
                    btn.innerText = "🚧 Scraping...";
                    lastRunningState = true;
                } else {
                    el.style.color = "#667eea";
                    el.classList.remove('pulse');
                    btn.disabled = false;
                    btn.style.opacity = "1";
                    btn.innerText = "🚀 Start Scrape";
                    if (lastRunningState) {
                        location.reload(); // Refresh when job finishes to show new data
                    }
                    lastRunningState = false;
                }
            });
        }
        setInterval(updateStatus, 3000);
    </script>
</body>
</html>
'''

@app.route('/')
async def index():
    pool = await init_db_pool()
    if pool == 'sqlite': return "Database Error."
    async with pool.acquire() as conn:
        contacts = await conn.fetch('SELECT * FROM contacts ORDER BY scraped_at DESC LIMIT 100')
        total = await conn.fetchval('SELECT COUNT(*) FROM contacts')
        with_phone = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE phone IS NOT NULL AND phone != ''")
        with_email = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL AND email != ''")
        sources = await conn.fetch('SELECT DISTINCT source FROM contacts')
        by_source = await conn.fetch('SELECT source, COUNT(*) c FROM contacts GROUP BY source')
        by_cat = await conn.fetch('SELECT category, COUNT(*) c FROM contacts GROUP BY category')
    
    return render_template_string(HTML,
        contacts=contacts, s={'total':total,'phone':with_phone,'email':with_email,'cities':len(sources)},
        by_source={r['source']:r['c'] for r in by_source}, by_cat={r['category']:r['c'] for r in by_cat})

@app.route('/api/status')
async def get_status():
    if not redis_client: return jsonify({"message": "Idle", "running": False})
    try:
        status = redis_client.get("scraper_status")
        if status:
            return Response(status, mimetype='application/json')
    except:
        pass
    return jsonify({"message": "Idle", "running": False})

@app.route('/api/trigger/scrape')
async def trigger_scrape():
    from tasks import scrape_category_task
    config = load_config()
    cities = config.get('cities', [])
    categories = config.get('categories', [])
    sources = config.get('sources', ['justdial'])
    
    count = 0
    for city in cities:
        for cat in categories:
            for source in sources:
                scrape_category_task.delay(city, cat, source)
                count += 1
    
    return jsonify({'message': f'Search started! {count} tasks added to the queue.', 'tasks': count})

@app.route('/export/<fmt>')
async def export(fmt):
    pool = await init_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM contacts')
    if fmt == 'csv':
        import csv
        out = io.StringIO()
        if rows:
            w = csv.DictWriter(out, fieldnames=rows[0].keys())
            w.writeheader()
            for r in rows: w.writerow(dict(r))
        return Response(out.getvalue(), mimetype='text/csv', headers={'Content-Disposition':'attachment;filename=contacts.csv'})
    return 'Invalid',400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)