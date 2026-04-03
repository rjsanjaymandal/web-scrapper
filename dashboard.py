from flask import Flask, render_template_string, request, jsonify, Response, send_file
import asyncpg
import asyncio
import yaml
import io
import re
import os
import logging
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

def get_db_pool():
    return asyncio.run(init_db_pool())

def validate_phone(phone):
    if not phone:
        return False
    digits = re.sub(r'[^\d]', '', phone)
    return len(digits) >= 10

def validate_email(email):
    if not email:
        return False
    return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email))

def format_phone(phone):
    if not phone:
        return ""
    digits = re.sub(r'[^\d]', '', phone)
    if len(digits) >= 10:
        return f"+91-{digits[-10:-7]}-{digits[-7:-4]}-{digits[-4:]}"
    return phone

def calc_quality(c):
    score = 0
    if c.get('phone_clean'): score += 30
    if c.get('email') and validate_email(c.get('email')): score += 30
    if c.get('address'): score += 20
    if c.get('city'): score += 10
    if c.get('area'): score += 10
    return min(score, 100)

HTML = '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Contact Scraper</title>
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
    </style>
</head>
<body>
    <div class="header">
        <h1>Contact Scraper Dashboard</h1>
    </div>
    <div class="stats">
        <div class="stat"><h3>Total</h3><div class="val">{{s.total}}</div></div>
        <div class="stat"><h3>Phone</h3><div class="val">{{s.phone}}</div></div>
        <div class="stat"><h3>Email</h3><div class="val">{{s.email}}</div></div>
        <div class="stat"><h3>Cities</h3><div class="val">{{s.cities}}</div></div>
    </div>
    <div class="card">
        <div class="charts" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:20px;">
            <div><h3>By Source</h3><canvas id="c1"></canvas></div>
            <div><h3>By Category</h3><canvas id="c2"></canvas></div>
        </div>
    </div>
    <div class="filters">
        <input type="text" id="q" placeholder="Search...">
        <select id="src"><option value="">Source</option>{% for s in sources %}<option>{{s}}</option>{% endfor %}</select>
        <select id="cat"><option value="">Category</option>{% for c in cats %}<option>{{c}}</option>{% endfor %}</select>
        <button class="btn btn-green" onclick="exportCSV()">CSV</button>
        <button class="btn btn-green" onclick="exportJSON()">JSON</button>
        <button class="btn" onclick="validate()">Validate</button>
        <button class="btn" style="background:#ff9800" onclick="startScrape()">🚀 Start Scrape</button>
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
        function validate(){if(confirm('Validate?'))window.location.href='/admin/validate'}
        function startScrape(){
            if(confirm('Start new scraping job for all cities/categories?')){
                fetch('/api/trigger/scrape')
                    .then(r => r.json())
                    .then(d => alert(d.message || d.error));
            }
        }
    </script>
</body>
</html>
'''

@app.route('/')
async def index():
    pool = await init_db_pool()
    if pool == 'sqlite':
        return "Database not configured. Set DATABASE_URL or DATABASE_PASSWORD."
    
    async with pool.acquire() as conn:
        contacts = await conn.fetch('SELECT * FROM contacts ORDER BY scraped_at DESC LIMIT 100')
        total = await conn.fetchval('SELECT COUNT(*) FROM contacts')
        with_phone = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE phone IS NOT NULL AND phone != ''")
        with_email = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL AND email != ''")
        sources = await conn.fetch('SELECT DISTINCT source FROM contacts')
        cats = await conn.fetch('SELECT DISTINCT category FROM contacts')
        by_source = await conn.fetch('SELECT source, COUNT(*) c FROM contacts GROUP BY source')
        by_cat = await conn.fetch('SELECT category, COUNT(*) c FROM contacts GROUP BY category')
    
    return render_template_string(HTML,
        contacts=contacts, s={'total':total,'phone':with_phone,'email':with_email,'cities':len(sources)},
        sources=[s['source'] for s in sources], cats=[c['category'] for c in cats],
        by_source={r['source']:r['c'] for r in by_source}, by_cat={r['category']:r['c'] for r in by_cat})

@app.route('/health')
async def health():
    pool = await init_db_pool()
    if pool == 'sqlite':
        return jsonify({'status':'error','db':'not configured'})
    try:
        async with pool.acquire() as conn:
            await conn.execute('SELECT 1')
        return jsonify({'status':'ok','db':'connected'})
    except:
        return jsonify({'status':'error','db':'disconnected'})

@app.route('/api/contacts')
async def api():
    pool = await init_db_pool()
    if pool == 'sqlite':
        return jsonify({'error':'no database'})
    limit = min(request.args.get('limit',100,type=int),1000)
    async with pool.acquire() as conn:
        contacts = await conn.fetch(f'SELECT name,phone,email,city,category,source FROM contacts ORDER BY scraped_at DESC LIMIT {limit}')
        total = await conn.fetchval('SELECT COUNT(*) FROM contacts')
    return jsonify({'total':total,'data':[dict(c) for c in contacts]})

@app.route('/api/stats')
async def stats():
    pool = await init_db_pool()
    if pool == 'sqlite':
        return jsonify({'error':'no database'})
    async with pool.acquire() as conn:
        by_source = await conn.fetch('SELECT source, COUNT(*) c FROM contacts GROUP BY source')
        by_cat = await conn.fetch('SELECT category, COUNT(*) c FROM contacts GROUP BY category')
    return jsonify({'by_source':{r['source']:r['c'] for r in by_source},'by_category':{r['category']:r['c'] for r in by_cat}})

@app.route('/export/<fmt>')
async def export(fmt):
    pool = await init_db_pool()
    if pool == 'sqlite':
        return jsonify({'error':'no database'})
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
    if fmt == 'json':
        return jsonify({'total':len(rows),'data':[dict(r) for r in rows]})
    if fmt == 'excel':
        wb = Workbook()
        ws = wb.active
        ws.title = "Contacts"
        if rows:
            ws.append(list(rows[0].keys()))
            for r in rows: ws.append(list(r.values()))
        out = io.BytesIO()
        wb.save(out)
        out.seek(0)
        return send_file(out, download_name='contacts.xlsx', as_attachment=True)
    return 'Invalid',400

@app.route('/admin/validate')
async def validate():
    pool = await init_db_pool()
    if pool == 'sqlite':
        return jsonify({'error':'no database'})
    updated = 0
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT id, phone, email FROM contacts')
        for r in rows:
            p = validate_phone(r['phone']) if r['phone'] else False
            e = validate_email(r['email']) if r['email'] else False
            await conn.execute('UPDATE contacts SET email_valid=$1 WHERE id=$2', e, r['id'])
            updated += 1
    return jsonify({'validated':updated})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)