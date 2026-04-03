from flask import Flask, render_template_string, request, jsonify, Response, send_file, g
import asyncpg
import asyncio
import yaml
import json
import io
import re
import os
import logging
from datetime import datetime, timedelta
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

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

_db_pool = None

async def init_pool():
    global _db_pool
    if _db_pool is None:
        db_url = os.environ.get('DATABASE_URL')
        if db_url:
            try:
                _db_pool = await asyncpg.create_pool(dsn=db_url, timeout=5)
                # Test connection
                async with _db_pool.acquire() as conn:
                    await conn.execute('SELECT 1')
            except Exception:
                _db_pool = 'sqlite'
        else:
            try:
                config = load_config().get('database', {})
                _db_pool = await asyncpg.create_pool(
                    host=config.get('host', 'localhost'),
                    port=config.get('port', 5432),
                    database=config.get('name', 'scraper_db'),
                    user=config.get('user', 'postgres'),
                    password=config.get('password', ''),
                    timeout=2
                )
                async with _db_pool.acquire() as conn:
                    await conn.execute('SELECT 1')
            except Exception:
                logger.warning("PostgreSQL connection failed. Using local SQLite.")
                _db_pool = 'sqlite'
    return _db_pool

def get_db_pool():
    # If using sync Flask with async routes, we still need to manage the loop
    # In a more traditional async Flask setup, we'd do this differently.
    return asyncio.run(init_pool())

def validate_phone(phone: str) -> bool:
    if not phone:
        return False
    digits = re.sub(r'[^\d]', '', phone)
    return len(digits) >= 10

def validate_email(email: str) -> bool:
    if not email:
        return False
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def format_phone(phone: str) -> str:
    if not phone:
        return ""
    digits = re.sub(r'[^\d]', '', phone)
    if len(digits) >= 10:
        return f"+91-{digits[-10:-7]}-{digits[-7:-4]}-{digits[-4:]}"
    return phone

def calculate_quality_score(contact: dict) -> int:
    score = 0
    if contact.get('phone_clean'):
        score += 30
    if contact.get('email') and validate_email(contact.get('email')):
        score += 30
    if contact.get('address'):
        score += 20
    if contact.get('city'):
        score += 10
    if contact.get('area'):
        score += 10
    return min(score, 100)

class RateLimiter:
    def __init__(self):
        self.requests = {}
        self.limits = {'default': 100, 'api': 1000}
    
    def check(self, key: str, limit_type: str = 'default') -> bool:
        limit = self.limits.get(limit_type, 100)
        now = datetime.now()
        
        if key not in self.requests:
            self.requests[key] = []
        
        self.requests[key] = [t for t in self.requests[key] if now - t < timedelta(minutes=1)]
        
        if len(self.requests[key]) >= limit:
            return False
        
        self.requests[key].append(now)
        return True

rate_limiter = RateLimiter()

def rate_limit(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get('X-API-Key') or request.remote_addr
        if not rate_limiter.check(key, 'api'):
            return jsonify({'error': 'Rate limit exceeded'}), 429
        return f(*args, **kwargs)
    return decorated

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not token:
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Contact Scraper Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f0f2f5; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; display: flex; justify-content: space-between; align-items: center; }
        .header h1 { font-size: 24px; }
        .nav a { color: white; text-decoration: none; margin-left: 20px; padding: 8px 16px; border-radius: 6px; }
        .nav a:hover { background: rgba(255,255,255,0.1); }
        .container { max-width: 1600px; margin: 0 auto; padding: 20px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat-card { background: white; padding: 20px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .stat-card h3 { color: #666; font-size: 14px; margin-bottom: 10px; }
        .stat-card .value { font-size: 32px; font-weight: bold; color: #333; }
        .stat-card .sub { font-size: 12px; color: #999; }
        .stat-card .badge { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 12px; margin-top: 5px; }
        .badge-good { background: #e8f5e9; color: #2e7d32; }
        .badge-warn { background: #fff3e0; color: #ef6c00; }
        .badge-bad { background: #ffebee; color: #c62828; }
        .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .chart-card { background: white; padding: 20px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .filters { background: white; padding: 20px; border-radius: 12px; margin-bottom: 20px; display: flex; flex-wrap: wrap; gap: 10px; }
        .filters input, .filters select { padding: 10px 15px; border: 1px solid #ddd; border-radius: 8px; }
        .btn { padding: 10px 20px; background: #667eea; color: white; border: none; border-radius: 8px; cursor: pointer; }
        .btn:hover { background: #5568d3; }
        .btn-green { background: #4caf50; }
        .btn-green:hover { background: #43a047; }
        .btn-red { background: #f44336; }
        .table-container { background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; }
        th { background: #f8f9fa; padding: 15px; text-align: left; font-weight: 600; }
        td { padding: 12px 15px; border-top: 1px solid #eee; }
        tr:hover { background: #f8f9fa; }
        .tag { padding: 4px 10px; border-radius: 20px; font-size: 12px; }
        .tag-source { background: #e3f2fd; color: #1976d2; }
        .tag-category { background: #e8f5e9; color: #388e3c; }
        .quality-score { font-weight: bold; }
        .score-high { color: #2e7d32; }
        .score-mid { color: #ef6c00; }
        .score-low { color: #c62828; }
        .api-key-input { font-family: monospace; background: #f5f5f5; padding: 10px; border-radius: 4px; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Contact Scraper Dashboard</h1>
        <nav class="nav">
            <a href="/">Dashboard</a>
            <a href="/api/contacts">API</a>
            <a href="/admin">Admin</a>
            <a href="/health">Health</a>
        </nav>
    </div>
    <div class="container">
        <div class="stats">
            <div class="stat-card">
                <h3>Total Contacts</h3>
                <div class="value">{{ stats.total }}</div>
                <div class="sub">Unique records in DB</div>
            </div>
            <div class="stat-card">
                <h3>With Phone</h3>
                <div class="value">{{ stats.with_phone }}</div>
                <div class="sub">{{ stats.phone_pct }}% coverage</div>
            </div>
            <div class="stat-card">
                <h3>With Email</h3>
                <div class="value">{{ stats.with_email }}</div>
                <div class="sub">{{ stats.email_pct }}% coverage</div>
            </div>
            <div class="stat-card">
                <h3>Quality Score</h3>
                <div class="value">{{ stats.avg_quality }}</div>
                <div class="sub">Average quality</div>
            </div>
            <div class="stat-card">
                <h3>Valid Contacts</h3>
                <div class="value">{{ stats.validated }}</div>
                <div class="sub">Validated records</div>
            </div>
            <div class="stat-card">
                <h3>Last Update</h3>
                <div class="value" style="font-size:16px">{{ stats.last_updated }}</div>
            </div>
        </div>
        
        <div class="charts">
            <div class="chart-card"><h3>Contacts by Source</h3><canvas id="sourceChart"></canvas></div>
            <div class="chart-card"><h3>Contacts by Category</h3><canvas id="categoryChart"></canvas></div>
            <div class="chart-card"><h3>Quality Distribution</h3><canvas id="qualityChart"></canvas></div>
            <div class="chart-card"><h3>Contacts by City</h3><canvas id="cityChart"></canvas></div>
        </div>
        
        <div class="filters">
            <input type="text" id="searchInput" placeholder="Search name, phone, email..." style="flex:1;">
            <select id="sourceFilter"><option value="">All Sources</option>{% for s in sources %}<option value="{{s}}">{{s}}</option>{% endfor %}</select>
            <select id="categoryFilter"><option value="">All Categories</option>{% for c in categories %}<option value="{{c}}">{{c}}</option>{% endfor %}</select>
            <select id="cityFilter"><option value="">All Cities</option>{% for c in cities %}<option value="{{c}}">{{c}}</option>{% endfor %}</select>
            <select id="qualityFilter"><option value="">All Quality</option><option value="high">High (70+)</option><option value="mid">Medium (40-70)</option><option value="low">Low (<40)</option></select>
            <button class="btn" onclick="applyFilters()">Filter</button>
            <button class="btn btn-green" onclick="triggerScrape()">Start Scrape</button>
            <button class="btn btn-green" onclick="exportData('csv')">CSV</button>
            <button class="btn btn-green" onclick="exportData('json')">JSON</button>
            <button class="btn btn-green" onclick="exportData('excel')">Excel</button>
            <button class="btn btn-red" onclick="validateAll()">Validate</button>
            <button class="btn btn-red" onclick="formatPhones()">Format Phones</button>
        </div>
        
        <div class="table-container">
            <table>
                <thead><tr><th>Name</th><th>Phone</th><th>Email</th><th>City</th><th>Source</th><th>Category</th><th>Quality</th><th>Date</th></tr></thead>
                <tbody>{% for c in contacts %}<tr>
                    <td>{{c.name}}</td>
                    <td>{{c.phone or '-'}}</td>
                    <td>{{c.email or '-'}}</td>
                    <td>{{c.city}}</td>
                    <td><span class="tag tag-source">{{c.source}}</span></td>
                    <td><span class="tag tag-category">{{c.category}}</span></td>
                    <td class="quality-score {% if c.quality_score>=70 %}score-high{% elif c.quality_score>=40 %}score-mid{% else %}score-low{% endif %}">{{c.quality_score}}</td>
                    <td>{{c.scraped_at.strftime('%Y-%m-%d') if c.scraped_at else '-'}}</td>
                </tr>{% endfor %}</tbody>
            </table>
        </div>
    </div>
    
    <script>
        new Chart(document.getElementById('sourceChart'), {type:'doughnut',data:{labels:Object.keys({{stats.by_source|tojson}}),datasets:[{data:Object.values({{stats.by_source|tojson}}),backgroundColor:['#667eea','#764ba2','#4caf50','#ff9800','#f44336']}]}});
        new Chart(document.getElementById('categoryChart'), {type:'bar',data:{labels:Object.keys({{stats.by_category|tojson}}),datasets:[{label:'Contacts',data:Object.values({{stats.by_category|tojson}}),backgroundColor:'#667eea'}]}});
        new Chart(document.getElementById('qualityChart'), {type:'bar',data:{labels:['High','Medium','Low'],datasets:[{data:[{{stats.quality_high}},{{stats.quality_mid}},{{stats.quality_low}}],backgroundColor:['#4caf50','#ff9800','#f44336']}]}});
        new Chart(document.getElementById('cityChart'), {type:'bar',data:{labels:Object.keys({{stats.by_city|tojson}}).slice(0,10),datasets:[{data:Object.values({{stats.by_city|tojson}}).slice(0,10),backgroundColor:'#667eea'}]}});
        function applyFilters() { const p=new URLSearchParams(); ['searchInput','sourceFilter','categoryFilter','cityFilter','qualityFilter'].forEach(id=>{const v=document.getElementById(id).value;if(v)p.set(id,v);});window.location.href='/?'+p.toString(); }
        function exportData(f) { window.location.href='/export/'+f; }
        function validateAll() {
            if(confirm('Start background validation?')) {
                fetch('/api/trigger/validate').then(r => r.json()).then(d => alert(d.message || d.error));
            }
        }
        function formatPhones() { if(confirm('Format all phones to +91?'))window.location.href='/admin/format-phones'; }
        function triggerScrape() {
            if(confirm('Trigger new background scrape?')) {
                fetch('/api/trigger/scrape').then(r => r.json()).then(d => alert(d.message || d.error));
            }
        }
    </script>
</body>
</html>
"""

@app.route('/')
async def index():
    pool = await get_db_pool()
    try:
        async with pool.acquire() as conn:
            contacts = await conn.fetch('SELECT * FROM contacts ORDER BY scraped_at DESC LIMIT 100')
            total = await conn.fetchval('SELECT COUNT(*) FROM contacts')
            with_phone = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE phone_clean IS NOT NULL")
            with_email = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL")
            validated = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE email_valid = true")
            
            sources = await conn.fetch('SELECT DISTINCT source FROM contacts')
            categories = await conn.fetch('SELECT DISTINCT category FROM contacts')
            cities = await conn.fetch('SELECT DISTINCT city FROM contacts')
            
            by_source = await conn.fetch('SELECT source, COUNT(*) as c FROM contacts GROUP BY source')
            by_category = await conn.fetch('SELECT category, COUNT(*) as c FROM contacts GROUP BY category')
            by_city = await conn.fetch('SELECT city, COUNT(*) as c FROM contacts GROUP BY city ORDER BY c DESC LIMIT 20')
            
            quality_high = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE (CASE WHEN phone_clean IS NOT NULL THEN 30 ELSE 0 END + CASE WHEN email IS NOT NULL AND email LIKE '%@%.%' THEN 30 ELSE 0 END + CASE WHEN address IS NOT NULL THEN 20 ELSE 0 END + CASE WHEN city IS NOT NULL THEN 10 ELSE 0 END + CASE WHEN area IS NOT NULL THEN 10 ELSE 0 END) >= 70")
            quality_mid = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE (CASE WHEN phone_clean IS NOT NULL THEN 30 ELSE 0 END + CASE WHEN email IS NOT NULL AND email LIKE '%@%.%' THEN 30 ELSE 0 END + CASE WHEN address IS NOT NULL THEN 20 ELSE 0 END + CASE WHEN city IS NOT NULL THEN 10 ELSE 0 END + CASE WHEN area IS NOT NULL THEN 10 ELSE 0 END) >= 40 AND (CASE WHEN phone_clean IS NOT NULL THEN 30 ELSE 0 END + CASE WHEN email IS NOT NULL AND email LIKE '%@%.%' THEN 30 ELSE 0 END + CASE WHEN address IS NOT NULL THEN 20 ELSE 0 END + CASE WHEN city IS NOT NULL THEN 10 ELSE 0 END + CASE WHEN area IS NOT NULL THEN 10 ELSE 0 END) < 70")
            quality_low = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE (CASE WHEN phone_clean IS NOT NULL THEN 30 ELSE 0 END + CASE WHEN email IS NOT NULL AND email LIKE '%@%.%' THEN 30 ELSE 0 END + CASE WHEN address IS NOT NULL THEN 20 ELSE 0 END + CASE WHEN city IS NOT NULL THEN 10 ELSE 0 END + CASE WHEN area IS NOT NULL THEN 10 ELSE 0 END) < 40")
            
            last_updated = await conn.fetchval("SELECT MAX(scraped_at) FROM contacts")
            
            for c in contacts:
                c['quality_score'] = calculate_quality_score(c)
    finally:
        await pool.close()
    
    avg_quality = round((quality_high * 80 + quality_mid * 50 + quality_low * 20) / total, 1) if total > 0 else 0
    
    return render_template_string(HTML_TEMPLATE, contacts=contacts,
        stats={'total': total, 'with_phone': with_phone, 'phone_pct': round(with_phone/total*100,1) if total else 0,
               'with_email': with_email, 'email_pct': round(with_email/total*100,1) if total else 0,
               'validated': validated, 'avg_quality': avg_quality, 'last_updated': last_updated.strftime('%Y-%m-%d %H:%M') if last_updated else 'N/A',
               'by_source': {r['source']: r['c'] for r in by_source}, 'by_category': {r['category']: r['c'] for r in by_category},
               'by_city': {r['city']: r['c'] for r in by_city}, 'quality_high': quality_high, 'quality_mid': quality_mid, 'quality_low': quality_low},
        sources=[s['source'] for s in sources], categories=[c['category'] for c in categories], cities=[c['city'] for c in cities])

@app.route('/health')
async def health_check():
    pool = await get_db_pool()
    try:
        async with pool.acquire() as conn:
            await conn.fetchval('SELECT 1')
        db_status = 'healthy'
    except Exception as e:
        db_status = f'unhealthy: {str(e)}'
    finally:
        await pool.close()
    
    return jsonify({
        'status': 'ok' if db_status == 'healthy' else 'error',
        'timestamp': datetime.now().isoformat(),
        'services': {'database': db_status}
    })

@app.route('/api/contacts')
@rate_limit
async def api_contacts():
    limit = min(request.args.get('limit', 100, type=int), 1000)
    offset = request.args.get('offset', 0, type=int)
    
    pool = await get_db_pool()
    try:
        async with pool.acquire() as conn:
            contacts = await conn.fetch(f'SELECT name, phone, email, city, category, source, scraped_at FROM contacts ORDER BY scraped_at DESC LIMIT {limit} OFFSET {offset}')
            total = await conn.fetchval('SELECT COUNT(*) FROM contacts')
    finally:
        await pool.close()
    
    return jsonify({'success': True, 'total': total, 'limit': limit, 'offset': offset, 'data': [dict(c) for c in contacts]})

@app.route('/api/stats')
@rate_limit
async def api_stats():
    pool = await get_db_pool()
    try:
        async with pool.acquire() as conn:
            by_source = await conn.fetch('SELECT source, COUNT(*) as c FROM contacts GROUP BY source')
            by_category = await conn.fetch('SELECT category, COUNT(*) as c FROM contacts GROUP BY category')
            by_city = await conn.fetch('SELECT city, COUNT(*) as c FROM contacts GROUP BY city ORDER BY c DESC LIMIT 10')
            total = await conn.fetchval('SELECT COUNT(*) FROM contacts')
            with_phone = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE phone_clean IS NOT NULL")
            with_email = await conn.fetchval("SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL")
    finally:
        await pool.close()
    
    return jsonify({'total': total, 'with_phone': with_phone, 'with_email': with_email,
        'by_source': {r['source']: r['c'] for r in by_source},
        'by_category': {r['category']: r['c'] for r in by_category},
        'by_city': {r['city']: r['c'] for r in by_city}})

@app.route('/export/<fmt>')
@rate_limit
async def export_data(fmt):
    pool = await get_db_pool()
    try:
        async with pool.acquire() as conn:
            contacts = await conn.fetch('SELECT * FROM contacts ORDER BY scraped_at DESC')
    finally:
        await pool.close()
    
    if fmt == 'csv':
        import csv
        output = io.StringIO()
        if contacts:
            w = csv.DictWriter(output, fieldnames=contacts[0].keys())
            w.writeheader()
            for r in contacts: w.writerow(dict(r))
        return Response(output.getvalue(), mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename=contacts.csv'})
    
    if fmt == 'json':
        return jsonify({'success': True, 'total': len(contacts), 'data': [dict(c) for c in contacts]})
    
    if fmt == 'excel':
        wb = Workbook()
        ws = wb.active
        ws.title = "Contacts"
        if contacts:
            ws.append(list(contacts[0].keys()))
            for r in contacts: ws.append(list(r.values()))
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        return send_file(output, download_name='contacts.xlsx', as_attachment=True)
    
    return jsonify({'error': 'Invalid format'}), 400

@app.route('/admin/validate')
async def validate_contacts():
    pool = await get_db_pool()
    updated = 0
    try:
        async with pool.acquire() as conn:
            contacts = await conn.fetch('SELECT id, phone, email FROM contacts')
            for c in contacts:
                p_valid = validate_phone(c['phone']) if c['phone'] else False
                e_valid = validate_email(c['email']) if c['email'] else False
                await conn.execute('UPDATE contacts SET phone_clean = CASE WHEN $2 THEN (REGEXP_REPLACE($1, $9, $10, $11))[LENGTH(REGEXP_REPLACE($1, $9, $10, $11))-9:] ELSE NULL END, email_valid = $3 WHERE id = $4', c['phone'], p_valid, e_valid, c['id'], r'[^0-9]', '', 'g')
                updated += 1
        logger.info(f"Validated {updated} contacts")
    finally:
        await pool.close()
    return jsonify({'success': True, 'validated': updated})

@app.route('/admin/format-phones')
async def format_phones():
    pool = await get_db_pool()
    updated = 0
    try:
        async with pool.acquire() as conn:
            contacts = await conn.fetch("SELECT id, phone FROM contacts WHERE phone IS NOT NULL AND phone != ''")
            for c in contacts:
                formatted = format_phone(c['phone'])
                if formatted:
                    await conn.execute('UPDATE contacts SET phone = $1 WHERE id = $2', formatted, c['id'])
                    updated += 1
        logger.info(f"Formatted {updated} phone numbers")
    finally:
        await pool.close()
    return jsonify({'success': True, 'formatted': updated})

@app.route('/api/trigger/scrape')
async def trigger_scrape():
    try:
        from tasks import scrape_category_task
        config = load_config()
        for city in config.get('cities', []):
            for cat in config.get('categories', []):
                scrape_category_task.delay(city, cat)
        return jsonify({'success': True, 'message': f"Scraping tasks for {len(config.get('cities', [])) * len(config.get('categories', []))} combinations added to queue"})
    except Exception as e:
        logger.error(f"Trigger scrape failed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/trigger/validate')
async def trigger_validate():
    try:
        from tasks import validate_all_contacts_task
        validate_all_contacts_task.delay()
        return jsonify({'success': True, 'message': 'Validation task added to queue'})
    except Exception as e:
        logger.error(f"Trigger validate failed: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting enhanced dashboard on port {port}")
    app.run(debug=True, host='0.0.0.0', port=port)