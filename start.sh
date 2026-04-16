#!/bin/bash
set -e

# Detect Process Type (Default: web)
TYPE=${PROCESS_TYPE:-web}

echo "🚀 [BOOTSTRAP] Starting $TYPE Deployment..."

# Ensure current directory is in PYTHONPATH for dashboard/tasks import
export PYTHONPATH=$PYTHONPATH:.

# 1. Wait for Database Connectivity (Universal for both Web and Worker)
echo "⏳ [BOOTSTRAP] Waiting for database connectivity..."
python3 -c "
import os
import time
import psycopg2
from urllib.parse import urlparse

def wait_for_db():
    db_url = os.environ.get('DATABASE_URL')
    if db_url:
        if db_url.startswith('postgres://'):
            db_url = db_url.replace('postgres://', 'postgresql://', 1)
        url = urlparse(db_url)
        host, port = url.hostname, url.port or 5432
    else:
        host = os.environ.get('DATABASE_HOST', 'localhost')
        port = int(os.environ.get('DATABASE_PORT', 5432))
    
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
            print(f'✅ [BOOTSTRAP] Database at {host}:{port} is reachable!')
            return True
        except Exception as e:
            print(f'⏳ [BOOTSTRAP] Still waiting for database... ({e})')
            time.sleep(1)
    print('❌ [BOOTSTRAP] Timeout waiting for database after 90s')
    return False

if not wait_for_db():
    exit(1)
"

# 2. Start logic based on PROCESS_TYPE
if [ "$TYPE" = "worker" ]; then
    echo "👷 [BOOTSTRAP] Initializing Celery Worker..."
    # Execute Celery (Note: tasks.celery_app matches your Procfile)
    exec celery -A tasks.celery_app worker \
        --loglevel=info \
        --pool=solo \
        --concurrency=1

elif [ "$TYPE" = "web" ]; then
    echo "📂 [BOOTSTRAP] Running eager database initialization..."
    python3 -c "
import os
import sys
import dashboard
with dashboard.app.app_context():
    success = dashboard.init_tables()
    if not success:
        print('❌ [BOOTSTRAP] Database initialization failed')
        sys.exit(1)
    print('✅ [BOOTSTRAP] Database tables ready!')
"
    
    PORT=${PORT:-8080}
    echo "🌐 [BOOTSTRAP] Starting Gunicorn on 0.0.0.0:$PORT"
    echo "🔍 [BOOTSTRAP] Health Check Path: /health"

    exec gunicorn dashboard:app \
        --bind 0.0.0.0:$PORT \
        --workers 1 \
        --threads 4 \
        --timeout 120 \
        --preload \
        --access-logfile - \
        --error-logfile -

else
    echo "❌ [BOOTSTRAP] Unknown PROCESS_TYPE: $TYPE"
    exit 1
fi