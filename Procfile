web: gunicorn dashboard:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120
worker: celery -A tasks.celery_app worker --loglevel=info --pool=solo --concurrency=1
