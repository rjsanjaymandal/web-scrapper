web: gunicorn dashboard:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120 --access-logfile - --error-logfile -
worker: celery -A tasks.celery_app worker --loglevel=info --pool=solo --concurrency=1
