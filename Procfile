web: gunicorn dashboard:app --bind 0.0.0.0:8080
worker: celery -A tasks.celery_app worker --loglevel=info
