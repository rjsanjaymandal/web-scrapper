web: gunicorn -w 1 -k uvicorn.workers.UvicornWorker dashboard:app --bind 0.0.0.0:$PORT
worker: celery -A tasks.celery_app worker --loglevel=info
