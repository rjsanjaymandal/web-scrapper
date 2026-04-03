FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    curl \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install playwright && playwright install chromium --with-deps

COPY . .

RUN mkdir -p exports logs

ENV PYTHONUNBUFFERED=1
ENV FLASK_APP=dashboard.py

EXPOSE 5000

CMD ["python", "dashboard.py"]