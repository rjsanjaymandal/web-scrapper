FROM python:3.12-slim

# Force unbuffered logs to avoid "silent" failures in Docker
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Minimal system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Create a non-root user
# Note: In Railway, the PORT environment variable is injected.
# We bind to 0.0.0.0:$PORT so the service is reachable.
RUN useradd -m scraper
COPY . .
RUN chown -R scraper:scraper /app

# Switch to non-root user
USER scraper

# Use entrypoint.py as the entrypoint
CMD ["python3", "entrypoint.py"]

