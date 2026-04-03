FROM python:3.12-slim

# Install system dependencies required by Playwright's Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libdbus-1-3 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libatspi2.0-0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 \
    libcairo2 libasound2 libwayland-client0 \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Ensure Playwright installs browsers to a fixed location that everyone can access
ENV PLAYWRIGHT_BROWSERS_PATH=/app/pw-browsers

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Force Playwright install to the specific path
RUN playwright install chromium

# Create a non-root user
RUN useradd -m scraper
COPY . .
RUN chown -R scraper:scraper /app

# Switch to non-root user
USER scraper

# Default port
EXPOSE 8080

# Start the web server by default (Railway can override this for the worker service)
CMD ["gunicorn", "dashboard:app", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "120"]
