# Financial Services Contact Scraper

Advanced web scraping tool to extract contact data from Just Dial, IndiaMart, ICICI and similar directories.

## Features

### Core
- Multiple source support (JustDial, IndiaMart, ICICI)
- PostgreSQL database storage
- CSV, JSON, Excel export
- Email extraction from detail pages
- Duplicate detection and removal
- Proxy rotation with multiple proxies

### Data Quality
- Phone validation and formatting (+91)
- Email validation
- Quality scoring system
- Data enrichment

### Dashboard
- Interactive web UI with charts
- REST API endpoints
- Rate limiting
- Health check endpoint
- Filter and search

### DevOps
- Docker support
- Celery background jobs
- Redis queue integration
- Comprehensive logging

## Quick Start

```bash
pip install -r requirements.txt
playwright install chromium
createdb scraper_db
python scraper.py
python dashboard.py
```

## Configuration

```yaml
database:
  host: localhost
  port: 5432
  name: scraper_db
  user: postgres
  password: your_password

proxy:
  proxies:
    - host: proxy1
      username: user1
      password: pass1

scraper:
  test_mode: false
  enable_deduplication: true
  enable_email_extraction: true
```

## Docker

```bash
docker-compose up --build
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Dashboard |
| `GET /health` | Health check |
| `GET /api/contacts` | List contacts |
| `GET /api/stats` | Get statistics |
| `GET /export/csv` | Export CSV |
| `GET /export/json` | Export JSON |
| `GET /export/excel` | Export Excel |
| `GET /admin/validate` | Validate all contacts |
| `GET /admin/format-phones` | Format phones to +91 |

## Quality Score

- Phone: +30
- Email: +30
- Address: +20
- City: +10
- Area: +10
