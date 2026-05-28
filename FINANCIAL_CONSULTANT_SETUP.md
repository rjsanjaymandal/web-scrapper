# Financial Consultant Data Scraper - Setup & Usage

## 🎯 Overview

The system has been **completely transitioned from school scraping to financial consultant data extraction**.

### What's Being Scraped

**108 Professional & Financial Consultant Categories:**

- Tax & Accounting: Accountant, Tax Practitioner, Tax Consultant, CA Office, Audit Firm, etc.
- Financial Services: Financial Advisor, Investment Consultant, Wealth Management, NPS Consultant, etc.
- Business Consulting: Business Consultant, Startup Consultant, MSME Consultant, ISO Consultant, etc.
- Compliance & Legal: GST Consultant, ROC Consultant, Company Registration, Trademark Consultant, etc.
- Digital Services: CSC Center, E-Mitra, Digital Services Center, Online Service Center, etc.
- Loan & Banking: Loan Consultant, Banking Consultant, Business Loan Consultant, etc.

See `config.yaml` for the complete list.

## 🚀 Quick Start

### Option 1: Direct Python Execution (Recommended)

```bash
python start_financial_scraper.py
```

This will:
1. Load all 108 financial consultant categories from `config.yaml`
2. Shuffle through 135+ Indian cities
3. Continuously scrape and deduplicate data
4. Save qualified leads to the database

### Option 2: Docker Deployment

```bash
# Set environment variable for financial automator
export PROCESS_TYPE=financial

# Run via Docker Compose
docker-compose up
```

### Option 3: Manual Execution

```bash
python automate_financial_consultants.py
```

## 📊 Configuration

Edit `config.yaml` to customize:

```yaml
cities:
  - "Mumbai"
  - "Delhi"
  - "Bangalore"
  # ... 130+ more cities

categories:
  - "Accountant"
  - "Tax Consultant"
  # ... 105+ more categories

scraper_settings:
  request_delay_min: 8          # Min delay between requests (seconds)
  request_delay_max: 25         # Max delay between requests (seconds)
  max_retries: 5                # Retry failed requests
  timeout: 60                   # Request timeout (seconds)
  max_concurrent: 1             # Concurrent requests (keep at 1 for politeness)
  max_pages_per_source: 3       # Max pages per search
```

## 🔄 How It Works

### Architecture

```
Financial Consultant Automator (Infinite Loop)
├── City Shuffling (135+ cities)
│   ├── Category Shuffling (108 categories)
│   │   ├── Direct Scraping (JustDial, TradeIndia, IndiaMART, Maps)
│   │   ├── Search Engine Scraping (Bing, DuckDuckGo, Yahoo)
│   │   ├── Contact Extraction & Validation
│   │   ├── O(1) Deduplication
│   │   └── Save to PostgreSQL
│   │   └── (8-15s polite delay)
│   └── Next Category
└── Next Cycle (60s wait between full rotations)
```

### Data Sources

1. **Direct Scraping**
   - JustDial (Category listings)
   - TradeIndia (B2B database)
   - IndiaMART (B2B suppliers)
   - Google Maps (Local business listings)

2. **Search Engines**
   - Bing Search
   - DuckDuckGo
   - Yahoo Search

3. **Extraction**
   - Company names
   - Phone numbers
   - Email addresses
   - Websites
   - Addresses
   - Service descriptions

### Deduplication Strategy

- **O(1) Lookup**: Hash-based deduplication in database
- **Multi-field Matching**: Phone + Email + Company Name
- **Fuzzy Matching**: Similar entries are merged
- **Timestamp Tracking**: Prevents re-processing

## 📈 Expected Output

### Cycle Statistics

Each cycle will show:
```
CYCLE #1
Loaded 135 cities and 108 financial consultant categories
Total leads saved so far: 0
Start time: 2026-05-28 14:11:09

[1/135] [1/108] Starting scrape: Accountant in Mumbai
  → Direct stealth scraping Accountant in Mumbai...
  → Got 47 contacts from direct scraping
  → Search engine scraping Accountant in Mumbai...
  → Got 23 contacts from search engines
  → Processing 70 raw contacts (deduplication + validation)...
  ✓ Saved 56 qualified contacts to database
  ✓ Running total: 56 leads
  → Waiting 11.3s before next request...

[2/135] [2/108] Starting scrape: Tax Practitioner in Mumbai
  ...
```

## 💾 Database Schema

All leads are stored with:
- Unique ID (MD5 hash of phone + email + company)
- Company Name
- Contact Person
- Phone Numbers (Primary + Secondary)
- Email Addresses
- Website URL
- Physical Address
- City
- Category
- Extraction Timestamp
- Last Updated
- Quality Score

## ⚙️ Automation in Production

### Docker Environment Variables

```bash
PROCESS_TYPE=financial              # Run financial automator
DATABASE_URL=postgresql://...       # Database connection
PORT=8080                           # Dashboard port
```

### Systemd Service (Linux)

```ini
[Unit]
Description=Financial Consultant Web Scraper
After=postgresql.service

[Service]
Type=simple
User=scraper
WorkingDirectory=/opt/financial-scraper
Environment="PROCESS_TYPE=financial"
ExecStart=/usr/bin/python3 start_financial_scraper.py
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

### Windows Task Scheduler

Create a scheduled task:
```
Program: python.exe
Arguments: C:\path\to\start_financial_scraper.py
Schedule: At startup, then repeat every 24 hours
```

## 📊 Monitoring

### Dashboard

The system includes a web dashboard at `http://localhost:8080` showing:
- Leads scraped by category
- Geographic distribution
- Data quality metrics
- Scraping rate (leads/hour)
- Error logs

### Command Line Monitoring

```bash
# Check scraper status
ps aux | grep automate_financial

# View recent logs
tail -f logs/scraper.log

# Database query - leads by category
psql -c "SELECT category, COUNT(*) FROM contacts GROUP BY category ORDER BY COUNT(*) DESC;"

# Database query - leads by city
psql -c "SELECT city, COUNT(*) FROM contacts GROUP BY city ORDER BY COUNT(*) DESC LIMIT 20;"
```

## 🛑 Stopping School Scraping

✅ **School scraping has been disabled:**
- `automate_schools.py` is no longer called
- `config.yaml` categories changed from `["schools"]` to 108 financial categories
- `entrypoint.py` rejects `schools` or `school_automator` process types

## ⚠️ Important Notes

1. **Polite Crawling**: Respects robots.txt, uses 8-25s delays between requests
2. **Rate Limiting**: Max 1 concurrent request to avoid IP blocks
3. **Error Handling**: Failed requests are retried up to 5 times
4. **De-duplication**: Automatic O(1) duplicate detection
5. **Data Quality**: Only saves contacts with valid phone/email/address

## 🔧 Troubleshooting

### Issue: "Connection refused to database"
```bash
# Ensure PostgreSQL is running
psql -U postgres -h localhost -d scraper_db
```

### Issue: "Too many requests / 429 error"
Increase delays in `config.yaml`:
```yaml
request_delay_min: 15
request_delay_max: 45
```

### Issue: "Scraper is slow"
Reduce retries or increase timeout:
```yaml
max_retries: 3
timeout: 30
```

## 📝 Categories Being Scraped

All 108 categories in `config.yaml`:
```
Accountant, Tax Practitioner, Tax Consultant, Tax Advocate, GST Consultant,
Income Tax Consultant, Insurance Advisor, Insurance Agent, LIC Agent,
Mutual Fund Advisor, Mutual Fund Distributor, Financial Advisor,
Investment Consultant, Cyber Cafe, Online Service Center, E Mitra, CSC Center,
Documentation Center, Loan Consultant, Finance Consultant, ITR Filing Consultant,
GST Return Filing, Accounting Services, Bookkeeping Services, Audit Firm,
Payroll Services, TDS Consultant, ROC Consultant, Company Registration Consultant,
MSME Consultant, Startup Consultant, Business Consultant, ISO Consultant,
Trademark Consultant, PF ESI Consultant, Import Export Consultant,
Digital Signature Provider, PAN Card Center, Aadhaar Update Center,
Passport Consultant, Visa Consultant, Property Consultant, Real Estate Consultant,
Loan Agent, Banking Consultant, Tax Filing Services, GST Registration Services,
Income Tax Return Services, Virtual Accountant, Accounting Firm, CA Office,
Taxation Services, Financial Planning Services, Investment Advisory Services,
Insurance Consultancy, Wealth Management Advisor, Share Market Advisor,
Stock Market Consultant, NPS Consultant, Retirement Consultant,
Business Loan Consultant, Personal Loan Consultant, Home Loan Consultant,
Vehicle Loan Consultant, Micro Finance Consultant, Legal Documentation Services,
Affidavit Services, Notary Services, Typing Center, Photocopy Center,
Online Form Center, Digital Services Center, E Governance Center,
Jan Seva Kendra, Suvidha Kendra, Computer Center, Internet Cafe,
Online Exam Center, GST Suvidha Provider, Billing Software Provider,
Accounting Software Services, Payroll Processing Services,
Audit & Assurance Services, Internal Auditor, Forensic Audit Consultant,
Compliance Consultant, Business Setup Consultant, NGO Consultant,
Society Registration Consultant, Trust Registration Consultant,
Partnership Firm Registration, LLP Registration Consultant,
Private Limited Registration, Trademark Filing Consultant, Copyright Consultant,
Patent Consultant, IEC Registration Consultant, FSSAI Consultant,
Labour Law Consultant, Professional Tax Consultant, Factory License Consultant,
Pollution NOC Consultant, Tender Consultant, Project Finance Consultant,
Subsidy Consultant, Government Scheme Consultant
```

---

**Status**: ✅ School scraping DISABLED | ✅ Financial consultant scraping ENABLED
**Last Updated**: 2026-05-28
