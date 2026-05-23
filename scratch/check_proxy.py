import urllib.request, urllib.error, json

# Check if we can access PG through the app's domain on a different port
# Railway public network for PG typically uses the same domain with a different port
# Or we can try to find the project details from the app

# First, check response headers from the main app
req = urllib.request.Request("https://lead-engine.railway.app/health", method="GET")
resp = urllib.request.urlopen(req, timeout=10)
print("Headers:")
for h, v in resp.headers.items():
    if h.lower() not in ("content-length", "content-type", "date", "server"):
        print(f"  {h}: {v}")
print(f"\nBody: {resp.read().decode()}")
print()

# Try the Railway metadata endpoints (some Railway apps expose these)
metadata_urls = [
    "http://localhost:8080/__metadata",  # inside container
    "https://lead-engine.railway.app/.railway/metadata",
    "https://lead-engine.railway.app/__env",
]
# These probably won't work from outside, but let's check
for url in metadata_urls[1:]:
    try:
        req = urllib.request.Request(url, method="GET")
        resp = urllib.request.urlopen(req, timeout=5)
        print(f"{url} -> {resp.status}: {resp.read().decode()[:300]}")
    except urllib.error.HTTPError as e:
        print(f"{url} -> HTTP {e.code}")
    except Exception as e:
        print(f"{url} -> {e}")
