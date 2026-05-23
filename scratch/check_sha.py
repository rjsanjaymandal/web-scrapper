import urllib.request, subprocess, json, time

# Wait a bit
time.sleep(5)

# Get local git SHA
local_sha = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
print(f"Local commit: {local_sha}")

# Try to get deployed SHA from Railway (check if there's a version header or endpoint)
urls = [
    "https://lead-engine.railway.app/health",
    "https://lead-engine.railway.app/api/status",
    "https://lead-engine.railway.app/",
]
for url in urls[:1]:
    try:
        req = urllib.request.Request(url, method="GET")
        resp = urllib.request.urlopen(req, timeout=10)
        print(f"{url} -> {resp.status}")
        # Check headers
        for h, v in resp.headers.items():
            if "version" in h.lower() or "sha" in h.lower() or "commit" in h.lower():
                print(f"  {h}: {v}")
    except Exception as e:
        print(f"{url} -> {e}")

print(f"\nPush was made at {local_sha[:8]}")
print("Waiting for Railway auto-deploy... this can take 2-5 minutes.")
