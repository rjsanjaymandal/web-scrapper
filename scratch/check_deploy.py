import urllib.request, sys

# Check health
resp = urllib.request.urlopen("https://lead-engine.railway.app/health", timeout=10)
body = resp.read().decode()
print(f"Health ({resp.status}): {body}")

# Check /up
resp = urllib.request.urlopen("https://lead-engine.railway.app/up", timeout=10)
body = resp.read().decode()
print(f"Up ({resp.status}): {body}")

# Check if merge endpoint exists (empty POST)
data = b'{"contacts":[]}'
req = urllib.request.Request(
    "https://lead-engine.railway.app/api/merge-local",
    data=data, headers={"Content-Type": "application/json"},
    method="POST"
)
try:
    resp = urllib.request.urlopen(req, timeout=15)
    print(f"Merge endpoint ({resp.status}): {resp.read().decode()}")
except urllib.error.HTTPError as e:
    print(f"Merge endpoint HTTP {e.code}: {e.read().decode()[:500]}")
except Exception as e:
    print(f"Merge endpoint error: {e}")
