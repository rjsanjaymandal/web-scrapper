import urllib.request, urllib.error, json, time

# Wait 10s
time.sleep(10)

# Test merge endpoint
data = json.dumps({"contacts": []}).encode()
req = urllib.request.Request(
    "https://lead-engine.railway.app/api/merge-local",
    data=data, headers={"Content-Type": "application/json"},
    method="POST"
)
try:
    resp = urllib.request.urlopen(req, timeout=15)
    print(f"DEPLOYED - Merge endpoint: {resp.status} - {resp.read().decode()}")
except urllib.error.HTTPError as e:
    body = e.read().decode()[:500]
    print(f"NOT DEPLOYED - HTTP {e.code}: {body}")
except Exception as e:
    print(f"Error: {e}")
