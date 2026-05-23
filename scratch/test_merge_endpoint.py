import urllib.request, json, sys, time

time.sleep(10)
url = "https://lead-engine.railway.app/api/merge-local"

# Ping health first
try:
    resp = urllib.request.urlopen("https://lead-engine.railway.app/health", timeout=10)
    print(f"Health: {resp.status} - {resp.read().decode()}")
except Exception as e:
    print(f"Health error: {e}")

# Test merge endpoint
data = json.dumps({"contacts": []}).encode()
req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
try:
    resp = urllib.request.urlopen(req, timeout=15)
    print(f"Merge: {resp.status} - {resp.read().decode()}")
except urllib.error.HTTPError as e:
    print(f"Merge HTTP {e.code}: {e.read().decode()[:300]}")
except Exception as e:
    print(f"Merge error: {e}")
