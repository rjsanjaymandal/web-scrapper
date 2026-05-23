import urllib.request, urllib.error, json

# Test merge endpoint (empty contacts - expects 400 but confirms endpoint exists)
data = json.dumps({"contacts": []}).encode()
req = urllib.request.Request(
    "https://lead-engine.railway.app/api/merge-local",
    data=data, headers={"Content-Type": "application/json"},
    method="POST"
)
try:
    resp = urllib.request.urlopen(req, timeout=15)
    print(f"DEPLOYED! Merge endpoint: {resp.status} - {resp.read().decode()}")
except urllib.error.HTTPError as e:
    body = e.read().decode()[:500]
    print(f"Endpoint status: HTTP {e.code}")
    print(f"Body: {body}")
    # If we get 400 (no contacts) instead of 404, it's deployed!
    if e.code == 400:
        print(">>> DEPLOYED! Endpoint exists and works.")
    else:
        print(">>> Not yet deployed (404).")
except Exception as e:
    print(f"Error: {e}")
