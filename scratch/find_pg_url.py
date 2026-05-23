import urllib.request, urllib.error, json, socket

# Try different endpoints that might leak DB info
endpoints = [
    "/health",
    "/up",
    "/api/status",
    "/error",
    "/debug",
    "/api/debug",
    "/api/db",
    "/api/config",
]

for ep in endpoints:
    try:
        url = f"https://lead-engine.railway.app{ep}"
        req = urllib.request.Request(url, method="GET")
        resp = urllib.request.urlopen(req, timeout=10)
        body = resp.read().decode("utf-8", errors="replace")[:500]
        print(f"{url} -> {resp.status}")
        print(f"  {body[:200]}")
        print()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:200]
        if e.code != 404:
            print(f"{url} -> {e.code}: {body}")
            print()
    except Exception as e:
        pass

# Try to guess Railway PG public hostname
# Railway uses patterns like: containers-us-west-xx.railway.app
# Let's try to resolve common patterns
hostnames = [
    "containers-us-west-1.railway.app",
    "containers-us-west-2.railway.app", 
    "containers-us-west-3.railway.app",
    "containers-us-west-4.railway.app",
    "containers-us-east-1.railway.app",
    "containers-us-east-2.railway.app",
    "containers-us-east-3.railway.app",
    "containers-us-east-4.railway.app",
]
print("=== Trying to find Railway PG hostname ===")
for h in hostnames:
    try:
        ip = socket.gethostbyname(h)
        print(f"{h} -> {ip}")
    except:
        pass
