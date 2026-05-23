import urllib.request, json

url = "https://api.github.com/repos/rjsanjaymandal/web-scrapper"
try:
    req = urllib.request.Request(url, method="GET")
    req.add_header("Accept", "application/vnd.github.v3+json")
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read())
    print(f"Repo: {data.get('full_name')}")
    print(f"Private: {data.get('private')}")
except urllib.error.HTTPError as e:
    print(f"GitHub API error: {e.code}")
except Exception as e:
    print(f"Error: {e}")

# Try hooks - likely needs auth
url2 = "https://api.github.com/repos/rjsanjaymandal/web-scrapper/hooks"
try:
    req = urllib.request.Request(url2, method="GET")
    req.add_header("Accept", "application/vnd.github.v3+json")
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read())
    print(f"Found {len(data)} hooks")
    for h in data:
        print(f"  Name: {h.get('name')}")
        print(f"  URL: {h.get('config', {}).get('url', 'N/A')}")
        print(f"  Active: {h.get('active')}")
except urllib.error.HTTPError as e:
    print(f"Hooks API: HTTP {e.code}")
    body = e.read().decode()[:300]
    print(f"  {body}")
except Exception as e:
    print(f"Error: {e}")
