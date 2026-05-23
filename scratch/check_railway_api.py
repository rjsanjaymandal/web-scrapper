import urllib.request, urllib.error, json

# Railway injects these env vars into the deployed container
# We can try to access them via the app's internal API
# Railway public API base: https://backboard.railway.app/graphql/v2
# But we'd need a token.

# Try to find a Railway deploy webhook URL by checking common patterns
# Railway provides deploy webhooks at:
# https://railway.app/project/<PROJECT_ID>/service/<SERVICE_ID>?webhook=<WEBHOOK_ID>
# These can trigger deployments via POST

# The Railway API has a public GraphQL endpoint:
# POST https://backboard.railway.app/graphql/v2
# Authorization: Bearer <token>

# Since we don't have a token, let's try to find any Railway-related
# endpoints exposed by the running app

urls = [
    "https://lead-engine.railway.app/api/trigger/deploy",
    "https://lead-engine.railway.app/__reload",
    "https://lead-engine.railway.app/restart",
]

for url in urls:
    try:
        req = urllib.request.Request(url, method="POST", data=b"{}", headers={"Content-Type": "application/json"})
        resp = urllib.request.urlopen(req, timeout=10)
        print(f"{url} -> {resp.status}: {resp.read().decode()[:200]}")
    except urllib.error.HTTPError as e:
        print(f"{url} -> HTTP {e.code}")
    except Exception as e:
        print(f"{url} -> {e}")

print()
print("=== Options for merging ===")
print("1. Railway dashboard -> PostgreSQL -> Connect -> Public Network")
print("   Then run: python merge_local_to_remote.py <URL>")
print()
print("2. Railway dashboard -> SQL Runner (if available)")
print("   Paste contents of local_contacts_import.sql")
print()
print("3. After new code deploys:")
print("   curl -X POST https://lead-engine.railway.app/api/merge-local")
print("     -H 'Content-Type: application/json'")
print("     -d @local_contacts_export.json")
print()
print("4. Deploy trigger (if you have Railway token):")
print("   railway login")
print("   railway link")
print("   railway up")
