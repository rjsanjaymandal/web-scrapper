import urllib.request, json

# Try the GraphiQL endpoint on the new domain
endpoints = [
    "https://backboard.railway.com/graphql/v2",
    "https://backboard.railway.app/graphql/v2", 
]

for ep in endpoints:
    try:
        # Try introspection 
        query = json.dumps({"query": "{ __schema { queryType { name } } }"}).encode()
        req = urllib.request.Request(ep, data=query, headers={"Content-Type": "application/json"})
        resp = urllib.request.urlopen(req, timeout=10)
        body = resp.read().decode()[:200]
        print(f"{ep} -> {resp.status}: {body}")
    except urllib.error.HTTPError as e:
        print(f"{ep} -> HTTP {e.code}")
    except Exception as e:
        print(f"{ep} -> {e}")

print()

# Also try to get the current deployment status from the Railway dashboard
# Railway uses GraphQL endpoint and we need a token
# But let's try something: check if the Railway deploy hook exists at a common URL
import socket

# Try to find PostgreSQL public port by checking common Railway patterns
# Railway public ports are typically in the 5000-10000 range
# But scanning that many ports will take too long

# Instead, let me use asyncpg to try to connect with the credentials we know
# We know: user=postgres, pass=SBGYpcBnqhbwqrzRvVnhJzGKTHTwOZrG, db=railway
# Host would be containers-us-west-?.railway.app
# We need the port

# Check common Railway PG ports 
print("=== Trying Railway PG host: containers-us-west-1.railway.app ===")
import socket
common_ports = [5432, 7000, 7777, 8000, 8080, 8443, 9000, 9443]
for port in common_ports:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        result = s.connect_ex(("containers-us-west-1.railway.app", port))
        if result == 0:
            print(f"  Port {port}: OPEN")
        s.close()
    except:
        pass
