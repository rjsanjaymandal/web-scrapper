import urllib.request, json

API = "https://api.railway.app/graphql/v2"
headers = {"Content-Type": "application/json"}

def graphql(query, variables=None):
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    data = json.dumps(payload).encode()
    req = urllib.request.Request(API, data=data, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:500]
        return {"error": f"HTTP {e.code}: {body}"}
    except Exception as e:
        return {"error": str(e)}

# Try to query projects (may require auth)
query1 = """
{
  projects {
    id
    name
    description
  }
}
"""
print("=== Projects (may need auth) ===")
result = graphql(query1)
print(json.dumps(result, indent=2)[:500])

# Try to find project by the app domain
# Railway injects RAILWAY_PROJECT_NAME and RAILWAY_PROJECT_ID
# Let's try to query a specific service by its domain
query2 = """
{
  projectByDomain(domain: "lead-engine") {
    id
    name
    services {
      id
      name
    }
  }
}
"""
print("\n=== Project by domain ===")
result = graphql(query2)
print(json.dumps(result, indent=2)[:1000])
