import urllib.request, json

API = "https://api.railway.app/graphql/v2"
headers = {"Content-Type": "application/json"}

def graphql(query, vars=None):
    payload = {"query": query}
    if vars:
        payload["variables"] = vars
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

# Try githubRepo - might not need auth for public repos
q1 = """
{
  githubRepo(fullRepoName: "rjsanjaymandal/web-scrapper") {
    owner
    repo
    fullRepoName
  }
}
"""
r1 = graphql(q1)
print("=== githubRepo ===")
print(json.dumps(r1, indent=2)[:300])

# Try serviceDomainAvailable for our domain
q2 = """
{
  serviceDomainAvailable(domain: "lead-engine") {
    available
    domain
  }
}
"""
r2 = graphql(q2)
print("\n=== serviceDomainAvailable ===")
print(json.dumps(r2, indent=2)[:500])

# Try publicStats
q3 = "{ publicStats { totalProjects totalDeployments } }"
r3 = graphql(q3)
print("\n=== publicStats ===")
print(json.dumps(r3, indent=2)[:300])
