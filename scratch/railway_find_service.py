import urllib.request, json

API = "https://api.railway.app/graphql/v2"
headers = {"Content-Type": "application/json"}

def graphql(query):
    data = json.dumps({"query": query}).encode()
    req = urllib.request.Request(API, data=data, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:500]
        return {"error": f"HTTP {e.code}: {body}"}
    except Exception as e:
        return {"error": str(e)}

# Get DomainAvailable type fields
q1 = """
{
  __type(name: "DomainAvailable") {
    fields { name }
  }
}
"""
r1 = graphql(q1)
print("DomainAvailable fields:")
for f in r1.get("data", {}).get("__type", {}).get("fields", []):
    print(f"  {f['name']}")

# Get tcpProxy types
q2 = """
{
  __type(name: "TcpProxy") {
    fields { name type { name kind ofType { name } } }
  }
}
"""
r2 = graphql(q2)
print("\nTcpProxy fields:")
for f in r2.get("data", {}).get("__type", {}).get("fields", []):
    print(f"  {f['name']}: {f['type']['name']}")

# Try the correct domain query
q3 = """
{
  serviceDomainAvailable(domain: "lead-engine") {
    available
  }
}
"""
r3 = graphql(q3)
print("\nserviceDomainAvailable:")
print(json.dumps(r3, indent=2)[:300])

# Try to get tcpProxies - needs environmentId and serviceId
# But maybe we can find them through the domain
q4 = "{ __type(name: \"TcpProxyConnection\") { fields { name } } }"
r4 = graphql(q4)
print("\nTcpProxyConnection fields:")
for f in r4.get("data", {}).get("__type", {}).get("fields", []):
    print(f"  {f['name']}")

q5 = "{ __type(name: \"Query\") { fields { name args { name type { name kind } } } } }"
r5 = graphql(q5)
print("\nAll Query fields with args:")
for f in r5.get("data", {}).get("__type", {}).get("fields", []):
    if f.get("args"):
        args_info = [(a["name"], a["type"]["name"]) for a in f["args"]]
        print(f"  {f['name']}({args_info})")
