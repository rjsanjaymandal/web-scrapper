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

# Get Query type fields
query = """
{
  __type(name: "Query") {
    name
    fields {
      name
      args {
        name
        type {
          name
          kind
        }
      }
    }
  }
}
"""
result = graphql(query)
fields = result.get("data", {}).get("__type", {}).get("fields", [])
print("=== Query fields ===")
for f in fields:
    args_str = ", ".join([a["name"] for a in f.get("args", [])])
    print(f"  {f['name']}({args_str})")

# Try projects query with pagination
query2 = """
{
  projects {
    edges {
      node {
        id
        name
      }
    }
  }
}
"""
print("\n=== Try projects query ===")
result = graphql(query2)
if "errors" in result:
    print(f"Errors: {result['errors']}")
else:
    print(json.dumps(result, indent=2)[:1000])
