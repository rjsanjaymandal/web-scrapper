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
        return {"error": f"HTTP {e.code}"}
    except Exception as e:
        return {"error": str(e)}

# Try githubRepo with correct field structure  
q1 = """
{
  __type(name: "GitHubRepoWithoutInstallation") {
    fields { name }
  }
}
"""
r1 = graphql(q1)
print("GitHubRepoWithoutInstallation fields:")
for f in r1.get("data", {}).get("__type", {}).get("fields", []):
    print(f"  {f['name']}")

# Try githubRepo query
q2 = """
{
  githubRepo(fullRepoName: "rjsanjaymandal/web-scrapper") {
    fullName
    id
    name
    isPrivate
    defaultBranch
  }
}
"""
r2 = graphql(q2)
print("\ngithubRepo:")
print(json.dumps(r2, indent=2)[:500])

# Try me() to see what we get without auth
q3 = "{ me { id name email } }"
r3 = graphql(q3)
print("\nme():")
print(json.dumps(r3, indent=2)[:300])
