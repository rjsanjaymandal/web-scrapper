import re

with open("dashboard.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

# Find all app.route decorators and their function names
routes = re.findall(r'@app\.route\("([^"]+)"[^\n]*\n\s*def\s+(\w+)', content)
for r in routes:
    print(f"Route: {r[0]} -> Function: {r[1]}")
