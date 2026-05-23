import re

with open("dashboard.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

# Find all request.args.get or request.args[...] in dashboard.py
matches = re.findall(r"request\.args(?:\.get)?\([^\)]+\)", content)
for m in sorted(list(set(matches))):
    print(m)
