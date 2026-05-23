with open("dashboard.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

# Search for any fetch or ajax calls to /api/contacts
import re
matches = re.findall(r"['\"]/api/contacts['\"]", content)
print("Matches for /api/contacts in dashboard.py:", matches)
