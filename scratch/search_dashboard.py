with open("dashboard.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

lines = content.splitlines()
start_line = -1
for idx, line in enumerate(lines):
    if "def render_dashboard_portal" in line:
        start_line = idx
        break

if start_line != -1:
    print(f"Found render_dashboard_portal at line {start_line+1}:")
    for i in range(start_line, min(len(lines), start_line + 150)):
        print(f"{i+1}: {lines[i]}")
else:
    print("Not found def render_dashboard_portal")
