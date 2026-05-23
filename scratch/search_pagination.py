with open("dashboard.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

lines = content.splitlines()
matches = []
for idx, line in enumerate(lines):
    if "function goToPage" in line or "function changePage" in line or "goToPage = function" in line or "changePage = function" in line:
        matches.append(idx)

for start_line in matches:
    print(f"Found function at line {start_line+1}:")
    for i in range(max(0, start_line - 5), min(len(lines), start_line + 45)):
        print(f"{i+1}: {lines[i]}")
    print("-" * 40)
