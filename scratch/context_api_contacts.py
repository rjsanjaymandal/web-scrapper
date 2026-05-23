with open("dashboard.py", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

for idx, line in enumerate(lines):
    if "/api/contacts" in line:
        print(f"Line {idx+1}: {line.strip()}")
        # print context
        for i in range(max(0, idx - 5), min(len(lines), idx + 6)):
            print(f"  {i+1}: {lines[i].rstrip()}")
