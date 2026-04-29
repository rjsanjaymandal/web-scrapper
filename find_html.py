with open('dashboard.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find the closing triple-quote of the HTML block (after line 468)
for i in range(832, 840):
    print(f"Line {i+1}: {repr(lines[i][:60])}")
