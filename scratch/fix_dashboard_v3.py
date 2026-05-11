import sys

path = r'c:\maysanlabs\web-scrapper\dashboard.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update exportData to use 'q' for the search box value
content = content.replace(
    "const category = document.getElementById('t-cat')?.value || \"\";",
    "const q = document.getElementById('t-cat')?.value || \"\";"
)
content = content.replace(
    "if (category) params.set('category', category); else params.delete('category');",
    "if (q) params.set('q', q); else params.delete('q');"
)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated exportData")
