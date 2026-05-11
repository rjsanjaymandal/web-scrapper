import sys

path = r'c:\maysanlabs\web-scrapper\dashboard.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Fix applyFilters variable declaration
content = content.replace(
    "const cat = document.getElementById('t-cat').value;",
    "const q = document.getElementById('t-cat').value;"
)

# 2. Fix setFilter
content = content.replace(
    "window.setFilter = function(city, cat) {",
    "window.setFilter = function(city, q) {"
)
content = content.replace(
    "document.getElementById('t-cat').value = cat;\n            window.applyFilters();",
    "document.getElementById('t-cat').value = q;\n            window.applyFilters();"
)

# 3. Fix setTemplate
content = content.replace(
    "window.setTemplate = function(city, cat, src) {",
    "window.setTemplate = function(city, q, src) {"
)
content = content.replace(
    "document.getElementById('t-cat').value = cat;\n            document.getElementById('t-source').value = src;",
    "document.getElementById('t-cat').value = q;\n            document.getElementById('t-source').value = src;"
)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
print("Applied more replacements")
