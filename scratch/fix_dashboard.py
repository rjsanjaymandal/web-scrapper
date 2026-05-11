import sys

path = r'c:\maysanlabs\web-scrapper\dashboard.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Fix applyFilters
content = content.replace(
    "if (cat) url.searchParams.set('category', cat);",
    "if (q) url.searchParams.set('q', q);"
)

# 2. Fix pagination info
old_info = """            if (infoEl) {
                var totalText = data.filtered_total !== undefined ? data.filtered_total.toLocaleString() : (data.total_pages * data.contacts.length);
                infoEl.innerHTML = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path><circle cx="9" cy="7" r="4"></circle><path d="M23 21v-2a4 4 0 0 0-3-3.87"></path><path d="M16 3.13a4 4 0 0 1 0 7.75"></path></svg>' +
                    '<span><span>' + data.contacts.length + '</span> of ' + totalText + ' leads</span>' +
                    '<span style="color:var(--border-muted);">|</span>' +
                    '<span>Page <span>' + data.page + '</span> of <span>' + data.total_pages + '</span></span>';
            }"""

new_info = """            if (infoEl) {
                var totalText = data.filtered_total !== undefined ? data.filtered_total.toLocaleString() : (data.total_pages * data.contacts.length);
                var startRange = (data.page - 1) * (data.limit || window.pageSize) + 1;
                var endRange = Math.min(startRange + data.contacts.length - 1, data.filtered_total || 0);
                if (data.contacts.length === 0) { startRange = 0; endRange = 0; }
                
                infoEl.innerHTML = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path><circle cx="9" cy="7" r="4"></circle><path d="M23 21v-2a4 4 0 0 0-3-3.87"></path><path d="M16 3.13a4 4 0 0 1 0 7.75"></path></svg>' +
                    '<span><span>' + startRange + '-' + endRange + '</span> of ' + totalText + ' leads</span>' +
                    '<span style="color:var(--border-muted);">|</span>' +
                    '<span>Page <span>' + data.page + '</span> of <span>' + data.total_pages + '</span></span>';
            }"""

# Since old_info has specific indentation, we'll use a more flexible approach if possible, 
# but let's try direct replace first.
if old_info in content:
    content = content.replace(old_info, new_info)
else:
    print("Warning: info block not found precisely")

# 3. Fix JSON response
content = content.replace(
    '"page": page,',
    '"page": page, "limit": limit,'
)

# 4. Fix pageSize update
content = content.replace(
    'window.totalPages = data.total_pages;',
    'window.totalPages = data.total_pages; if (data.limit) window.pageSize = data.limit;'
)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
print("Applied replacements")
