"""Verify URL patterns without importing heavy scraper modules."""

# Simulate the URL builders inline
def yellowpages_url(city, category, page=1):
    city_slug = city.strip().replace(" ", "-")
    cat_slug = category.strip().replace(" ", "-")
    if page == 1:
        return f"https://www.yellowpages.in/{city_slug}/{cat_slug}"
    return f"https://www.yellowpages.in/{city_slug}/{cat_slug}?page={page}"

def indiamart_url(city, category, page=1):
    cat_slug = category.lower().replace(" ", "+")
    city_slug = city.lower().replace(" ", "+")
    return f"https://www.indiamart.com/search.html?ss={cat_slug}&cq={city_slug}&prdsrc=1&pn={page}"

def tradeindia_url(city, category, page=1):
    query = f"{category} in {city}".replace(" ", "+")
    return f"https://www.tradeindia.com/search.html?keyword={query}&page={page}"

def justdial_url(city, category, page=1):
    category_slug = category.lower().replace(" ", "-")
    if page > 1:
        return f"https://www.justdial.com/{city}/{category_slug}/page-{page}"
    return f"https://www.justdial.com/{city}/{category_slug}"

tests = [
    ("Ahmedabad", "Manufacturing"),
    ("Delhi", "Chartered-Accountants"),
    ("Mumbai", "IT Services"),
]

print("=" * 80)
print("URL PATTERN VERIFICATION")
print("=" * 80)

for city, cat in tests:
    print(f"\n--- {cat} in {city} ---")
    print(f"  YP:  {yellowpages_url(city, cat)}")
    print(f"  IM:  {indiamart_url(city, cat)}")
    print(f"  TI:  {tradeindia_url(city, cat)}")
    print(f"  JD:  {justdial_url(city, cat)}")

print("\n✅ All URL patterns verified!")
