import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import ssl

def diagnose_site():
    url = "https://maysanlabs.com"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    print(f"Fetching {url}...")
    req = urllib.request.Request(url, headers=headers)
    context = ssl._create_unverified_context()
    
    try:
        with urllib.request.urlopen(req, context=context, timeout=8) as response:
            html = response.read().decode('utf-8', errors='replace')
            code = response.getcode()
            print(f"Status Code: {code}")
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return

    soup = BeautifulSoup(html, "lxml")
    
    print("\n=== Meta Tags Check ===")
    title = soup.find("title")
    print(f"Title: {title.text.strip() if title else 'MISSING'}")
    
    description = soup.find("meta", attrs={"name": "description"})
    print(f"Description: {description.get('content') if description else 'MISSING'}")
    
    viewport = soup.find("meta", attrs={"name": "viewport"})
    print(f"Viewport (Mobile responsiveness): {viewport.get('content') if viewport else 'MISSING'}")
    
    robots = soup.find("meta", attrs={"name": "robots"})
    print(f"Robots Tag (noindex/nofollow check): {robots.get('content') if robots else 'NOT EXPLICITLY SET'}")
    
    html_lang = soup.find("html")
    print(f"HTML lang attribute: {html_lang.get('lang') if html_lang else 'MISSING'}")

    print("\n=== Heading Structure ===")
    h1s = soup.find_all("h1")
    print(f"H1 Count: {len(h1s)}")
    for i, h1 in enumerate(h1s):
        print(f"  H1 #{i+1}: {h1.text.strip()}")
        
    h2s = soup.find_all("h2")
    print(f"H2 Count: {len(h2s)}")
    
    print("\n=== Layout Shift Check (CLS) ===")
    images = soup.find_all("img")
    print(f"Total Images: {len(images)}")
    missing_dimensions = 0
    missing_alt = 0
    for img in images:
        has_width = img.has_attr("width")
        has_height = img.has_attr("height")
        has_alt = img.has_attr("alt") and img.get("alt").strip()
        
        if not has_width or not has_height:
            missing_dimensions += 1
        if not has_alt:
            missing_alt += 1
            
    print(f"Images missing explicit width/height: {missing_dimensions} (Major cause of high CLS!)")
    print(f"Images missing alt text: {missing_alt}")

    print("\n=== Render Blocking Assets Check (LCP) ===")
    styles = soup.find_all("link", attrs={"rel": "stylesheet"})
    scripts = soup.find_all("script")
    print(f"Stylesheets count: {len(styles)}")
    print(f"Script tags count: {len(scripts)}")
    
    async_scripts = 0
    defer_scripts = 0
    blocking_scripts = 0
    for s in scripts:
        if s.has_attr("src"):
            if s.has_attr("async"):
                async_scripts += 1
            elif s.has_attr("defer"):
                defer_scripts += 1
            else:
                blocking_scripts += 1
                
    print(f"Blocking external scripts (missing async/defer): {blocking_scripts} (Major cause of high LCP/poor TTFB!)")

if __name__ == "__main__":
    diagnose_site()
