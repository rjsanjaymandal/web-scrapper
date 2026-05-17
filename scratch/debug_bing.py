import requests
import random
from bs4 import BeautifulSoup

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

def debug():
    url = "https://www.bing.com/search?q=schools+in+North+Delhi&first=1"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.google.com/"
    }
    
    print(f"Fetching: {url}")
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        print(f"Status Code: {resp.status_code}")
        print(f"Response HTML Length: {len(resp.text)} bytes")
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Check for common bot protection pages
        if "captcha" in resp.text.lower() or "verification" in resp.text.lower() or "robot" in resp.text.lower():
            print("WARNING: Bot protection page detected!")
            
        # Check for b_algo elements
        algos = soup.find_all('li', class_='b_algo')
        print(f"Found {len(algos)} 'li.b_algo' elements.")
        
        if algos:
            for idx, a in enumerate(algos[:3]):
                h2 = a.find('h2')
                link = h2.find('a') if h2 else None
                title = link.get_text(strip=True) if link else "N/A"
                href = link['href'] if link and link.has_attr('href') else "N/A"
                print(f"  {idx+1}. Title: {title} | Link: {href}")
        else:
            # Let's see some other high-level elements to see if it's a real search page
            print("No li.b_algo found. High level elements:")
            for tag in ['div', 'li', 'ol', 'ul']:
                elts = soup.find_all(tag)
                print(f"  Total {tag} tags: {len(elts)}")
                
            # Print first 500 characters of page text
            print("\nPage text snippet:")
            print(soup.get_text()[:800].strip())
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    debug()
