import requests
import random
from bs4 import BeautifulSoup

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

def analyze():
    url = "https://search.yahoo.com/search?p=schools+in+North+Delhi+phone+email"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.google.com/"
    }
    
    resp = requests.get(url, headers=headers, timeout=10)
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    print("=== LI TAGS ===")
    for idx, li in enumerate(soup.find_all('li')[:20]):
        classes = li.get('class', [])
        print(f"  [{idx+1}] classes: {classes}")
        
    print("\n=== A TAGS ===")
    for idx, a in enumerate(soup.find_all('a')[:30]):
        classes = a.get('class', [])
        href = a.get('href', 'N/A')
        text = a.get_text(strip=True)[:50]
        text_clean = text.encode('ascii', errors='ignore').decode('ascii')
        print(f"  [{idx+1}] href: {href[:60]} | text: {text_clean} | classes: {classes}")

if __name__ == '__main__':
    analyze()
