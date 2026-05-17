import requests
import sys
import os
from bs4 import BeautifulSoup

# Ensure project root is in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stealth_utils import StealthManager

def test_stealth():
    url = "https://www.bing.com/search?q=schools+in+North+Delhi&first=1"
    ua = StealthManager.get_persistent_ua()
    headers = StealthManager.get_modern_headers(ua)
    
    # Customize headers to fit Bing search specifically
    headers["Sec-Fetch-Site"] = "same-origin"
    
    print(f"Fetching Bing with Stealth headers...")
    print(f"UA: {ua}")
    print("Client Hints:")
    for k, v in headers.items():
        if k.startswith("sec-ch-"):
            print(f"  {k}: {v}")
            
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        print(f"Status Code: {resp.status_code}")
        print(f"HTML Length: {len(resp.text)} bytes")
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Check for bot protection page
        if "captcha" in resp.text.lower() or "verification" in resp.text.lower() or "robot" in resp.text.lower():
            print("WARNING: Bot protection page detected!")
            
        algos = soup.find_all('li', class_='b_algo')
        print(f"Found {len(algos)} 'li.b_algo' elements.")
        
        if algos:
            for idx, a in enumerate(algos[:5]):
                h2 = a.find('h2')
                link = h2.find('a') if h2 else None
                title = link.get_text(strip=True) if link else "N/A"
                href = link['href'] if link and link.has_attr('href') else "N/A"
                
                title_clean = title.encode('ascii', errors='ignore').decode('ascii')
                print(f"  {idx+1}. Title: {title_clean} | Link: {href}")
        else:
            print("No organic results found.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    test_stealth()
