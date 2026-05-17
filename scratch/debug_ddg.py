import requests
import random
from bs4 import BeautifulSoup

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

def debug():
    url = "https://html.duckduckgo.com/html/?q=schools+in+North+Delhi+phone+email"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://duckduckgo.com/"
    }
    
    print(f"Fetching DuckDuckGo: {url}")
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        print(f"Status Code: {resp.status_code}")
        print(f"Response HTML Length: {len(resp.text)} bytes")
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Check for bot protection page
        if "captcha" in resp.text.lower() or "verification" in resp.text.lower() or "robot" in resp.text.lower():
            print("WARNING: Bot protection page detected on DuckDuckGo!")
            
        # In DuckDuckGo HTML search, organic results have class "result" or "result__body"
        results = soup.find_all('div', class_='result')
        print(f"Found {len(results)} 'div.result' elements.")
        
        if results:
            for idx, r in enumerate(results[:3]):
                link_tag = r.find('a', class_='result__snippet') or r.find('a', class_='result__url') or r.find('a', class_='result__snippet')
                # Title anchor has class "result__a"
                a_tag = r.find('a', class_='result__a')
                title = a_tag.get_text(strip=True) if a_tag else "N/A"
                href = a_tag['href'] if a_tag and a_tag.has_attr('href') else "N/A"
                
                snippet_tag = r.find('a', class_='result__snippet')
                snippet = snippet_tag.get_text(strip=True) if snippet_tag else "N/A"
                
                # Print clean results without printing unicode characters that crash Windows CP1252
                title_clean = title.encode('ascii', errors='ignore').decode('ascii')
                snippet_clean = snippet.encode('ascii', errors='ignore').decode('ascii')
                print(f"  {idx+1}. Title: {title_clean} | Link: {href}")
                print(f"     Snippet: {snippet_clean[:100]}")
        else:
            print("No div.result found.")
            # Print page structure
            for tag in ['div', 'li', 'ol', 'ul', 'a']:
                elts = soup.find_all(tag)
                print(f"  Total {tag} tags: {len(elts)}")
            
            # Print page title
            print("Page title:", soup.title.string if soup.title else "N/A")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    debug()
