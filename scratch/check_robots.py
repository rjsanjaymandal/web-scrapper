import urllib.request
import ssl

def check_robots():
    url = "https://maysanlabs.com/robots.txt"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    print(f"Fetching {url}...")
    req = urllib.request.Request(url, headers=headers)
    context = ssl._create_unverified_context()
    
    try:
        with urllib.request.urlopen(req, context=context, timeout=8) as response:
            content = response.read().decode('utf-8', errors='replace')
            print("\n=== robots.txt content ===")
            print(content)
    except Exception as e:
        print(f"Failed to fetch {url}: {e} (Probably does not exist or blocked)")

if __name__ == "__main__":
    check_robots()
