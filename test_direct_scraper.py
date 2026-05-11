"""
Test script for Direct Scraping (No Proxy)
Tests government and regulatory sites without DataImpulse proxy
"""

import sys
import os
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_sebi_direct():
    """Test SEBI direct scraping"""
    print("\n" + "="*60)
    print("TESTING SEBI DIRECT SCRAPER")
    print("="*60)
    
    try:
        from direct_scraper import SEBIDirectScraper, DirectPoliteFetcher
        
        fetcher = DirectPoliteFetcher()
        scraper = SEBIDirectScraper(fetcher)
        
        results = scraper.scrape(city="Delhi", category="Investment Advisors")
        
        print(f"\nResult: {len(results)} records extracted")
        
        if results:
            print("\nSample records:")
            for i, r in enumerate(results[:3]):
                print(f"  {i+1}. Name: {r.get('name', 'N/A')[:50]}")
                print(f"     Address: {r.get('address', 'N/A')[:50] if r.get('address') else 'N/A'}")
                print(f"     Reg No: {r.get('registration_no', 'N/A')}")
                print()
        
        return len(results) > 0
        
    except Exception as e:
        logger.error(f"SEBI test failed: {e}")
        return False


def test_nse_direct():
    """Test NSE direct scraping"""
    print("\n" + "="*60)
    print("TESTING NSE DIRECT SCRAPER")
    print("="*60)
    
    try:
        from direct_scraper import NSEDirectScraper, DirectPoliteFetcher
        
        fetcher = DirectPoliteFetcher()
        scraper = NSEDirectScraper(fetcher)
        
        results = scraper.scrape(city="Mumbai", category="Stock Brokers")
        
        print(f"\nResult: {len(results)} records extracted")
        
        if results:
            print("\nSample records:")
            for i, r in enumerate(results[:3]):
                print(f"  {i+1}. Name: {r.get('name', 'N/A')[:50]}")
                print(f"     Address: {r.get('address', 'N/A')[:50] if r.get('address') else 'N/A'}")
                print(f"     Code: {r.get('registration_no', 'N/A')}")
                print()
        
        return len(results) > 0
        
    except Exception as e:
        logger.error(f"NSE test failed: {e}")
        return False


def test_icai_direct():
    """Test ICAI direct scraping"""
    print("\n" + "="*60)
    print("TESTING ICAI DIRECT SCRAPER")
    print("="*60)
    
    try:
        from direct_scraper import ICAIDirectScraper, DirectPoliteFetcher
        
        fetcher = DirectPoliteFetcher()
        scraper = ICAIDirectScraper(fetcher)
        
        results = scraper.scrape(city="Delhi", category="Chartered Accountants")
        
        print(f"\nResult: {len(results)} records extracted")
        
        if results:
            print("\nSample records:")
            for i, r in enumerate(results[:3]):
                print(f"  {i+1}. Name: {r.get('name', 'N/A')[:50]}")
                print(f"     Email: {r.get('email', 'N/A')}")
                print(f"     Phone: {r.get('phone', 'N/A')}")
                print()
        
        return len(results) > 0
        
    except Exception as e:
        logger.error(f"ICAI test failed: {e}")
        return False


def test_fetcher_only():
    """Test the polite fetcher directly"""
    print("\n" + "="*60)
    print("TESTING DIRECT POLITE FETCHER")
    print("="*60)
    
    try:
        from direct_scraper import DirectPoliteFetcher
        
        fetcher = DirectPoliteFetcher()
        
        # Test fetching SEBI
        print("\nFetching SEBI website...")
        html, status = fetcher.fetch("https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRegistrants=yes")
        
        if html:
            print(f"[OK] SEBI fetch successful (status: {status}, size: {len(html)} bytes)")
        else:
            print(f"[FAIL] SEBI fetch failed (status: {status})")
        
        # Test fetching NSE
        print("\nFetching NSE website...")
        html, status = fetcher.fetch("https://www.nseindia.com/members/content/member_directory.htm")
        
        if html:
            print(f"[OK] NSE fetch successful (status: {status}, size: {len(html)} bytes)")
        else:
            print(f"[FAIL] NSE fetch failed (status: {status})")
        
        return True
        
    except Exception as e:
        logger.error(f"Fetcher test failed: {e}")
        return False


def main():
    print("\n" + "#"*60)
    print("# DIRECT SCRAPING TEST (No Proxy)")
    print("# Testing government sites without DataImpulse")
    print("#"*60)
    
    tests = [
        ("Polite Fetcher", test_fetcher_only),
        ("SEBI Scraper", test_sebi_direct),
        ("NSE Scraper", test_nse_direct),
        ("ICAI Scraper", test_icai_direct),
    ]
    
    results = {}
    
    for name, test_func in tests:
        try:
            results[name] = test_func()
        except Exception as e:
            logger.error(f"Test {name} crashed: {e}")
            results[name] = False
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    for name, passed in results.items():
        status = "[OK] PASSED" if passed else "[FAIL] FAILED"
        print(f"  {name}: {status}")
    
    passed_count = sum(1 for v in results.values() if v)
    print(f"\nTotal: {passed_count}/{len(results)} tests passed")
    
    if passed_count > 0:
        print("\n[OK] Direct scraping is working! Government sites accessible without proxy.")
    else:
        print("\n[FAIL] Direct scraping failed. Check network connectivity.")


if __name__ == "__main__":
    main()
