import asyncio
import sys
import os

# Add parent directory to path so python can import seo_analyzer
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def test_analyzer():
    from seo_analyzer import SEOAnalyzer
    
    test_url = "https://example.com"
    print(f"Starting test analysis on: {test_url}")
    
    html, load_time, error = await SEOAnalyzer.fetch_html(test_url)
    if error:
        print(f"Fetch failed with error: {error}")
        return
        
    print(f"Fetched HTML successfully in {load_time}s. Parsing...")
    report = SEOAnalyzer.analyze(test_url, html, load_time)
    
    print("\n--- RESULTS ---")
    print(f"URL: {report['url']}")
    print(f"Overall Health Score: {report['overall_score']}/100")
    print(f"  - SEO Score: {report['seo_score']}/100")
    print(f"  - Best Practices & Structure Score: {report['perf_score']}/100")
    print(f"  - Security Score: {report['sec_score']}/100")
    print(f"Load Time: {report['load_time']}s")
    
    print(f"\nCritical Issues ({len(report['critical'])}):")
    for crit in report['critical']:
        print(f"  [CRITICAL] {crit['title']} - {crit['desc']}")
        
    print(f"\nWarnings ({len(report['warnings'])}):")
    for warn in report['warnings']:
        print(f"  [WARNING] {warn['title']} - {warn['desc']}")
        
    print(f"\nPassed Audits ({len(report['passed'])}):")
    for pass_audit in report['passed']:
        print(f"  [PASSED] {pass_audit['title']} - {pass_audit['desc']}")
        
    print("\nTest completed successfully!")

if __name__ == "__main__":
    asyncio.run(test_analyzer())
