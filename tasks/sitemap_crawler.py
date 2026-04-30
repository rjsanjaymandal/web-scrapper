import re
import logging
from celery import shared_task
from bs4 import BeautifulSoup
from utils.polite_fetcher import fetcher

logger = logging.getLogger(__name__)

@shared_task(name="tasks.sitemap_crawler.discover_from_sitemap")
def discover_from_sitemap(sitemap_url, regex_pattern=r"/profile/|/member/"):
    """
    Sitemap Discovery Task.
    Parses sitemap.xml to find profile URLs and pushes them to the queue.
    """
    logger.info(f"🌐 Crawling Sitemap: {sitemap_url}")
    
    response = fetcher.fetch(sitemap_url)
    if not response:
        logger.error(f"❌ Could not download sitemap: {sitemap_url}")
        return {"status": "failed", "reason": "download_error"}
        
    try:
        # Parse XML
        soup = BeautifulSoup(response.content, 'xml')
        urls = soup.find_all('loc')
        
        discovered_count = 0
        pattern = re.compile(regex_pattern)
        
        for url_node in urls:
            url = url_node.text.strip()
            
            # Match against specific pattern (profiles, members, etc.)
            if pattern.search(url):
                # TODO: Push individual URL to Redis/Celery queue for profile extraction
                # tasks.profile_scraper.extract_profile_data.delay(url)
                discovered_count += 1
                
        logger.info(f"🎯 Discovered {discovered_count} matching URLs from sitemap.")
        return {
            "status": "success",
            "urls_discovered": discovered_count,
            "source": sitemap_url
        }
        
    except Exception as e:
        logger.error(f"❌ Error parsing sitemap {sitemap_url}: {e}")
        return {"status": "error", "message": str(e)}
