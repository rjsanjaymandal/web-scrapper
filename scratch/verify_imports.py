import sys
import os
import logging

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Verification")

def test_imports():
    try:
        from fast_scraper import fast_scrape_all
        logger.info("✅ fast_scrape_all successfully imported from fast_scraper")
    except ImportError as e:
        logger.error(f"❌ Failed to import fast_scrape_all: {e}")
        return False

    try:
        # We don't want to run the full cycle, just check if it compiles/imports
        import automate_100_cities
        logger.info("✅ automate_100_cities successfully imported")
    except Exception as e:
        logger.error(f"❌ Failed to import automate_100_cities: {e}")
        return False

    return True

if __name__ == "__main__":
    if test_imports():
        logger.info("🚀 All critical imports are working!")
    else:
        logger.error("💀 Verification failed.")
        sys.exit(1)
