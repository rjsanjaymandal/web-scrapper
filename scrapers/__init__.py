from scrapers.base import BaseScraper, ScraperRegistry

# Import modules to trigger registration
import scrapers.official
import scrapers.business
import scrapers.directory

# Common exports
__all__ = ["BaseScraper", "ScraperRegistry"]
