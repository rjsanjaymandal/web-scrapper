from celery import shared_task
import logging
import json
from utils.polite_fetcher import fetcher

logger = logging.getLogger(__name__)

@shared_task(name="tasks.api_scraper.bulk_api_paginator")
def bulk_api_paginator(base_api_url, max_pages=50):
    """
    High-volume API Paginator.
    Automatically crawls through JSON endpoints (page=1, 2, 3...).
    Bypasses Playwright to save memory on Railway.
    """
    current_page = 1
    total_saved = 0
    
    logger.info(f"📊 Starting Bulk API extraction from: {base_api_url}")
    
    while current_page <= max_pages:
        # Construct the paginated URL
        # Assumes format like https://example.com/api/data?page=
        target_url = f"{base_api_url}{current_page}"
        
        logger.info(f"🔍 Fetching Page {current_page}...")
        response = fetcher.fetch(target_url)
        
        if not response:
            logger.warning(f"⚠️ No response for page {current_page}. Stopping.")
            break
            
        try:
            data = response.json()
            
            # Stop if the API returns an empty list or null data
            if not data or (isinstance(data, list) and len(data) == 0):
                logger.info(f"🏁 Reached end of registry at page {current_page}.")
                break
                
            # Process the list of items (assuming data is a list or contains a list)
            items = data if isinstance(data, list) else data.get('results', [])
            
            if not items:
                logger.info(f"🏁 No more items found on page {current_page}.")
                break

            for item in items:
                # TODO: Save to DB (Postgres Model)
                # Example: Lead.objects.update_or_create(reg_id=item['id'], defaults={...})
                total_saved += 1
                
            logger.info(f"✅ Processed {len(items)} items from page {current_page}.")
            current_page += 1
            
        except json.JSONDecodeError:
            logger.error(f"❌ Failed to parse JSON from {target_url}")
            break
        except Exception as e:
            logger.error(f"❌ Unexpected error on page {current_page}: {e}")
            break
            
    return {
        "status": "completed",
        "total_pages": current_page - 1,
        "total_records": total_saved
    }
