import re
import logging
from celery import shared_task
from bs4 import BeautifulSoup
from utils.polite_fetcher import fetcher

logger = logging.getLogger(__name__)

@shared_task(name="tasks.profile_scraper.extract_profile_data")
def extract_profile_data(profile_url):
    """
    Profile Extractor Task.
    Extracts professional data from a single profile page using BeautifulSoup and Regex.
    """
    logger.info(f"👤 Extracting Profile: {profile_url}")
    
    response = fetcher.fetch(profile_url)
    if not response:
        return {"status": "failed", "reason": "fetch_error"}
        
    try:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # 1. Structure Template Extraction
        # Note: These selectors are examples and should be adjusted per target site
        data = {
            "name": soup.select_one('.name, h1, #profile-name').text.strip() if soup.select_one('.name, h1, #profile-name') else None,
            "email": None,
            "phone": None,
            "reg_id": soup.select_one('.registration, .membership-id').text.strip() if soup.select_one('.registration, .membership-id') else None,
            "city": soup.select_one('.city, .location').text.strip() if soup.select_one('.city, .location') else None,
            "source_url": profile_url
        }
        
        # 2. Regex Fallbacks for Email and Phone
        html_text = soup.get_text()
        
        if not data["email"]:
            email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', html_text)
            if email_match:
                data["email"] = email_match.group(0)
                
        if not data["phone"]:
            # Basic Indian phone number regex
            phone_match = re.search(r'(?:\+91|0)?[6789]\d{9}', html_text)
            if phone_match:
                data["phone"] = phone_match.group(0)

        # 3. Data Cleaning
        for key in data:
            if isinstance(data[key], str):
                data[key] = data[key].strip()

        logger.info(f"✅ Extracted: {data['name']} ({data['email'] or 'No Email'})")
        
        # TODO: Save to DB (Postgres)
        # Example: 
        # from models import Contact
        # Contact.objects.update_or_create(source_url=profile_url, defaults=data)
        
        return {"status": "success", "data": data}
        
    except Exception as e:
        logger.error(f"❌ Error extracting data from {profile_url}: {e}")
        return {"status": "error", "message": str(e)}
