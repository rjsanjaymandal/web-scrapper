#!/usr/bin/env python3
"""
Direct test of AMFI API endpoint.
Run: python test_amfi_api.py
"""
import asyncio
import aiohttp
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SEARCH_API_URL = "https://www.amfiindia.com/api/distributor-agent"

async def test_amfi_api(city: str = "Delhi", page: int = 1, page_size: int = 10):
    """Test AMFI API directly."""
    
    params = {
        'strOpt': 'ALL',
        'city': city,
        'page': page,
        'pageSize': page_size,
    }
    
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.amfiindia.com/locate-distributor',
    }
    
    logger.info(f"Testing AMFI API for city={city}, page={page}")
    logger.info(f"URL: {SEARCH_API_URL}")
    logger.info(f"Params: {params}")
    
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        try:
            async with session.get(SEARCH_API_URL, params=params) as response:
                logger.info(f"Status: {response.status}")
                logger.info(f"Content-Type: {response.headers.get('Content-Type')}")
                
                # Check raw response
                raw_text = await response.text()
                logger.info(f"Response length: {len(raw_text)} chars")
                logger.info(f"First 500 chars: {raw_text[:500]}")
                
                # Try to parse as JSON
                try:
                    payload = await response.json(content_type=None)
                    logger.info(f"JSON keys: {list(payload.keys()) if isinstance(payload, dict) else 'Not a dict'}")
                    
                    if isinstance(payload, dict):
                        if 'data' in payload:
                            data = payload['data']
                            logger.info(f"Data type: {type(data)}")
                            logger.info(f"Data length: {len(data) if data else 0}")
                            
                            if data:
                                logger.info(f"\nFirst record: {json.dumps(data[0], indent=2)}")
                        else:
                            logger.info(f"Available keys: {list(payload.keys())}")
                            logger.info(f"Full payload: {json.dumps(payload, indent=2)[:1000]}")
                    else:
                        logger.info(f"Payload type: {type(payload)}")
                        logger.info(f"Payload: {payload}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"JSON parse error: {e}")
                    logger.error(f"Raw response: {raw_text[:2000]}")
                    
        except aiohttp.ClientError as e:
            logger.error(f"Request error: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
            import traceback
            traceback.print_exc()

async def test_different_cities():
    """Test with different city names."""
    cities = ["Delhi", "Mumbai", "Bangalore", "delhi", "mumbai", "DELHI"]
    
    for city in cities:
        logger.info(f"\n{'='*50}")
        logger.info(f"Testing city: {city}")
        logger.info('='*50)
        await test_amfi_api(city)
        await asyncio.sleep(1)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        city = sys.argv[1]
        asyncio.run(test_amfi_api(city))
    else:
        # Test one city first
        asyncio.run(test_amfi_api("Delhi"))
        
        # Then test different variations
        print("\n\nTesting different city name formats...\n")
        asyncio.run(test_different_cities())
