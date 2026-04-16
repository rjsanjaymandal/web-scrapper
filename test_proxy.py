import asyncio
import logging
import os
import yaml
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ProxyTest")

async def test_proxies():
    # Load config manually to get proxies
    with open("config.yaml", "r") as f:
        config_data = yaml.safe_load(f)
    
    proxy_list = config_data.get("proxy", {}).get("proxies", [])
    if not proxy_list:
        # Check global host
        host = config_data.get("proxy", {}).get("host")
        if host:
            proxy_list = [{"host": host, "username": config_data["proxy"].get("username"), "password": config_data["proxy"].get("password")}]

    if not proxy_list:
        logger.error("No proxies found in config.yaml")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        for proxy in proxy_list:
            logger.info(f"Testing proxy: {proxy['host']}")
            
            # Check for placeholder
            if "your_username" in str(proxy.get("username")):
                logger.warning("Skipping placeholder credentials. Update config.yaml with real Data Impulse info.")
                continue

            try:
                context = await browser.new_context(
                    proxy={
                        "server": proxy["host"],
                        "username": proxy.get("username"),
                        "password": proxy.get("password")
                    }
                )
                page = await context.new_page()
                
                # Check IP via external service
                await page.goto("https://api.ipify.org?format=json", timeout=15000)
                ip_data = await page.inner_text("body")
                logger.info(f"Success! Proxy IP: {ip_data}")
                
            except Exception as e:
                logger.error(f"Proxy failed: {e}")
            finally:
                await context.close()
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_proxies())
