import random
import asyncio
import logging
import os
import psutil
import re
from typing import Dict, List, Optional
from playwright.async_api import async_playwright, Browser, BrowserContext
import asyncpg
try:
    import redis.asyncio as redis
except ImportError:
    redis = None
try:
    from curl_cffi.requests import AsyncSession
except ImportError:
    AsyncSession = None

# Trigger scraper registration before using ScraperRegistry
import scraper  # noqa: F401
import enhanced_utils  # noqa: F401

from scrapers_registry import ScraperRegistry
from processing import ProcessingHandler
from stealth_utils import StealthManager


logger = logging.getLogger(__name__)


class FastScraperConfig:
    def __init__(self, config_dict: Dict):
        self.db_url = os.environ.get("DATABASE_URL")
        if self.db_url and self.db_url.startswith("postgres://"):
            self.db_url = self.db_url.replace("postgres://", "postgresql://", 1)

        self.max_concurrent = int(os.environ.get("MAX_CONCURRENT", 20))
        self.headless = os.environ.get("HEADLESS", "true").lower() == "true"
        # Convert seconds from config to milliseconds for Playwright
        timeout_sec = int(os.environ.get("TIMEOUT", config_dict.get("scraper_settings", {}).get("timeout", 60)))
        self.timeout = timeout_sec * 1000
        self.max_retries = int(os.environ.get("SCRAPER_MAX_RETRIES", config_dict.get("scraper_settings", {}).get("max_retries", 3)))
        self.block_resources = True
        self.redis_url = os.environ.get("REDIS_URL")
        self.enable_city_targeting = os.environ.get("PROXY_CITY_TARGETING", "false").lower() == "true"
        self.force_http1_sources = config_dict.get("scraper_settings", {}).get("force_http1_sources", [])

        self.cities = config_dict.get("cities", [])
        self.categories = config_dict.get("categories", [])
        
        # Enterprise Infrastructure: Global source throttling map
        # Ensures sensitive sites like Google aren't hammered, preventing CAPTCHAs.
        self.delay_map = {
            "FOOTPRINT": 15.0,  # High Cooldown for Google
            "NSE": 5.0,
            "BSE": 5.0,
            "SEBI": 3.0,
            "AMFI": 1.0,        # Trusted APIs can be faster
            "DEFAULT": 2.0
        }
        
        # Memory Safety: Railway vCPU/RAM monitoring threshold
        self.memory_threshold = 0.85 
        
        # Security: Prefer environment variables for proxies (Railway best practice)
        self.proxy_list = []
        env_proxy_host = os.environ.get("PROXY_HOST")
        
        # Determine source for logging
        if env_proxy_host:
            env_proxy_host = env_proxy_host.strip()
            source = "Environment (PROXY_HOST)"
            env_proxy_port = os.environ.get("PROXY_PORT", "").strip()
            
            # Build host:port string if port is provided separately and not in host
            if ":" not in env_proxy_host:
                if env_proxy_port:
                    proxy_host = f"{env_proxy_host}:{env_proxy_port}"
                elif "dataimpulse.com" in env_proxy_host.lower():
                    # Fallback to Data Impulse default if missing
                    proxy_host = f"{env_proxy_host}:823"
                    logger.warning(f"⚠️  Auto-appending default port :823 for Data Impulse.")
                else:
                    proxy_host = env_proxy_host
            else:
                proxy_host = env_proxy_host
                
            proxy_user = os.environ.get("PROXY_USER", "").strip()
            proxy_pass = os.environ.get("PROXY_PASS", "").strip()
            
            # Enterprise Update: Auto-append country suffix for DataImpulse Indian residential proxies
            # This fixes 'ERR_TUNNEL_CONNECTION_FAILED' caused by missing geo-targeting strings.
            if proxy_user and "dataimpulse.com" in env_proxy_host.lower() and "__cr." not in proxy_user:
                proxy_user = f"{proxy_user}__cr.in"
                logger.info(f"🛡️  Harden: Auto-appended country suffix to proxy user: {proxy_user[:5]}...__cr.in")

            # If they are empty strings after stripping, set to None so we don't pass empty auth
            proxy_user = proxy_user if proxy_user else None
            proxy_pass = proxy_pass if proxy_pass else None
            
            self.proxy_list.append({
                "host": proxy_host,
                "username": proxy_user,
                "password": proxy_pass
            })
            
            # Security-aware logging: Mask sensitive parts
            masked_host = proxy_host.split("@")[-1]  # In case host string already contains credentials
            logger.info(f"✅ Proxy configured from {source}: {masked_host}")
            
            # Validation: Port is critical for residential proxies
            if ":" not in proxy_host.split("//")[-1]:
                logger.warning(f"⚠️  PROXY MUDDLED! Host '{masked_host}' has no port. Residents usually need :80, :823, etc.")
        else:
            config_proxies = config_dict.get("proxies", [])
            if config_proxies:
                self.proxy_list = config_proxies
                logger.info(f"✅ Proxy configured from Config File: {len(self.proxy_list)} proxies")
            else:
                logger.warning("⚠️  NO PROXY CONFIGURED! Running on direct IP (Risk of ban).")


class ParallelScraper:
    """Efficient parallel engine using Playwright & asyncpg."""

    def __init__(self, config: FastScraperConfig):
        self.config = config
        self.semaphore = asyncio.Semaphore(config.max_concurrent)
        self.playwright = None
        self.browser = None
        self.pool = None
        self.redis_conn = None

    async def init(self):
        if self.config.redis_url and redis:
            self.redis_conn = await redis.from_url(self.config.redis_url)
        
        self.playwright = await async_playwright().start()
        # Browsers will be lazy-loaded in _setup_context to save memory
        
        if self.config.db_url and asyncpg:
            try:
                self.pool = await asyncpg.create_pool(self.config.db_url, min_size=1, max_size=self.config.max_concurrent)
            except Exception as e:
                logger.error(f"Failed to create DB pool: {e}")
        else:
            logger.error("No DATABASE_URL found for parallel scraper pool.")

        logger.info(f"Parallel Engine V2 ready: Concurrency={self.config.max_concurrent}")

        # Proxy connectivity smoke test
        if self.config.proxy_list:
            await self._test_proxy_connectivity()

    async def close(self):
        if self.browser:
            await self.browser.close()
        if hasattr(self, "browser_h1") and self.browser_h1:
            await self.browser_h1.close()
        if self.playwright:
            await self.playwright.stop()
        if self.pool:
            await self.pool.close()

    async def _test_proxy_connectivity(self):
        """Quick smoke test: one page load through the proxy to validate connectivity."""
        p = self.config.proxy_list[0]
        host = p["host"]
        if not host.startswith("http"):
            host = f"http://{host}"
        proxy_config = {
            "server": host,
            "username": p.get("username"),
            "password": p.get("password"),
        }
        logger.info(f"🔍 Testing proxy: server={host}")
        # Ensure browser is launched for the test
        if not self.browser:
            self.browser = await self.playwright.chromium.launch(
                headless=self.config.headless,
                args=["--disable-dev-shm-usage", "--no-sandbox"]
            )
            
        ctx = None
        try:
            ctx = await self.browser.new_context(
                proxy=proxy_config,
                ignore_https_errors=True,
            )
            page = await ctx.new_page()
            resp = await page.goto("https://httpbin.org/ip", timeout=30000, wait_until="domcontentloaded")
            body = await page.inner_text("body")
            logger.info(f"✅ PROXY OK! Response status={resp.status}, IP={body.strip()[:80]}")
        except Exception as e:
            logger.critical(f"❌ PROXY FAILED! Error: {e}")
            logger.critical(f"❌ Check PROXY_HOST (must include port) and PROXY_USER/PROXY_PASS env vars!")
        finally:
            if ctx:
                await ctx.close()

    async def _setup_context(self, city: Optional[str] = None, session_id: Optional[str] = None, force_http1: bool = False) -> BrowserContext:
        user_agent = StealthManager.get_random_ua()
        extra_headers = StealthManager.get_modern_headers(user_agent)
        
        from stealth_utils import DataImpulseManager

        # Selection of proxy if available
        proxy_config = None
        if self.config.proxy_list:
            p = random.choice(self.config.proxy_list)
            host = p["host"]
            # Playwright requires full URL format: http://host:port
            if not host.startswith("http"):
                host = f"http://{host}"
            proxy_config = {
                "server": host,
                "username": DataImpulseManager.format_auth(
                    p.get("username"), 
                    city=city, 
                    session_id=session_id,
                    enable_city=self.config.enable_city_targeting
                ),
                "password": p.get("password")
            }
            logger.debug(f"Worker using proxy: {host} (H1={force_http1})")

        # Select browser based on protocol requirement
        # Lazy-load browser based on protocol requirement
        if force_http1:
            if not getattr(self, "browser_h1", None):
                logger.info("🚀 Launching Stealth HTTP/1.1 Engine...")
                self.browser_h1 = await self.playwright.chromium.launch(
                    headless=self.config.headless,
                    args=[
                        "--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu",
                        "--disable-software-rasterizer", "--disable-extensions",
                        "--disable-http2",
                    ],
                )
            target_browser = self.browser_h1
        else:
            if not getattr(self, "browser", None):
                logger.info("🚀 Launching Standard Engine...")
                self.browser = await self.playwright.chromium.launch(
                    headless=self.config.headless,
                    args=[
                        "--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu",
                        "--disable-software-rasterizer", "--disable-extensions",
                    ],
                )
            target_browser = self.browser

        context = await target_browser.new_context(
            user_agent=user_agent,
            extra_http_headers=extra_headers,
            viewport={"width": 1280, "height": 720},
            device_scale_factor=random.choice([1, 2]),
            has_touch=random.choice([True, False]),
            ignore_https_errors=True,
            proxy=proxy_config,
        )

        # Apply advanced stealth patches
        await StealthManager.apply_stealth(context)

        # Block heavy and tracking resources
        if self.config.block_resources:
            # Domain blocklist for trackers/analytics
            block_domains = [
                "google-analytics.com",
                "googletagmanager.com",
                "facebook.net",
                "hotjar.com",
                "clarity.ms",
                "doubleclick.net",
                "adnxs.com",
            ]

            async def block_aggressively(route):
                url = route.request.url.lower()
                # Enterprise Bandwidth Saving: Block ALL Media, Images, Fonts, AND Stylesheets (CSS)
                if route.request.resource_type in [
                    "image",
                    "font",
                    "media",
                    "stylesheet"
                ] or any(domain in url for domain in block_domains):
                    await route.abort()
                else:
                    await route.continue_()

            await context.route("**/*", block_aggressively)

        return context

    async def scrape_job(self, city: str, category: str, source_name: str) -> int:
        """Run a single scraper job within the semaphore limit (With Auto-Retry and Proxy Swap)."""
        async with self.semaphore:
            scraper = ScraperRegistry.get(source_name)
            if not scraper:
                logger.error(f"No scraper found for {source_name}")
                return 0

            context = None
            page = None
            
            # Generate a unique session ID for this job to maintain Sticky IP consistency
            import uuid
            session_id = str(uuid.uuid4())[:8]
            
            for attempt in range(self.config.max_retries):
                # If we detected a block in the previous attempt, 
                # rotate the session_id to get a fresh IP for the retry.
                if attempt > 0:
                    session_id = str(uuid.uuid4())[:8]
                    logger.info(f"🛡️  Rotating Sticky Session: New IP requested for attempt {attempt+1}")

                url = scraper.build_search_url(city, category)
                
                # ==== PHASE 1: HYBRID HTTP FETCH (Enterprise Speed Loop) ====
                if AsyncSession and attempt == 0:
                    try:
                        async with AsyncSession(impersonate="chrome120") as s:
                            headers = {"User-Agent": StealthManager.get_random_ua()}
                            
                            # Inject cached cookies to bypass Cloudflare
                            if self.redis_conn:
                                cached = await self.redis_conn.get(f"cookies_{source_name}")
                                if cached:
                                    headers["Cookie"] = cached.decode("utf-8")
                                    
                            fast_resp = await s.get(url, headers=headers, timeout=10)
                            html = fast_resp.text
                            # Heuristic check for WAF/Cloudflare
                            if fast_resp.status_code == 200 and "Just a moment" not in html and "Verify you are human" not in html:
                                fast_leads = scraper.extract_raw_fallback(html, city, category)
                                if fast_leads:
                                    logger.info(f"⚡ FAST HTTP SUCCESS! Extracted {len(fast_leads)} via curl_cffi regex from {source_name}. Bypassed Playwright.")
                                    processed = ProcessingHandler.process_batch(fast_leads)
                                    valid = ProcessingHandler.filter_valid(processed)
                                    if valid:
                                        await self._batch_insert(valid, category, city, source_name)
                                    return len(valid)
                    except Exception as e:
                        logger.debug(f"Fast HTTP fetch failed: {e}. Falling back to Browser.")

                # ==== PHASE 2: BROWSER FALLBACK ====
                try:
                    # Memory Safety Check: Prevent OOM on Railway
                    mem_percent = psutil.virtual_memory().percent
                    if mem_percent > self.config.memory_threshold * 100:
                        wait_time = random.uniform(5.0, 10.0)
                        logger.warning(f"⚠️  High Memory Usage ({mem_percent}%). Cooling down for {wait_time:.1f}s...")
                        await asyncio.sleep(wait_time)
                        # Re-check or continue
                        if mem_percent > 95:
                            logger.error("🛑 CRITICAL MEMORY: Aborting job to prevent restart.")
                            return 0

                    # Enterprise Protocol Selection
                    force_h1 = scraper.force_http1 or source_name in self.config.force_http1_sources
                    if attempt > 0 and "http2" in str(getattr(self, 'last_error', '')).lower():
                        force_h1 = True
                        logger.warning(f"🛡️  Protocol Error detected previously. Forcing HTTP/1.1 for retry.")

                    # Use a unique session_id per attempt to force DataImpulse to provide a new IP
                    session_id = f"sess_{int(asyncio.get_event_loop().time())}_{attempt}"
                    context = await self._setup_context(city=city, session_id=session_id, force_http1=force_h1)
                    page = await context.new_page()
                    
                    try:
                        response = await page.goto(
                            url, timeout=self.config.timeout, wait_until="domcontentloaded"
                        )
                        status_code = response.status if response else 0
                        
                        if status_code in [403, 404, 429] and source_name in ["TRADEINDIA", "INDIAMART", "YELLOWPAGES"]:
                             logger.warning(f"🛡️  {source_name} returned {status_code}. Likely URL deprecated or IP Blocked.")
                             raise Exception(f"WAF Block/URL Error: HTTP {status_code}")
                             
                    except Exception as goto_err:
                        self.last_error = str(goto_err)
                        # Specific handling for Playwright navigation errors that are often WAF stealth blocks
                        if any(x in str(goto_err) for x in ["ERR_ABORTED", "ERR_CONNECTION_CLOSED", "ERR_CONNECTION_RESET"]):
                            logger.warning(f"🛡️  Connection error on {source_name}: likely WAF block. Retrying...")
                        raise goto_err

                    listings = await scraper.extract_listings(page, city, category)
                    
                    # WAF / Block Detection Logic
                    page_title = await page.title()
                    body_text = await page.evaluate("() => document.body.innerText.substring(0, 500)")
                    
                    block_keywords = [
                        "Access Denied", "Forbidden", "403", "404", "Not Found", 
                        "Resource cannot be found", "Unusual traffic", "Verify you are human",
                        "Checking your browser", "Cloudflare", "Server Error"
                    ]
                    
                    is_blocked = any(kw.lower() in page_title.lower() or kw.lower() in body_text.lower() for kw in block_keywords)
                    
                    if is_blocked and not listings:
                        logger.warning(f"🛡️  WAF/Block detected on {source_name} (Title: {page_title})")
                        raise Exception(f"WAF Block detected: {page_title}")

                    logger.info(f"Job: {source_name} | Attempt {attempt + 1}/{self.config.max_retries} | Raw Extracted: {len(listings)} leads")

                    if not listings:
                        logger.warning(f"Job: {source_name} | 0 Leads | Page Title: {page_title} | Body: {body_text[:100].replace('\n', ' ')}")

                    if listings:
                        processed = ProcessingHandler.process_batch(listings)
                        valid = ProcessingHandler.filter_valid(processed)
                        
                        if processed and not valid:
                            sample = processed[0]
                            logger.warning(
                                f"Job: {source_name} | ALL REJECTED | Sample: "
                                f"name='{sample.get('name','')[:40]}' "
                                f"phone='{sample.get('phone')}' "
                                f"email='{sample.get('email')}'"
                            )
                        
                        if valid:
                            await self._batch_insert(valid, category, city, source_name)
                            # Phase 4: Cache Authorized Cookies (Immunity Vault)
                            if self.redis_conn and context:
                                new_cookies = await context.cookies()
                                # Only store cookies if we got valid lists
                                cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in new_cookies])
                                if cookie_str:
                                    await self.redis_conn.setex(f"cookies_{source_name}", 3600, cookie_str)
                                    logger.debug(f"Saved authorized session cookie to Redis for {source_name}")
                        return len(valid)
                        
                    # If we reach here and it's 0 leads, it might be a block. Break if last attempt.
                    if attempt == self.config.max_retries - 1:
                        return 0
                        
                except Exception as e:
                    logger.warning(f"Job: {source_name} | Attempt {attempt + 1}/{self.config.max_retries} Failed: {str(e)[:100]}. Rotating proxy...")
                    if attempt == self.config.max_retries - 1:
                        logger.error(f"Job failed completely: {source_name} | {city}/{category}")
                        return 0
                finally:
                    if page:
                        await page.close()
                    if context:
                        await context.close()
                    # Sleep based on Adaptive Delay Map
                    delay = self.config.delay_map.get(source_name, self.config.delay_map["DEFAULT"])
                    if attempt < self.config.max_retries - 1:
                        # Add some jitter to avoid pattern detection
                        await asyncio.sleep(delay + random.uniform(2.0, 5.0))

    async def _batch_insert(
        self, listings: List[Dict], category: str, city: str, source: str
    ):
        if not self.pool:
            return

        records = [
            (
                rec.get("name", "")[:255],
                rec.get("phone", "")[:50],
                rec.get("email", "")[:255],
                rec.get("address", ""),
                category[:100],
                city[:100],
                rec.get("area", "")[:100],
                rec.get("state", "")[:100],
                source[:100],
                rec.get("detail_url", ""),
                rec.get("phone_clean", "")[:50],
                rec.get("email_valid", False),
                True,  # enriched
                rec.get("arn", "")[:50],
                rec.get("license_no", "")[:100],
                rec.get("membership_no", "")[:100],
                rec.get("quality_score", 0),
                rec.get("quality_tier", "low"),
            )
            for rec in listings
            if rec.get("name")
        ]

        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO contacts (
                    name, phone, email, address, category, city, area, state, source, 
                    source_url, phone_clean, email_valid, enriched, arn, license_no, 
                    membership_no, quality_score, quality_tier
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                ON CONFLICT (phone_clean) WHERE phone_clean IS NOT NULL
                DO UPDATE SET
                    quality_score = EXCLUDED.quality_score,
                    quality_tier = EXCLUDED.quality_tier,
                    scraped_at = EXCLUDED.scraped_at,
                    enriched = TRUE
                WHERE EXCLUDED.quality_score >= contacts.quality_score
            """,
                records,
            )

    async def run_parallel_suite(self, jobs: List[tuple]):
        tasks = [self.scrape_job(city, cat, src) for city, cat, src in jobs]
        results = await asyncio.gather(*tasks)
        total_leads = sum(results)
        logger.info(f"Parallel Suite Complete: Total Leads Found: {total_leads}")
        return total_leads


async def fast_scrape_all(config_dict: Dict, cities: List[str], categories: List[str]):
    config = FastScraperConfig(config_dict)
    engine = ParallelScraper(config)
    await engine.init()

    try:
        jobs = []
        for city in cities:
            for cat in categories:
                # Get ALL sources for this category, not just one
                sources = ScraperRegistry.get_all_sources_for_category(cat)
                for source in sources:
                    jobs.append((city, cat, source))

        logger.info(
            f"Total scraping jobs: {len(jobs)} (cities: {len(cities)}, categories: {len(categories)})"
        )
        return await engine.run_parallel_suite(jobs)
    finally:
        await engine.close()
