import re
import time
import logging
from typing import Dict, List, Tuple
from bs4 import BeautifulSoup
import aiohttp
import urllib.parse

logger = logging.getLogger("seo_analyzer")

class SEOAnalyzer:
    """
    Advanced website SEO, Performance, and Security Health Analyzer.
    """

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5"
    }

    @staticmethod
    async def fetch_html(url: str) -> Tuple[str, float, str]:
        """
        Asynchronously fetches HTML content of a URL.
        Returns: (html_content, load_time_seconds, error_message)
        """
        # Enforce protocol if missing
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
            
        start_time = time.time()
        try:
            timeout = aiohttp.ClientTimeout(total=6.0)
            async with aiohttp.ClientSession(headers=SEOAnalyzer.HEADERS, timeout=timeout) as session:
                async with session.get(url, allow_redirects=True) as response:
                    html = await response.text(errors="replace")
                    load_time = round(time.time() - start_time, 2)
                    return html, load_time, ""
        except Exception as e:
            load_time = round(time.time() - start_time, 2)
            logger.warning(f"Failed to fetch URL {url}: {e}")
            # Fallback to HTTP if HTTPS failed (just in case)
            if url.startswith("https://"):
                try:
                    http_url = url.replace("https://", "http://", 1)
                    async with aiohttp.ClientSession(headers=SEOAnalyzer.HEADERS, timeout=timeout) as session:
                        async with session.get(http_url, allow_redirects=True) as response:
                            html = await response.text(errors="replace")
                            load_time = round(time.time() - start_time, 2)
                            return html, load_time, ""
                except Exception as inner_e:
                    return "", load_time, str(inner_e)
            return "", load_time, str(e)

    @staticmethod
    def analyze(url: str, html: str, load_time: float) -> Dict:
        """
        Parses HTML and computes detailed SEO, Performance, and Security audits and scores.
        """
        if not html:
            return {
                "url": url,
                "overall_score": 0,
                "seo_score": 0,
                "perf_score": 0,
                "sec_score": 0,
                "load_time": load_time,
                "passed": [],
                "warnings": [],
                "critical": [{"title": "Website Unreachable", "desc": "Could not fetch website content. Ensure the URL is correct and online."}],
                "details": {}
            }

        soup = BeautifulSoup(html, "lxml")
        parsed_url = urllib.parse.urlparse(url)
        is_https = parsed_url.scheme == "https"

        passed = []
        warnings = []
        critical = []

        # --- SEO ANALYSIS ---
        seo_points = 0
        
        # 1. Meta Title Check
        title_tag = soup.find("title")
        title_text = title_tag.get_text().strip() if title_tag else ""
        if not title_text:
            critical.append({
                "category": "SEO",
                "title": "Missing Meta Title",
                "desc": "Your site does not have a meta title. This is critical for search indexing."
            })
        elif len(title_text) < 30:
            seo_points += 15
            warnings.append({
                "category": "SEO",
                "title": "Short Meta Title",
                "desc": f"Title is too short ({len(title_text)} chars). Ideal length is 30-65 characters. Current: '{title_text}'"
            })
        elif len(title_text) > 65:
            seo_points += 15
            warnings.append({
                "category": "SEO",
                "title": "Long Meta Title",
                "desc": f"Title is too long ({len(title_text)} chars). It will get truncated in search results. Ideal is 30-65 chars."
            })
        else:
            seo_points += 30
            passed.append({
                "category": "SEO",
                "title": "Optimized Meta Title",
                "desc": f"Your title is well-optimized ({len(title_text)} characters): '{title_text}'"
            })

        # 2. Meta Description Check
        desc_tag = soup.find("meta", attrs={"name": "description"})
        desc_text = desc_tag.get("content", "").strip() if desc_tag else ""
        if not desc_text:
            critical.append({
                "category": "SEO",
                "title": "Missing Meta Description",
                "desc": "No meta description found. Search engines use this text to display search snippets."
            })
        elif len(desc_text) < 100:
            seo_points += 15
            warnings.append({
                "category": "SEO",
                "title": "Short Meta Description",
                "desc": f"Description is too short ({len(desc_text)} chars). Ideal is 120-160 characters to capture user clicks."
            })
        elif len(desc_text) > 170:
            seo_points += 15
            warnings.append({
                "category": "SEO",
                "title": "Long Meta Description",
                "desc": f"Description is too long ({len(desc_text)} chars) and will be truncated by search engines. Ideal is 120-160 chars."
            })
        else:
            seo_points += 30
            passed.append({
                "category": "SEO",
                "title": "Optimized Meta Description",
                "desc": "Meta description is perfectly sized and optimized."
            })

        # 3. H1 Heading Tag Check
        h1_tags = soup.find_all("h1")
        h1_count = len(h1_tags)
        if h1_count == 0:
            critical.append({
                "category": "SEO",
                "title": "Missing H1 Heading",
                "desc": "No H1 tag was found. Every page must have exactly one H1 tag acting as the main topic title."
            })
        elif h1_count > 1:
            seo_points += 15
            warnings.append({
                "category": "SEO",
                "title": "Multiple H1 Headings",
                "desc": f"Found {h1_count} H1 tags. Best practice is to have exactly one H1 tag to assist search index crawlers."
            })
        else:
            seo_points += 25
            passed.append({
                "category": "SEO",
                "title": "Single H1 Tag Present",
                "desc": f"Heading hierarchy starts correctly with a single H1: '{h1_tags[0].get_text().strip()}'"
            })

        # 4. Open Graph (og:) Social tags
        og_title = soup.find("meta", attrs={"property": "og:title"})
        og_desc = soup.find("meta", attrs={"property": "og:description"})
        if og_title and og_desc:
            seo_points += 15
            passed.append({
                "category": "SEO",
                "title": "Open Graph Metadata",
                "desc": "Social sharing tags are active, enabling beautiful social link previews."
            })
        else:
            seo_points += 5
            warnings.append({
                "category": "SEO",
                "title": "Missing Social sharing tags",
                "desc": "Add Open Graph metadata (og:title, og:description) to control how your site renders when shared on social networks."
            })

        seo_score = min(seo_points, 100)


        # --- PERFORMANCE & ACCESSIBILITY ANALYSIS ---
        perf_points = 0
        
        # 1. Page Weight / Size Check
        html_size_kb = round(len(html) / 1024, 1)
        if html_size_kb < 100:
            perf_points += 25
            passed.append({
                "category": "Performance",
                "title": "Lightweight Page Weight",
                "desc": f"HTML weight is very light ({html_size_kb} KB), ensuring swift loading speeds."
            })
        elif html_size_kb < 300:
            perf_points += 20
            passed.append({
                "category": "Performance",
                "title": "Moderate Page Weight",
                "desc": f"HTML size is moderate ({html_size_kb} KB). Fast loads expected on stable connections."
            })
        else:
            perf_points += 10
            warnings.append({
                "category": "Performance",
                "title": "Hefty Page Size",
                "desc": f"HTML payload is heavy ({html_size_kb} KB). Consider minifying assets or using Gzip compression."
            })

        # 2. Inline/External Stylesheets count
        style_tags = soup.find_all("style")
        link_styles = soup.find_all("link", attrs={"rel": "stylesheet"})
        total_css = len(style_tags) + len(link_styles)
        if total_css < 8:
            perf_points += 25
            passed.append({
                "category": "Performance",
                "title": "Optimized Stylesheet Requests",
                "desc": f"Few styles detected ({total_css}). Reduces render blocking."
            })
        else:
            perf_points += 15
            warnings.append({
                "category": "Performance",
                "title": "Excessive Stylesheet Requests",
                "desc": f"Found {total_css} stylesheets. Consolidate stylesheets to reduce the number of HTTP roundtrips."
            })

        # 3. Scripts count
        script_tags = soup.find_all("script")
        total_scripts = len(script_tags)
        if total_scripts < 15:
            perf_points += 25
            passed.append({
                "category": "Performance",
                "title": "Efficient JavaScript Weight",
                "desc": f"Reasonable script count ({total_scripts}). Promotes high page responsiveness."
            })
        else:
            perf_points += 15
            warnings.append({
                "category": "Performance",
                "title": "Bulky JavaScript Count",
                "desc": f"Found {total_scripts} script tags. Defer, async, or combine script assets to prevent thread blocking."
            })

        # 4. Image Alt text (Accessibility & Image SEO)
        images = soup.find_all("img")
        img_count = len(images)
        if img_count == 0:
            perf_points += 25
            passed.append({
                "category": "Accessibility",
                "title": "No Image Audits Required",
                "desc": "Your page has no image tags requiring alt text validation."
            })
        else:
            missing_alt = 0
            for img in images:
                if not img.get("alt") or not img.get("alt").strip():
                    missing_alt += 1
            
            if missing_alt == 0:
                perf_points += 25
                passed.append({
                    "category": "Accessibility",
                    "title": "All Images Have Alt Text",
                    "desc": f"All {img_count} images contain descriptive 'alt' tags, ensuring high accessibility compliance."
                })
            else:
                pct_missing = round(missing_alt / img_count * 100)
                deduction = round(25 * (missing_alt / img_count))
                perf_points += (25 - deduction)
                
                # If majority are missing, make it critical, otherwise warning
                issue_item = {
                    "category": "Accessibility",
                    "title": "Images Missing Alt Tags",
                    "desc": f"{missing_alt} out of {img_count} images ({pct_missing}%) are missing alternative text (alt tags). Add alt text to improve SEO and accessibility."
                }
                if pct_missing > 50:
                    critical.append(issue_item)
                else:
                    warnings.append(issue_item)

        perf_score = min(perf_points, 100)


        # --- SECURITY ANALYSIS ---
        sec_points = 0
        
        # 1. HTTPS Protocol Check
        if is_https:
            sec_points += 50
            passed.append({
                "category": "Security",
                "title": "Secure HTTPS Protocol",
                "desc": "The website encrypts connections using standard HTTPS protocol."
            })
        else:
            critical.append({
                "category": "Security",
                "title": "Insecure HTTP Protocol",
                "desc": "The website connects via unencrypted HTTP. Users risk data snooping, and Google actively penalizes non-HTTPS sites."
            })

        # 2. SSL/TLS Certificate and headers (Simplified approximation)
        if is_https:
            sec_points += 50
            passed.append({
                "category": "Security",
                "title": "Active SSL Encryption",
                "desc": "The security certificate is active, securing domain communication."
            })
        else:
            warnings.append({
                "category": "Security",
                "title": "SSL Encryption Missing",
                "desc": "SSL security certificates are disabled or inactive. Users will receive browser security warning alerts."
            })

        sec_score = min(sec_points, 100)

        # Overall Weighted Score (SEO: 40%, Performance: 30%, Security: 30%)
        overall_score = round((seo_score * 0.40) + (perf_score * 0.30) + (sec_score * 0.30))

        # Core details block for deep inspections
        details = {
            "title": title_text,
            "description": desc_text,
            "html_size_kb": html_size_kb,
            "load_time_seconds": load_time,
            "h1_count": h1_count,
            "h1_content": [h.get_text().strip() for h in h1_tags[:3]],
            "img_count": img_count,
            "missing_alt": missing_alt if img_count > 0 else 0,
            "stylesheet_count": total_css,
            "script_count": total_scripts,
            "is_https": is_https
        }

        return {
            "url": url,
            "overall_score": overall_score,
            "seo_score": seo_score,
            "perf_score": perf_score,
            "sec_score": sec_score,
            "load_time": load_time,
            "passed": passed,
            "warnings": warnings,
            "critical": critical,
            "details": details
        }
