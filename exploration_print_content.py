"""
Extract all possible data from a URL and write to exploration_printed_content.txt.
Usage: python exploration_print_content.py <URL>
Or run without arguments to paste URL interactively.
"""

import asyncio
import json
import re
import sys
from urllib.parse import urlparse

import dns.resolver
from playwright.async_api import async_playwright

from main_clinics import scrape_clinic
from core import extract_email, extract_phone

OUTPUT_FILE = "exploration_printed_content.txt"


async def get_dns_records(domain: str) -> dict:
    """Get all DNS records for a domain."""
    domain = domain.replace("www.", "")
    records = {
        "MX": [],
        "A": [],
        "AAAA": [],
        "TXT": [],
        "CNAME": [],
        "NS": [],
    }
    loop = asyncio.get_event_loop()

    for record_type in ["MX", "A", "AAAA", "TXT", "CNAME", "NS"]:
        try:
            if record_type == "MX":
                answers = await loop.run_in_executor(
                    None, lambda: dns.resolver.resolve(domain, "MX")
                )
                records["MX"] = [f"{r.preference} {r.exchange}" for r in answers]
            elif record_type == "TXT":
                answers = await loop.run_in_executor(
                    None, lambda: dns.resolver.resolve(domain, "TXT")
                )
                records["TXT"] = [str(r).strip('"') for r in answers]
            else:
                answers = await loop.run_in_executor(
                    None, lambda rt=record_type: dns.resolver.resolve(domain, rt)
                )
                records[record_type] = [str(r) for r in answers]
        except Exception as e:
            records[record_type] = [f"Error: {str(e)}"]

    return records


def extract_json_ld(html: str) -> list:
    """Extract JSON-LD from script type='application/ld+json' tags."""
    found = []
    pattern = r'<script[^>]*type\s*=\s*["\']application/ld\+json["\'][^>]*>(.*?)</script>'
    for match in re.finditer(pattern, html, re.DOTALL | re.IGNORECASE):
        try:
            data = json.loads(match.group(1).strip())
            found.append(data)
        except json.JSONDecodeError:
            found.append({"raw": match.group(1).strip()[:500]})
    return found


def extract_meta_tags(html: str) -> list:
    """Extract all meta tags as dicts."""
    found = []
    pattern = r'<meta\s+([^>]+)>'
    for match in re.finditer(pattern, html, re.IGNORECASE):
        attrs = {}
        for attr in re.finditer(r'(\w+)\s*=\s*["\']([^"\']*)["\']', match.group(1)):
            attrs[attr.group(1).lower()] = attr.group(2)
        if attrs:
            found.append(attrs)
    return found


def extract_link_tags(html: str) -> list:
    """Extract link tags (stylesheet, canonical, etc.)."""
    found = []
    pattern = r'<link\s+([^>]+)>'
    for match in re.finditer(pattern, html, re.IGNORECASE):
        attrs = {}
        for attr in re.finditer(r'(\w+)\s*=\s*["\']([^"\']*)["\']', match.group(1)):
            attrs[attr.group(1).lower()] = attr.group(2)
        if attrs:
            found.append(attrs)
    return found


def extract_script_srcs(html: str) -> list:
    """Extract script src URLs."""
    found = []
    pattern = r'<script[^>]*src\s*=\s*["\']([^"\']+)["\'][^>]*>'
    for match in re.finditer(pattern, html, re.IGNORECASE):
        found.append(match.group(1))
    return found


def extract_iframe_srcs(html: str) -> list:
    """Extract iframe src URLs."""
    found = []
    pattern = r'<iframe[^>]*src\s*=\s*["\']([^"\']+)["\'][^>]*>'
    for match in re.finditer(pattern, html, re.IGNORECASE):
        found.append(match.group(1))
    return found


def extract_inline_scripts(html: str) -> list:
    """Extract inline script content (first 500 chars each)."""
    found = []
    pattern = r'<script[^>]*>(.*?)</script>'
    for match in re.finditer(pattern, html, re.DOTALL | re.IGNORECASE):
        content = match.group(1).strip()
        if content and "application/ld+json" not in match.group(0).lower():
            found.append(content[:1000] + ("..." if len(content) > 1000 else ""))
    return found


def format_section(title: str, content: str, char: str = "━") -> str:
    """Format a section with header."""
    return f"\n{char * 80}\n{title}\n{char * 80}\n\n{content}"


async def collect_all_data(url: str) -> str:
    """Scrape URL and collect all extractable data into a string."""
    lines = []
    domain = urlparse(url).netloc.replace("www.", "")

    # Collected data
    response_headers = {}
    html = ""
    page_text = ""
    cookies = []
    json_responses = []
    scrape_result = {}

    async def handle_response(response):
        """Capture main doc headers and JSON API responses."""
        try:
            req_url = response.url
            if req_url == url or response.request.url == url:
                if hasattr(response, "all_headers"):
                    response_headers.update(response.all_headers())
            # Capture JSON responses (XHR/fetch)
            ct = response.headers.get("content-type", "").lower()
            if "application/json" in ct and len(json_responses) < 30:
                try:
                    body = await response.text()
                    if len(body) < 10000:
                        data = json.loads(body)
                        json_responses.append({"url": req_url, "body": data})
                    else:
                        json_responses.append({"url": req_url, "body": body[:2000] + "..."})
                except Exception:
                    pass
        except Exception:
            pass

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()
        page.on("response", handle_response)

        try:
            response = await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)

            if response:
                try:
                    response_headers.update(response.all_headers())
                except Exception:
                    pass

            html = await page.content()
            page_text = await page.inner_text("body") if await page.query_selector("body") else ""

            cookies = await context.cookies()

            scrape_result = await scrape_clinic(browser, url)

        except Exception as e:
            lines.append(f"⚠️  Warning: {e}\n")
        finally:
            await context.close()
            await browser.close()

    # DNS records
    dns_records = await get_dns_records(domain)

    # Build output
    lines.append("=" * 80)
    lines.append(f"EXPLORATION PRINTED CONTENT: {url}")
    lines.append("=" * 80)

    # 1. Scrape results summary
    lines.append(format_section("📊 SCRAPING RESULTS (from scrape_clinic)", ""))
    for k, v in scrape_result.items():
        if k not in ("found_emails",) and v is not None:
            lines.append(f"  {k}: {v}")
    if scrape_result.get("found_emails"):
        lines.append(f"  found_emails: {scrape_result['found_emails']}")

    # 2. HTTP Response Headers
    lines.append(format_section("📋 HTTP RESPONSE HEADERS", ""))
    for k, v in sorted(response_headers.items()):
        lines.append(f"  {k}: {v}")

    # 3. Cookies
    lines.append(format_section("🍪 COOKIES", ""))
    for c in cookies:
        lines.append(f"  {c.get('name')}: {c.get('value', '')[:80]}...")
    if not cookies:
        lines.append("  (none)")

    # 4. DNS Records
    lines.append(format_section("🌐 DNS RECORDS", f"Domain: {domain}\n"))
    for rt, vals in dns_records.items():
        lines.append(f"  {rt}:")
        for v in vals:
            lines.append(f"    {v}")
        lines.append("")

    # 5. Meta tags
    meta_tags = extract_meta_tags(html)
    lines.append(format_section("📌 META TAGS", ""))
    for m in meta_tags:
        lines.append(f"  {json.dumps(m)}")
    if not meta_tags:
        lines.append("  (none)")

    # 6. Link tags
    link_tags = extract_link_tags(html)
    lines.append(format_section("🔗 LINK TAGS", ""))
    for l in link_tags[:50]:  # Limit to 50
        lines.append(f"  {json.dumps(l)}")
    if len(link_tags) > 50:
        lines.append(f"  ... and {len(link_tags) - 50} more")
    if not link_tags:
        lines.append("  (none)")

    # 7. Script sources
    script_srcs = extract_script_srcs(html)
    lines.append(format_section("📜 SCRIPT SRC URLs", ""))
    for s in script_srcs:
        lines.append(f"  {s}")
    if not script_srcs:
        lines.append("  (none)")

    # 8. Iframe sources
    iframe_srcs = extract_iframe_srcs(html)
    lines.append(format_section("🖼️ IFRAME SRC URLs", ""))
    for s in iframe_srcs:
        lines.append(f"  {s}")
    if not iframe_srcs:
        lines.append("  (none)")

    # 9. JSON-LD
    json_ld = extract_json_ld(html)
    lines.append(format_section("📦 JSON-LD (Structured Data)", ""))
    for i, j in enumerate(json_ld):
        lines.append(f"  --- Block {i + 1} ---")
        lines.append(json.dumps(j, indent=2, default=str))
    if not json_ld:
        lines.append("  (none)")

    # 10. JSON API responses (XHR/fetch)
    lines.append(format_section("📡 JSON API RESPONSES (XHR/Fetch)", ""))
    for i, jr in enumerate(json_responses):
        lines.append(f"  --- Response {i + 1}: {jr.get('url', '')[:80]} ---")
        body = jr.get("body")
        if isinstance(body, (dict, list)):
            lines.append(json.dumps(body, indent=2, default=str))
        else:
            lines.append(str(body))
    if not json_responses:
        lines.append("  (none)")

    # 11. Extracted contact info
    email = extract_email(page_text)
    phone = extract_phone(page_text)
    lines.append(format_section("📮 EXTRACTED CONTACT INFO", ""))
    lines.append(f"  Email: {email or '(none)'}")
    lines.append(f"  Phone: {phone or '(none)'}")

    # 12. Page text (visible)
    lines.append(format_section("📝 PAGE TEXT (Visible Content)", ""))
    lines.append(page_text[:15000])
    if len(page_text) > 15000:
        lines.append(f"\n... [truncated, total {len(page_text)} chars]")

    # 13. Full HTML
    lines.append(format_section("📄 FULL HTML", ""))
    lines.append(html)
    lines.append(f"\n[Total HTML length: {len(html)} chars]")

    return "\n".join(lines)


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        print("Enter URL to scrape (or paste and press Enter):")
        url = input().strip()

    if not url:
        print("❌ Error: No URL provided")
        sys.exit(1)

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    print(f"\n🔍 Extracting all data from: {url}")
    print(f"   Writing to: {OUTPUT_FILE}\n")

    content = asyncio.run(collect_all_data(url))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"✅ Done! Output written to {OUTPUT_FILE}")
    print(f"   Total size: {len(content):,} characters")


if __name__ == "__main__":
    main()
