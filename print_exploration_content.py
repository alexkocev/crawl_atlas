"""
exploration_print_content.py
────────────────────────────
Dump raw website data for LLM inspection — no tech detection, no field analysis.
Usage:  python exploration_print_content.py [URL]
        If no URL is provided, you will be prompted to enter one.
"""

import asyncio
import re
import sys
from typing import List
from urllib.parse import urljoin, urlparse

import aiohttp
from playwright.async_api import async_playwright

SECTION = "═" * 70


def normalize_url(url: str) -> str:
    """Ensure URL has a scheme (https://) for valid navigation."""
    u = url.strip()
    if not u:
        return u
    if not u.startswith(("http://", "https://")):
        return f"https://{u}"
    return u


def section(title: str):
    print(f"\n{SECTION}")
    print(f"  {title}")
    print(SECTION)


async def fetch_robots_txt(base_url: str) -> str:
    """Fetch raw robots.txt content."""
    try:
        parsed = urlparse(base_url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        headers = {"User-Agent": "Mozilla/5.0"}
        async with aiohttp.ClientSession() as session:
            async with session.get(
                robots_url, timeout=aiohttp.ClientTimeout(total=10), headers=headers
            ) as resp:
                return await resp.text(errors="ignore")
    except Exception as e:
        return f"(failed to fetch: {e})"


def extract_meta_tags(html: str) -> List[dict]:
    """Extract all <meta> tags: name, property, content."""
    metas = []
    # Match <meta ...> with name=, property=, content=
    for m in re.finditer(
        r'<meta\s+([^>]+)>',
        html,
        re.IGNORECASE | re.DOTALL
    ):
        attrs = m.group(1)
        name = re.search(r'name\s*=\s*["\']([^"\']*)["\']', attrs, re.I)
        prop = re.search(r'property\s*=\s*["\']([^"\']*)["\']', attrs, re.I)
        content = re.search(r'content\s*=\s*["\']([^"\']*)["\']', attrs, re.I)
        metas.append({
            "name": name.group(1) if name else None,
            "property": prop.group(1) if prop else None,
            "content": content.group(1) if content else None,
        })
    return metas


def extract_jsonld_blocks(html: str) -> List[str]:
    """Extract raw JSON from <script type="application/ld+json"> blocks."""
    blocks = []
    for m in re.finditer(
        r'<script[^>]*type\s*=\s*["\']application/ld\+json["\'][^>]*>([\s\S]*?)</script>',
        html,
        re.IGNORECASE
    ):
        blocks.append(m.group(1).strip())
    return blocks


async def dump_page_raw(page, url: str) -> tuple:
    """Navigate to URL, return (html, visible_text)."""
    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(1500)
        html = await page.content()
        try:
            page_text = await page.inner_text("body")
        except Exception:
            page_text = ""
        return html, page_text
    except Exception as e:
        return f"(failed to load: {e})", ""


async def explore(url: str):
    url = normalize_url(url)
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    print(f"\n{SECTION}")
    print(f"  🔍 RAW DATA DUMP: {url}")
    print(SECTION)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # ── ALL OUTGOING NETWORK REQUESTS ───────────────────────────────────
        all_requests = []

        def on_request(request):
            all_requests.append(request.url)

        page.on("request", on_request)

        # ── LOAD HOMEPAGE ───────────────────────────────────────────────────
        print("\n⏳ Loading homepage...")
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)

        html = await page.content()
        page_text = await page.inner_text("body")

        # ── 1. FULL HTML (first 3000 chars) ─────────────────────────────────
        section("📄 HOMEPAGE HTML (first 3000 chars)")
        print(html[:3000])
        print(f"\n  ... [{len(html):,} total chars]")

        # ── 2. HTTP RESPONSE HEADERS (all of them) ───────────────────────────
        section("📡 HTTP RESPONSE HEADERS (all)")
        if response:
            headers = await response.all_headers()
            for k, v in headers.items():
                print(f"  {k}: {v}")
            print(f"\n  HTTP Status: {response.status}")
        else:
            print("  (no response captured)")

        # ── 3. ALL COOKIES ──────────────────────────────────────────────────
        section("🍪 COOKIES (name, value, domain)")
        cookies = await context.cookies()
        for c in cookies:
            print(f"  name={c.get('name')}  value={c.get('value')}  domain={c.get('domain')}")
        if not cookies:
            print("  (none)")

        # ── 4. SCRIPT SRC URLs ───────────────────────────────────────────────
        section("📜 SCRIPT SRC URLs")
        script_elems = await page.query_selector_all("script[src]")
        for s in script_elems:
            src = await s.get_attribute("src")
            if src:
                print(f"  {src}")
        if not script_elems:
            print("  (none)")

        # ── 5. IFRAME SRC URLs ───────────────────────────────────────────────
        section("🖼️ IFRAME SRC URLs")
        iframe_elems = await page.query_selector_all("iframe[src]")
        for i in iframe_elems:
            src = await i.get_attribute("src")
            if src:
                print(f"  {src}")
        if not iframe_elems:
            print("  (none)")

        # ── 6. ALL META TAGS ─────────────────────────────────────────────────
        section("🏷️ META TAGS (name, property, content)")
        metas = extract_meta_tags(html)
        for m in metas:
            parts = []
            if m["name"]:
                parts.append(f"name={m['name']}")
            if m["property"]:
                parts.append(f"property={m['property']}")
            if m["content"]:
                parts.append(f"content={m['content']}")
            print(f"  {' | '.join(parts)}")
        if not metas:
            print("  (none)")

        # ── 7. JSON-LD BLOCKS ───────────────────────────────────────────────
        section("📋 JSON-LD BLOCKS (raw)")
        jsonld = extract_jsonld_blocks(html)
        for i, block in enumerate(jsonld):
            print(f"\n  --- Block {i + 1} ---")
            print(block)
        if not jsonld:
            print("  (none)")

        # ── 8. ROBOTS.TXT ────────────────────────────────────────────────────
        section("🤖 ROBOTS.TXT (raw)")
        robots_content = await fetch_robots_txt(url)
        print(robots_content)

        # ── 9. VISIBLE PAGE TEXT ────────────────────────────────────────────
        section("📝 VISIBLE PAGE TEXT (body)")
        print(page_text)

        # ── 10. ALL <a href> LINKS ───────────────────────────────────────────
        section("🔗 ALL <a href> LINKS")
        link_elems = await page.query_selector_all("a[href]")
        for a in link_elems:
            href = await a.get_attribute("href")
            if href:
                print(f"  {href}")
        if not link_elems:
            print("  (none)")

        # ── 11. TEL: AND MAILTO: LINKS ───────────────────────────────────────
        section("📞 TEL: AND MAILTO: LINKS")
        tel_mailto = []
        for a in link_elems:
            href = await a.get_attribute("href")
            if href and (href.lower().startswith("tel:") or href.lower().startswith("mailto:")):
                tel_mailto.append(href)
        for link in tel_mailto:
            print(f"  {link}")
        if not tel_mailto:
            print("  (none)")

        # ── /contact PAGE ───────────────────────────────────────────────────
        section("📄 /contact PAGE")
        contact_url = urljoin(base, "/contact")
        contact_html, contact_text = await dump_page_raw(page, contact_url)
        print(f"\n  URL: {contact_url}")
        print(f"\n  --- HTML (first 3000 chars) ---")
        print(contact_html[:3000] if isinstance(contact_html, str) else contact_html)
        if isinstance(contact_html, str) and len(contact_html) > 3000:
            print(f"\n  ... [{len(contact_html):,} total chars]")
        print(f"\n  --- Visible text ---")
        print(contact_text)

        # ── /about PAGE ──────────────────────────────────────────────────────
        section("📄 /about PAGE")
        about_url = urljoin(base, "/about")
        about_html, about_text = await dump_page_raw(page, about_url)
        print(f"\n  URL: {about_url}")
        print(f"\n  --- HTML (first 3000 chars) ---")
        print(about_html[:3000] if isinstance(about_html, str) else about_html)
        if isinstance(about_html, str) and len(about_html) > 3000:
            print(f"\n  ... [{len(about_html):,} total chars]")
        print(f"\n  --- Visible text ---")
        print(about_text)

        # ── ALL OUTGOING NETWORK REQUESTS (homepage + contact + about) ────────
        section("🌐 ALL OUTGOING NETWORK REQUESTS")
        for req_url in all_requests:
            print(f"  {req_url}")
        if not all_requests:
            print("  (none captured)")

        await browser.close()

    print(f"\n{SECTION}")
    print("  ✅ RAW DATA DUMP COMPLETE")
    print(SECTION)


OUTPUT_FILE = "printed_exploration_content.txt"


if __name__ == "__main__":
    if len(sys.argv) > 1:
        target = sys.argv[1]
    else:
        target = input("Enter URL to scrape: ").strip()
        if not target:
            print("No URL provided. Exiting.")
            sys.exit(1)

    orig_stdout = sys.stdout
    with open(OUTPUT_FILE, "w") as f:
        sys.stdout = f
        try:
            asyncio.run(explore(target))
        finally:
            sys.stdout = orig_stdout
    print(f"Output written to {OUTPUT_FILE}")
