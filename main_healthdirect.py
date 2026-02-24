"""
main_healthdirect.py
────────────────────
Scrapes https://www.healthdirect.gov.au/australian-health-services
to collect all Australian clinics (name, website URL, address, phone).

Strategy:
  1. Iterate through AU_POSTCODES list (one per major suburb/city)
  2. On the search results page, paginate and collect all detail-page links
  3. Visit each detail page and extract clinic data
  4. Deduplicate by website URL before saving to Google Sheets

Anti-bot measures:
  - Randomized delays (3–8s between pages, 1–3s between clicks)
  - Random User-Agent rotation
  - Random viewport sizes
  - Human-like mouse movement before key interactions
  - Randomized scroll behaviour
"""

import asyncio
import random
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from core import (
    init_google_sheets,
    get_current_timestamp,
    _standardize_phone,
    _standardize_state,
    _standardize_country,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

SHEET_KEY_OR_URL = "https://docs.google.com/spreadsheets/d/1y9zzp1J1Fn60UKYN0RkTsSQcHcMb1mi2cD4NH8OfAF4/edit?gid=1242976304#gid=1242976304"
SERVICE_ACCOUNT_FILE = "yoluko-frontdesk-3d208271a3c0.json"
SHEET_TAB_NAME = "main_healthdirect"  # Tab will be created if it doesn't exist

BASE_URL = "https://www.healthdirect.gov.au/australian-health-services"

# Safety cap: max result pages to paginate per search (healthdirect shows ~10 results/page,
# so 20 pages = up to 200 clinics per postcode — more than enough).
MAX_PAGES_PER_SEARCH = 20

# Services to search — each postcode is searched once per service.
# Comment out any you don't need to reduce run time.
SERVICES_TO_SEARCH = [
    "GP (General practice)",
    "Physiotherapy",
    "Psychology",
    "Dental",
    "Pharmacy",
]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

VIEWPORTS = [
    {"width": 1440, "height": 900},
    {"width": 1280, "height": 800},
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
]

# Representative AU postcodes — covers all states/territories, major + regional cities.
# Add more for denser coverage; remove to run a quick test.
AU_POSTCODES = [
    # NSW
    "2000", "2010", "2020", "2060", "2100", "2150", "2200", "2300",
    "2350", "2500", "2600", "2640", "2680", "2750", "2800",
    # VIC
    "3000", "3030", "3065", "3121", "3175", "3200", "3280", "3350",
    "3500", "3550", "3630", "3690", "3750", "3800",
    # QLD
    "4000", "4030", "4060", "4101", "4210", "4350", "4551", "4700",
    "4740", "4820", "4870",
    # SA
    "5000", "5010", "5042", "5107", "5200", "5290",
    # WA
    "6000", "6010", "6050", "6100", "6150", "6210", "6330", "6430", "6530",
    # TAS
    "7000", "7005", "7250", "7320",
    # ACT
    "2600", "2601", "2615",
    # NT
    "0800", "0810", "0870",
]

# Google Sheets column headers
SHEET_HEADERS = [
    "website_url",
    "clinic_name",
    "phone",
    "street",
    "city",
    "state",
    "postcode",
    "country",
    "healthdirect_url",
    "scraped_at",
]


# ─────────────────────────────────────────────────────────────────────────────
# ANTI-BOT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

async def human_delay(min_s: float = 3.0, max_s: float = 8.0):
    """Pause like a human would between page loads."""
    await asyncio.sleep(random.uniform(min_s, max_s))


async def short_delay(min_s: float = 0.8, max_s: float = 2.5):
    """Short pause between clicks / form interactions."""
    await asyncio.sleep(random.uniform(min_s, max_s))


async def human_mouse_move(page: Page):
    """Move mouse in a random arc to simulate human behaviour."""
    try:
        vp = page.viewport_size or {"width": 1280, "height": 800}
        x = random.randint(200, vp["width"] - 200)
        y = random.randint(200, vp["height"] - 200)
        await page.mouse.move(x, y, steps=random.randint(8, 20))
    except Exception:
        pass


async def human_scroll(page: Page):
    """Scroll down a random amount, then back a bit, like skimming content."""
    try:
        scroll_down = random.randint(400, 900)
        scroll_back = random.randint(50, 200)
        await page.evaluate(f"window.scrollBy(0, {scroll_down})")
        await asyncio.sleep(random.uniform(0.4, 1.0))
        await page.evaluate(f"window.scrollBy(0, -{scroll_back})")
    except Exception:
        pass


def random_context_options() -> dict:
    """Return random browser context options to avoid fingerprinting."""
    return {
        "user_agent": random.choice(USER_AGENTS),
        "viewport": random.choice(VIEWPORTS),
        "locale": "en-AU",
        "timezone_id": "Australia/Sydney",
        "extra_http_headers": {
            "Accept-Language": "en-AU,en-GB;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACTION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def extract_phone_from_text(text: str) -> Optional[str]:
    """Pull first plausible AU/international phone from a block of text."""
    patterns = [
        r'\+61[\s\-]?\d[\s\d]{8,12}',
        r'\(0\d\)\s?\d{4}\s?\d{4}',
        r'0\d[\s\-]?\d{4}[\s\-]?\d{4}',
        r'0[45]\d{2}[\s\-]?\d{3}[\s\-]?\d{3}',
        r'1[38]00[\s\-]?\d{3}[\s\-]?\d{3}',
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            return _standardize_phone(m.group(0))
    return None


def extract_website_from_text(text: str) -> Optional[str]:
    """Pull first URL that looks like a clinic website from text."""
    m = re.search(
        r'https?://(?!www\.healthdirect\.gov\.au)[^\s\'"<>]{5,}',
        text,
    )
    if m:
        return m.group(0).rstrip(".,;)")
    return None


def parse_address_text(raw: str) -> dict:
    """
    Attempt to split a free-text address into street / city / state / postcode.
    Falls back gracefully when the format is unexpected.
    Expected AU format: "123 Example St, Suburb STATE 1234"
    """
    addr = {"street": "", "city": "", "state": "", "postcode": "", "country": "Australia"}
    if not raw:
        return addr

    raw = raw.strip()

    # Try to extract postcode (last 4 digits)
    postcode_m = re.search(r'\b(\d{4})\b', raw)
    if postcode_m:
        addr["postcode"] = postcode_m.group(1)

    # Try to extract state
    state_m = re.search(r'\b(NSW|VIC|QLD|SA|WA|TAS|ACT|NT)\b', raw, re.IGNORECASE)
    if state_m:
        addr["state"] = _standardize_state(state_m.group(1))

    # Split on comma: first part = street, rest = city/state/postcode
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) >= 2:
        addr["street"] = parts[0]
        city_part = parts[1]
        # Remove state + postcode from city_part
        city_clean = re.sub(
            r'\b(?:NSW|VIC|QLD|SA|WA|TAS|ACT|NT)\b', "", city_part, flags=re.IGNORECASE
        )
        city_clean = re.sub(r'\b\d{4}\b', "", city_clean).strip().strip(",").strip()
        addr["city"] = city_clean
    else:
        addr["street"] = raw

    return addr


# ─────────────────────────────────────────────────────────────────────────────
# HEALTHDIRECT SCRAPING
# ─────────────────────────────────────────────────────────────────────────────

async def search_clinics_for_postcode(page: Page, postcode: str, service: str = "GP (General practice)") -> list:
    """
    Search healthdirect using BOTH fields:
      1. "Search by service"   → type and select service (e.g. "GP (General practice)")
      2. "Enter suburb or postcode" → type postcode and select first autocomplete suggestion
      3. Click Search

    Returns list of detail-page URLs found across all result pages.
    """
    detail_urls = []

    print(f"  🔎 Searching [{service}] in postcode {postcode}...")

    try:
        await page.goto(BASE_URL, timeout=30000, wait_until="domcontentloaded")
        await short_delay(1.5, 3.0)
        await human_mouse_move(page)

        # ── Step 1: Fill "Search by service" field ─────────────────────────
        # The field shows "Popular services" dropdown when clicked
        service_input = await page.wait_for_selector(
            'input[placeholder*="service"], input[aria-label*="service"], '
            'input[id*="service"], input[name*="service"], '
            # Fallback: first input on the page (the service field is always first)
            'form input:first-of-type',
            timeout=10000,
        )
        await service_input.click()
        await short_delay(0.4, 0.9)

        # Clear and type service name character by character
        await service_input.fill("")
        await asyncio.sleep(0.2)
        for char in service:
            await service_input.type(char, delay=random.randint(55, 140))
        await short_delay(0.8, 1.5)

        # Click the matching suggestion in the dropdown
        # The dropdown shows items like "GP (General practice)", "Psychiatry", etc.
        try:
            # Try to find an exact or partial text match in the suggestion list
            suggestion = await page.wait_for_selector(
                f'[role="option"]:has-text("{service}"), '
                f'[class*="suggestion"]:has-text("{service}"), '
                f'[class*="autocomplete"] li:has-text("{service}"), '
                f'ul li:has-text("{service}")',
                timeout=4000,
            )
            await suggestion.click()
            print(f"    ✅ Selected service: {service}")
        except Exception:
            # Dropdown did not appear or text match failed — press Enter and continue
            await service_input.press("Enter")
            print(f"    ⚠️ No dropdown match for service — pressed Enter")
        await short_delay(0.5, 1.2)

        # ── Step 2: Fill "Enter suburb or postcode" field ──────────────────
        location_input = await page.wait_for_selector(
            'input[placeholder*="suburb"], input[placeholder*="postcode"], '
            'input[aria-label*="suburb"], input[aria-label*="postcode"], '
            'input[id*="location"], input[name*="location"]',
            timeout=10000,
        )
        await location_input.click()
        await short_delay(0.3, 0.7)
        await location_input.fill("")
        await asyncio.sleep(0.2)
        for char in postcode:
            await location_input.type(char, delay=random.randint(60, 160))
        await short_delay(0.8, 1.5)

        # Click first autocomplete suburb suggestion
        try:
            suburb_suggestion = await page.wait_for_selector(
                '[role="option"], [class*="suggestion"] li, [class*="autocomplete"] li, ul[role="listbox"] li',
                timeout=4000,
            )
            await suburb_suggestion.click()
            print(f"    ✅ Selected suburb for postcode {postcode}")
        except Exception:
            await location_input.press("Enter")
            print(f"    ⚠️ No suburb dropdown — pressed Enter")
        await short_delay(0.5, 1.0)

        # ── Step 3: Click Search button ────────────────────────────────────
        try:
            search_btn = await page.wait_for_selector(
                'button:has-text("Search"), button[type="submit"], input[type="submit"]',
                timeout=5000,
            )
            await human_mouse_move(page)
            await search_btn.click()
        except Exception:
            # May have already submitted
            pass

        await page.wait_for_load_state("domcontentloaded")
        await short_delay(2.0, 4.0)

        # ── Confirm results loaded (check for results count text) ──────────
        try:
            await page.wait_for_selector(
                '[class*="results"], [class*="Result"], h2, h3',
                timeout=8000,
            )
        except Exception:
            print(f"    ⚠️ Results container not found for {postcode} — may be empty")

        # ── Paginate through results ───────────────────────────────────────
        page_num = 1
        prev_url = ""
        prev_link_count = -1

        while page_num <= MAX_PAGES_PER_SEARCH:
            current_url = page.url
            print(f"    📄 Results page {page_num} for {postcode} ({current_url.split('?')[0]})")
            await human_scroll(page)
            await short_delay(0.5, 1.2)

            # Collect all clinic detail links on this results page
            links = await page.query_selector_all(
                'a[href*="/service/"], a[href*="/clinic/"], '
                'a:has-text("View details"), a:has-text("More details"), '
                'a:has-text("See details")'
            )
            new_on_page = 0
            for link in links:
                href = await link.get_attribute("href")
                if href:
                    full = urljoin("https://www.healthdirect.gov.au", href)
                    if full not in detail_urls:
                        detail_urls.append(full)
                        new_on_page += 1

            print(f"      → {len(links)} links found, {new_on_page} new")

            # Stop if no links at all (empty results page)
            if len(links) == 0:
                print(f"    ✋ No links on page {page_num} — stopping pagination")
                break

            # Stop if same number of links as last page AND url didn't change
            # (means the "Next" click didn't actually navigate)
            if len(links) == prev_link_count and current_url == prev_url:
                print(f"    ✋ Page didn't change after Next click — stopping")
                break

            prev_url = current_url
            prev_link_count = len(links)

            # ── Find and click the Next button ────────────────────────────
            next_clicked = False
            try:
                # Strategy A: aria-label="Next" or "Next page"
                next_btn = await page.query_selector(
                    '[aria-label="Next page"], [aria-label="Next"], '
                    'button[aria-label*="next" i], a[aria-label*="next" i]'
                )
                if next_btn:
                    is_disabled = (
                        await next_btn.get_attribute("aria-disabled") == "true"
                        or await next_btn.get_attribute("disabled") is not None
                    )
                    if not is_disabled:
                        await human_mouse_move(page)
                        await next_btn.click()
                        next_clicked = True

                # Strategy B: look for a ">" or "Next" text link NOT in the service/suburb fields
                if not next_clicked:
                    all_btns = await page.query_selector_all(
                        'button, a[href]'
                    )
                    for btn in all_btns:
                        try:
                            txt = (await btn.inner_text()).strip()
                            if txt.lower() in ("next", ">", "›", "→"):
                                # Make sure it's not hidden
                                is_visible = await btn.is_visible()
                                if is_visible:
                                    await human_mouse_move(page)
                                    await btn.click()
                                    next_clicked = True
                                    break
                        except Exception:
                            continue

                if not next_clicked:
                    print(f"    ✋ No Next button found — end of results for {postcode}")
                    break

                # Wait for the page to update after clicking Next
                await page.wait_for_load_state("domcontentloaded")
                await short_delay(2.5, 5.0)
                page_num += 1

            except Exception as e:
                print(f"    ✋ Pagination stopped: {e}")
                break

        if page_num > MAX_PAGES_PER_SEARCH:
            print(f"    ⚠️ Hit MAX_PAGES_PER_SEARCH={MAX_PAGES_PER_SEARCH} cap for {postcode}")

    except PlaywrightTimeoutError:
        print(f"    ⏱️ Timeout on postcode {postcode} — skipping")
    except Exception as e:
        print(f"    ❌ Error on postcode {postcode}: {e}")

    print(f"    ✅ Found {len(detail_urls)} clinic links for {postcode}")
    return detail_urls


async def scrape_detail_page(page: Page, detail_url: str) -> Optional[dict]:
    """
    Visit a healthdirect clinic detail page and extract:
    - clinic name
    - website URL
    - phone
    - address (street / city / state / postcode)
    Returns None on failure.
    """
    try:
        await page.goto(detail_url, timeout=20000, wait_until="domcontentloaded")
        await short_delay(1.5, 3.5)
        await human_scroll(page)

        html = await page.content()
        page_text = await page.inner_text("body")

        # ── Clinic name ────────────────────────────────────────────────────
        clinic_name = ""
        try:
            h1 = await page.query_selector("h1")
            if h1:
                clinic_name = (await h1.inner_text()).strip()
        except Exception:
            pass

        # ── Website URL ────────────────────────────────────────────────────
        website_url = ""
        try:
            # Healthdirect usually renders external website as an <a> with "Website" text
            website_link = await page.query_selector(
                'a[href^="http"]:has-text("Website"), '
                'a[href^="http"][class*="website"], '
                'a[href^="http"][class*="url"]'
            )
            if website_link:
                href = await website_link.get_attribute("href")
                if href and "healthdirect.gov.au" not in href:
                    website_url = href.strip()
        except Exception:
            pass

        # Fallback: regex scan of page text for external URL
        if not website_url:
            website_url = extract_website_from_text(page_text) or ""

        # ── Phone ──────────────────────────────────────────────────────────
        phone = ""
        try:
            tel_link = await page.query_selector('a[href^="tel:"]')
            if tel_link:
                tel_href = await tel_link.get_attribute("href")
                phone = _standardize_phone(tel_href.replace("tel:", "").strip())
        except Exception:
            pass

        if not phone:
            phone = extract_phone_from_text(page_text) or ""

        # ── Address ────────────────────────────────────────────────────────
        raw_address = ""
        try:
            # Try schema.org address elements or common address selectors
            addr_el = await page.query_selector(
                '[itemprop="address"], [class*="address"], [class*="Address"], '
                'address, [data-testid*="address"]'
            )
            if addr_el:
                raw_address = (await addr_el.inner_text()).strip()
        except Exception:
            pass

        # Fallback: regex AU address from page text
        if not raw_address:
            m = re.search(
                r'\d+[^\n,]{5,50}(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|'
                r'Boulevard|Blvd|Lane|Ln|Place|Pl|Court|Ct|Way|Parade|Pde|'
                r'Close|Cl|Terrace|Tce|Highway|Hwy)[^\n]{0,80}'
                r'(?:NSW|VIC|QLD|SA|WA|TAS|ACT|NT)\s+\d{4}',
                page_text,
                re.IGNORECASE,
            )
            if m:
                raw_address = m.group(0).strip()

        addr = parse_address_text(raw_address)

        return {
            "website_url":      website_url,
            "clinic_name":      clinic_name,
            "phone":            phone,
            "street":           addr["street"],
            "city":             addr["city"],
            "state":            addr["state"],
            "postcode":         addr["postcode"],
            "country":          "Australia",
            "healthdirect_url": detail_url,
            "scraped_at":       get_current_timestamp(),
        }

    except PlaywrightTimeoutError:
        print(f"    ⏱️ Timeout on detail page: {detail_url}")
    except Exception as e:
        print(f"    ❌ Error on detail page {detail_url}: {e}")

    return None


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def init_healthdirect_worksheet(sheet_key_or_url: str, service_account_file: str, tab_name: str):
    """
    Connect to Google Sheets and return the named tab (e.g. "main_healthdirect").
    Creates the tab if it doesn't exist yet.
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(service_account_file, scope)
    client = gspread.authorize(creds)

    if "docs.google.com" in sheet_key_or_url:
        sheet_key = sheet_key_or_url.split("/d/")[1].split("/")[0]
    else:
        sheet_key = sheet_key_or_url

    spreadsheet = client.open_by_key(sheet_key)
    print(f"✅ Connected to Google Sheet: {spreadsheet.title}")

    # Get or create the named tab
    try:
        worksheet = spreadsheet.worksheet(tab_name)
        print(f"📋 Using existing tab: '{tab_name}'")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=tab_name, rows=5000, cols=len(SHEET_HEADERS))
        print(f"✨ Created new tab: '{tab_name}'")

    return worksheet


def ensure_sheet_headers(worksheet) -> None:
    try:
        worksheet.update([SHEET_HEADERS], "A1:J1")
    except Exception as e:
        print(f"⚠️ Could not write headers: {e}")


def get_existing_urls(worksheet) -> set:
    """Return set of website_url values already in Column A (to skip duplicates)."""
    try:
        col_a = worksheet.col_values(1)
        return set(v.strip().lower() for v in col_a if v.strip())
    except Exception:
        return set()


def get_existing_healthdirect_urls(worksheet) -> set:
    """Return set of healthdirect_url values already in Column I."""
    try:
        col_i = worksheet.col_values(9)
        return set(v.strip().lower() for v in col_i if v.strip())
    except Exception:
        return set()


def append_row(worksheet, record: dict) -> None:
    """Append a single clinic record to the sheet."""
    row = [record.get(h, "") for h in SHEET_HEADERS]
    worksheet.append_row(row, value_input_option="USER_ENTERED")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    print("\n" + "=" * 70)
    print("  🏥 HEALTHDIRECT CLINIC SCRAPER")
    print("=" * 70)

    # ── Google Sheets setup ────────────────────────────────────────────────
    worksheet = init_healthdirect_worksheet(SHEET_KEY_OR_URL, SERVICE_ACCOUNT_FILE, SHEET_TAB_NAME)
    ensure_sheet_headers(worksheet)
    existing_urls = get_existing_urls(worksheet)
    existing_hd_urls = get_existing_healthdirect_urls(worksheet)
    print(f"📊 Sheet has {len(existing_urls)} existing clinic URLs already")

    # ── Collect all detail page URLs (Phase 1) ─────────────────────────────
    all_detail_urls = []
    seen_detail = set()

    total_combos = len(SERVICES_TO_SEARCH) * len(AU_POSTCODES)
    print(f"\n🔍 Phase 1: {len(SERVICES_TO_SEARCH)} services × {len(AU_POSTCODES)} postcodes = {total_combos} searches\n")

    combo_count = 0
    async with async_playwright() as p:
        for service in SERVICES_TO_SEARCH:
            print(f"\n  🏷️  Service: {service}")
            for i, postcode in enumerate(AU_POSTCODES):
                combo_count += 1
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(**random_context_options())
                page = await context.new_page()

                # Block images & fonts to speed up page loads
                await page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}", lambda route: route.abort())

                try:
                    detail_urls = await search_clinics_for_postcode(page, postcode, service=service)
                    for u in detail_urls:
                        u_lower = u.lower()
                        if u_lower not in seen_detail and u_lower not in existing_hd_urls:
                            seen_detail.add(u_lower)
                            all_detail_urls.append(u)
                finally:
                    await context.close()
                    await browser.close()

                print(f"  📈 Running total: {len(all_detail_urls)} new detail URLs  [{combo_count}/{total_combos}]")

                # Longer rest every 5 postcodes to reduce fingerprint risk
                if (i + 1) % 5 == 0:
                    rest = random.uniform(15, 30)
                    print(f"  💤 Taking a {rest:.0f}s rest after {i + 1} postcodes...")
                    await asyncio.sleep(rest)
                else:
                    await human_delay(3.0, 7.0)

            # Rest between services
            service_rest = random.uniform(20, 40)
            print(f"\n  💤 Service break — resting {service_rest:.0f}s before next service...")
            await asyncio.sleep(service_rest)

    print(f"\n✅ Phase 1 complete — {len(all_detail_urls)} unique detail pages to visit")

    # ── Visit each detail page and extract data (Phase 2) ─────────────────
    print(f"\n🔬 Phase 2: Extracting clinic data from {len(all_detail_urls)} detail pages...\n")

    processed = 0
    skipped = 0
    errors = 0

    async with async_playwright() as p:
        for i, detail_url in enumerate(all_detail_urls):
            # Use a fresh context every ~20 pages
            if i % 20 == 0:
                if i > 0:
                    await context.close()
                    await browser.close()
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(**random_context_options())

            page = await context.new_page()
            await page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}", lambda route: route.abort())

            try:
                print(f"  [{i + 1}/{len(all_detail_urls)}] {detail_url}")
                record = await scrape_detail_page(page, detail_url)

                if record is None:
                    errors += 1
                elif record["website_url"].lower() in existing_urls:
                    print(f"    ⏭️  Already in sheet — skipping")
                    skipped += 1
                else:
                    append_row(worksheet, record)
                    if record["website_url"]:
                        existing_urls.add(record["website_url"].lower())
                    print(
                        f"    ✅ Saved: {record['clinic_name']} | "
                        f"{record['website_url'] or 'no website'} | "
                        f"{record['phone'] or 'no phone'}"
                    )
                    processed += 1

            finally:
                await page.close()

            # Random delay between detail pages
            if (i + 1) % 10 == 0:
                rest = random.uniform(10, 20)
                print(f"  💤 Mini-rest {rest:.0f}s after {i + 1} pages...")
                await asyncio.sleep(rest)
            else:
                await human_delay(2.0, 5.0)

        # Close last browser
        try:
            await context.close()
            await browser.close()
        except Exception:
            pass

    # ── Summary ────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("SCRAPING COMPLETE")
    print("=" * 70)
    print(f"✅ Saved:    {processed}")
    print(f"⏭️  Skipped:  {skipped}")
    print(f"❌ Errors:   {errors}")
    print(f"🔍 Total detail pages visited: {len(all_detail_urls)}")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())