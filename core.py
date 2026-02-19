"""
Core shared functions for crawl_atlas scrapers.
Used by both main_clinics.py and main_ecom.py.
"""

import asyncio
import json
import re
from datetime import datetime
from typing import Dict, Optional
from urllib.parse import urlparse

import dns.resolver
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.async_api import Page


async def get_email_provider(domain: str) -> str:
    """Get email provider from MX records (non-blocking). Returns all providers found."""
    try:
        # Strip www., http://, and https:// from domain
        domain = domain.replace("www.", "").replace("http://", "").replace("https://", "").strip()
        # Remove trailing slashes and paths
        domain = domain.split('/')[0]
        # Run DNS lookup in executor to avoid blocking event loop
        answers = await asyncio.get_event_loop().run_in_executor(
            None, lambda: dns.resolver.resolve(domain, 'MX')
        )
        records = [str(r.exchange).lower() for r in answers]

        providers = []
        if any("google" in r for r in records):
            providers.append("Gmail / Google Workspace")
        if any("outlook" in r or "pphosted" in r or "office365" in r for r in records):
            providers.append("Microsoft 365")
        if any("zoho" in r for r in records):
            providers.append("Zoho")

        if providers:
            return ", ".join(providers)
        return "Other / Private"
    except Exception:
        return "Unknown"


async def count_team_members(page: Page) -> int:
    """Count team members by looking for team page and common patterns."""
    from urllib.parse import urljoin

    try:
        team_texts = ['Team', 'Staff', 'Practitioners', 'About Us', 'Our Team']
        team_link = None

        for text in team_texts:
            try:
                links = await page.query_selector_all(f'a:text-matches("{text}", "i")')
                if links:
                    href = await links[0].get_attribute('href')
                    if href:
                        team_link = urljoin(page.url, href)
                        break
            except Exception:
                continue

        if team_link:
            try:
                await page.goto(team_link, timeout=10000, wait_until='domcontentloaded')
                await page.wait_for_timeout(1000)
            except Exception:
                pass

        content = await page.content()
        title_pattern = r'(Dr\.\s[A-Z][a-z]+|Physiotherapist|Osteopath|Podiatrist|Psychologist|Therapist|Practitioner)'
        doctors = len(re.findall(title_pattern, content, re.IGNORECASE))

        team_images = await page.query_selector_all(
            'img[class*="team"], img[class*="staff"], img[class*="practitioner"], '
            'img[alt*="doctor"], img[alt*="dr"], img[src*="team"]'
        )
        image_count = len(team_images)

        return max(doctors // 2, image_count) if doctors > 0 or image_count > 0 else 0
    except Exception:
        return 0


def extract_email(text: str) -> Optional[str]:
    """Extract email address from text using regex."""
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    matches = re.findall(email_pattern, text)

    exclude_patterns = ['example.com', 'test.com', 'placeholder', 'noreply', 'no-reply']
    for match in matches:
        if not any(exclude in match.lower() for exclude in exclude_patterns):
            return match

    return None


def extract_address_from_jsonld(html: str) -> dict:
    """Extract address from JSON-LD schema.org (LocalBusiness, MedicalOrganization, etc.)"""
    address = {}
    pattern = r'<script[^>]*type\s*=\s*["\']application/ld\+json["\'][^>]*>(.*?)</script>'
    for match in re.finditer(pattern, html, re.DOTALL | re.IGNORECASE):
        try:
            data = json.loads(match.group(1).strip())
            # Handle both single object and @graph array
            items = data if isinstance(data, list) else [data]
            if isinstance(data, dict) and "@graph" in data:
                items = data["@graph"]
            for item in items:
                addr = item.get("address") or {}
                if isinstance(addr, str):
                    address["full_address"] = addr
                    return address
                if isinstance(addr, dict):
                    address["street"] = addr.get("streetAddress", "")
                    address["city"] = addr.get("addressLocality", "")
                    address["state"] = addr.get("addressRegion", "")
                    address["postcode"] = addr.get("postalCode", "")
                    country = addr.get("addressCountry", "")
                    if isinstance(country, dict):
                        country = country.get("name", "")
                    address["country"] = country or ""
                    if any(address.values()):
                        return address
        except Exception:
            continue
    return address


def extract_address_from_text(text: str) -> dict:
    """Fallback: regex-based address extraction from visible page text."""
    address = {}
    # Australian postcode pattern (4 digits), captures surrounding context
    au_pattern = r'(\d+\s[\w\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Lane|Ln|Place|Pl|Court|Ct|Way|Parade|Pde|Close|Cl|Terrace|Tce)[,\s]+[\w\s]+[,\s]+(?:NSW|VIC|QLD|SA|WA|TAS|ACT|NT)[,?\s]+(\d{4}))'
    match = re.search(au_pattern, text, re.IGNORECASE)
    if match:
        address["full_address"] = match.group(0).strip()

    # Postcode extraction as minimum signal
    postcode_match = re.search(r'\b(\d{4})\b', text)
    if postcode_match and not address.get("postcode"):
        address["postcode"] = postcode_match.group(1)

    return address


def extract_full_address(html: str, page_text: str) -> dict:
    """Try JSON-LD first, then text fallback. Returns dict with street/city/state/postcode/country."""
    addr = extract_address_from_jsonld(html)
    if not addr:
        addr = extract_address_from_text(page_text)
    return addr


def extract_all_phones(text: str) -> list:
    """Extract all phone numbers from text, deduplicated."""
    patterns = [
        r'\(0\d\)\s?\d{4}\s?\d{4}',          # (02) 9999 9999 Australian landline
        r'0\d{9}',                              # 0412345678 mobile
        r'0\d\s\d{4}\s\d{4}',                  # 02 9999 9999
        r'1[38]00\s?\d{3}\s?\d{3}',            # 1300/1800 numbers
        r'\+61\s?\d[\s\d]{8,11}',              # +61 international
        r'\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{4}[\s\-]?\d{4}',  # Generic international
    ]
    found = set()
    for pattern in patterns:
        for m in re.finditer(pattern, text):
            cleaned = re.sub(r'\s+', ' ', m.group(0)).strip()
            found.add(cleaned)
    return sorted(found)


def extract_all_emails(text: str, html: str = "") -> list:
    """Extract all unique, valid emails from text and mailto: links in HTML."""
    found = set()
    exclude = ['example.com', 'test.com', 'placeholder', 'noreply', 'no-reply', 'sentry', 'wixpress']

    email_pattern = r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b'
    for match in re.findall(email_pattern, text):
        if not any(ex in match.lower() for ex in exclude):
            found.add(match.lower())

    # Also parse mailto: from raw HTML
    for match in re.finditer(r'href=["\']mailto:([^"\'?\s]+)', html, re.IGNORECASE):
        email = match.group(1).strip().lower()
        if '@' in email and not any(ex in email for ex in exclude):
            found.add(email)

    return sorted(found)


def extract_phone(text: str) -> Optional[str]:
    """Extract phone number from text using regex."""
    patterns = [
        r'\+?\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}',
        r'\(\d{3}\)\s?\d{3}[-.\s]?\d{4}',
        r'\d{2,4}\s?\d{4}\s?\d{4}',
    ]

    for pattern in patterns:
        matches = re.findall(pattern, text)
        if matches:
            return matches[0].strip()

    return None


def extract_social_media(html: str) -> Dict[str, str]:
    """Extract social media presence from HTML."""
    html_lower = html.lower()
    return {
        'instagram': 'Yes' if 'instagram.com' in html_lower else 'No',
        'whatsapp': 'Yes' if 'wa.me' in html_lower or 'whatsapp' in html_lower else 'No'
    }


async def get_company_name(page: Page, url: str) -> str:
    """Extract company name from page."""
    try:
        selectors = ['h1', '.site-title', '.clinic-name', '[class*="logo"]']

        for selector in selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    text = await element.inner_text()
                    if text and len(text.strip()) < 100:
                        return text.strip()
            except Exception:
                pass

        domain = urlparse(url).netloc
        return domain.replace('www.', '')
    except Exception:
        domain = urlparse(url).netloc
        return domain.replace('www.', '')


def init_google_sheets(sheet_key_or_url: str, service_account_file: str = 'service_account.json'):
    """
    Initialize Google Sheets connection using service account credentials.
    Pass the appropriate sheet key/URL for each scraper (e.g. clinics vs ecom).

    Args:
        sheet_key_or_url: Google Sheet key (from URL) or full URL
        service_account_file: Path to service account JSON file

    Returns:
        gspread worksheet object
    """
    try:
        if not sheet_key_or_url or sheet_key_or_url == 'YOUR_SHEET_KEY_OR_URL_HERE':
            print("❌ Error: Sheet key/URL not configured!")
            print("   Please update SHEET_KEY_OR_URL in main() function with your Google Sheet key or URL")
            raise ValueError("Sheet key/URL not configured")

        if 'docs.google.com' in sheet_key_or_url:
            sheet_key = sheet_key_or_url.split('/d/')[1].split('/')[0]
            print(f"📋 Extracted sheet key: {sheet_key}")
        else:
            sheet_key = sheet_key_or_url
            print(f"📋 Using sheet key: {sheet_key}")

        try:
            with open(service_account_file, 'r') as f:
                service_account_data = json.load(f)
                service_account_email = service_account_data.get('client_email', 'unknown')
                print(f"🔑 Service account: {service_account_email}")
        except Exception:
            service_account_email = "unknown"

        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            service_account_file, scope
        )
        client = gspread.authorize(credentials)
        sheet = client.open_by_key(sheet_key)
        worksheet = sheet.sheet1

        print(f"✅ Connected to Google Sheet: {sheet.title}")
        return worksheet

    except FileNotFoundError:
        print(f"❌ Error: Service account file '{service_account_file}' not found")
        raise
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"❌ Error: Google Sheet not found (404)")
        print(f"   Sheet key used: {sheet_key if 'sheet_key' in locals() else 'N/A'}")
        print(f"\n   Possible causes:")
        print(f"   1. The sheet key/URL is incorrect")
        print(f"   2. The sheet is not shared with the service account")
        print(f"   3. The sheet doesn't exist")
        print(f"\n   Action required:")
        print(f"   - Share your Google Sheet with: {service_account_email if 'service_account_email' in locals() else 'the service account email'}")
        print(f"   - Verify the sheet key/URL is correct")
        raise
    except Exception as e:
        print(f"❌ Error connecting to Google Sheets: {e}")
        raise


def get_current_timestamp() -> str:
    """Get current timestamp in YYYY-MM-DD HH:MM:SS format."""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
