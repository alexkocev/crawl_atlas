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
