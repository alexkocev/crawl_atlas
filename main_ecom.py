"""
E-commerce Lead Qualification Scraper
Scrapes e-commerce websites (Shopify, WooCommerce, etc.) to extract tech stack,
social links, and contact info. Uses shared functions from core.py.
"""

import asyncio
import random
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError

from core import (
    extract_email,
    extract_phone,
    get_company_name,
    get_current_timestamp,
)

# -----------------------------------------------------------------------------
# CONFIG & FINGERPRINTS
# -----------------------------------------------------------------------------

# Google Sheet Config (same spreadsheet as clinics, different tab)
SHEET_KEY_OR_URL = 'https://docs.google.com/spreadsheets/d/1y9zzp1J1Fn60UKYN0RkTsSQcHcMb1mi2cD4NH8OfAF4/edit'
WORKSHEET_NAME = 'main_ecom'
WORKSHEET_GID = 659638589  # Fallback if tab name differs
SERVICE_ACCOUNT_FILE = 'yoluko-frontdesk-3d208271a3c0.json'

# Tech Stack Signatures (HTML text search)
TECH_SIGNATURES = {
    'platform': {
        'Shopify': ['cdn.shopify.com', 'Shopify.theme', 'shopify.com'],
        'WooCommerce': ['wp-content/plugins/woocommerce', 'woocommerce-product-gallery'],
        'BigCommerce': ['cdn11.bigcommerce.com', 'bigcommerce.com'],
        'Magento': ['/static/version', 'mage/cookies'],
        'Wix': ['wix.com', 'wix-thunderbolt'],
        'Squarespace': ['squarespace.com', 'static1.squarespace.com'],
    },
    'email_marketing': {
        'Klaviyo': ['klaviyo.js', 'klaviyo.com', '_learnq'],
        'Mailchimp': ['chimpstatic.com', 'mailchimp.com', 'mc.js'],
        'Omnisend': ['omnisend.com', 'omnisrc.com'],
        'Privy': ['privy.com', 'privy-widget'],
        'Sendlane': ['sendlane.com'],
    },
    'sms': {
        'Postscript': ['postscript.io', 'sdk.postscript.io'],
        'Attentive': ['attentivemobile.com', 'cdn.attn.tv'],
        'SMSBump': ['smsbump.com', 'yotpo-sms'],
        'Recart': ['recart.com'],
    },
    'subscriptions': {
        'Recharge': ['rechargeapps.com', 'recharge-payments'],
        'Skio': ['skio.com', 'skio-plan-picker'],
        'Bold': ['boldapps.net', 'bold-common'],
        'Smartrr': ['smartrr.com'],
    },
    'reviews': {
        'Yotpo': ['staticw2.yotpo.com', 'yotpo-widgets'],
        'Stamped': ['stamped.io', 'stamped-main-widget'],
        'Loox': ['loox.io', 'loox-rating'],
        'Judge.me': ['judge.me', 'judgeme_core'],
        'Okendo': ['okendo.io', 'oke-reviews'],
        'Junip': ['junip.co'],
    },
    'loyalty': {
        'Smile.io': ['smile.io', 'smile-ui'],
        'Yotpo Loyalty (Swell)': ['swell-rewards', 'yotpo-loyalty'],
        'LoyaltyLion': ['loyaltylion.com', 'loyaltylion-sdk'],
        'Rivo': ['rivo.io'],
    },
    'pixels': {
        'Meta/FB': ['fbevents.js', 'fbq('],
        'TikTok': ['ttq.load', 'analytics.tiktok.com'],
        'GA4/GTM': ['googletagmanager.com', 'gtag(', 'ga('],
        'Pinterest': ['pintrk('],
        'Snapchat': ['snaptr('],
    },
}

# -----------------------------------------------------------------------------
# HELPER FUNCTIONS
# -----------------------------------------------------------------------------


def detect_tech_stack(html: str) -> Dict[str, str]:
    """Scan HTML content for tech signatures."""
    results = {
        'platform': 'Unknown',
        'email_marketing': 'None',
        'sms': 'None',
        'subscriptions': 'None',
        'reviews': 'None',
        'loyalty': 'None',
        'pixels': [],
    }

    html_lower = html.lower()

    for category, providers in TECH_SIGNATURES.items():
        found = []
        for provider_name, signatures in providers.items():
            if any(sig.lower() in html_lower for sig in signatures):
                found.append(provider_name)

        if found:
            if category == 'pixels':
                results[category] = ", ".join(sorted(found))
            elif category == 'platform':
                results[category] = found[0]
            else:
                results[category] = ", ".join(sorted(found))

    if not results['pixels']:
        results['pixels'] = "None"

    return results


def extract_specific_socials(html: str) -> Dict[str, str]:
    """Find specific social media URLs (IG, FB, TikTok)."""
    html_lower = html.lower()
    return {
        'instagram': 'Yes' if 'instagram.com' in html_lower else 'No',
        'facebook': 'Yes' if 'facebook.com' in html_lower else 'No',
        'tiktok': 'Yes' if 'tiktok.com' in html_lower else 'No',
    }


async def find_contact_page(page: Page) -> Optional[str]:
    """Find a Contact or About page URL from the homepage."""
    try:
        keywords = ['contact', 'about', 'support', 'help']
        links = await page.query_selector_all('a[href]')

        for link in links:
            href = await link.get_attribute('href')
            text = await link.inner_text()

            if href and text:
                href_lower = href.lower()
                text_lower = text.lower()

                if any(kw in text_lower or kw in href_lower for kw in keywords):
                    full_url = urljoin(page.url, href)
                    if urlparse(full_url).netloc == urlparse(page.url).netloc:
                        return full_url
    except Exception:
        pass
    return None


# -----------------------------------------------------------------------------
# SCRAPER CORE
# -----------------------------------------------------------------------------


async def scrape_ecom_store(browser, url: str, max_retries: int = 2) -> Dict:
    """Scrape a single e-commerce store with retry on timeouts/403s."""
    result = {
        'url': url,
        'store_name': '',
        'platform': 'Unknown',
        'email_mktg': 'None',
        'sms': 'None',
        'subs': 'None',
        'reviews': 'None',
        'loyalty': 'None',
        'pixels': 'None',
        'instagram': 'No',
        'facebook': 'No',
        'tiktok': 'No',
        'email': '',
        'phone': '',
        'error': None,
    }

    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={'width': 1366, 'height': 768},
    )
    page = await context.new_page()

    try:
        print(f"\n🛍️  Analyzing: {url}...")

        # 1. Load Homepage (with retry)
        last_error = None
        for attempt in range(max_retries + 1):
            try:
                await page.goto(url, timeout=30000, wait_until='domcontentloaded')
                await page.wait_for_timeout(2000)
                break
            except PlaywrightTimeoutError as e:
                last_error = 'Timeout loading homepage'
                if attempt < max_retries:
                    wait = random.uniform(3, 6)
                    print(f"   ⚠️ Timeout, retrying in {wait:.1f}s...")
                    await asyncio.sleep(wait)
                else:
                    result['error'] = last_error
                    await context.close()
                    return result
            except Exception as e:
                err_str = str(e).lower()
                if '403' in err_str or 'forbidden' in err_str:
                    last_error = '403 Forbidden'
                else:
                    last_error = f'Error loading: {str(e)[:80]}'
                if attempt < max_retries:
                    wait = random.uniform(5, 10)
                    print(f"   ⚠️ {last_error}, retrying in {wait:.1f}s...")
                    await asyncio.sleep(wait)
                else:
                    result['error'] = last_error
                    await context.close()
                    return result

        # 2. Extract Homepage Data
        result['store_name'] = await get_company_name(page, url)
        html = await page.content()
        body_text = await page.inner_text('body')

        # Detect Tech Stack
        tech_stack = detect_tech_stack(html)
        result.update({
            'platform': tech_stack['platform'],
            'email_mktg': tech_stack['email_marketing'],
            'sms': tech_stack['sms'],
            'subs': tech_stack['subscriptions'],
            'reviews': tech_stack['reviews'],
            'loyalty': tech_stack['loyalty'],
            'pixels': tech_stack['pixels'],
        })

        # Socials
        socials = extract_specific_socials(html)
        result.update(socials)

        # Basic Contact Info from Homepage
        found_emails = [extract_email(body_text)]
        found_phones = [extract_phone(body_text)]

        # 3. Check Contact/About Page
        contact_url = await find_contact_page(page)

        if contact_url:
            print(f"   ↳ Checking Contact Page: {contact_url}")
            try:
                await page.goto(contact_url, timeout=15000, wait_until='domcontentloaded')
                await page.wait_for_timeout(1000)

                contact_text = await page.inner_text('body')
                found_emails.append(extract_email(contact_text))
                found_phones.append(extract_phone(contact_text))
            except Exception as e:
                print(f"   ⚠️ Could not scrape contact page: {e}")

        # Clean up contact info
        valid_emails = [e for e in found_emails if e]
        valid_phones = [p for p in found_phones if p]

        result['email'] = valid_emails[0] if valid_emails else ''
        result['phone'] = valid_phones[0] if valid_phones else ''

    except Exception as e:
        result['error'] = f"Unexpected error: {str(e)}"
        print(f"  ❌ Error: {e}")
    finally:
        await context.close()

    return result


# -----------------------------------------------------------------------------
# MAIN LOOP
# -----------------------------------------------------------------------------


async def main():
    """Main execution for E-com scraper."""
    try:
        # 1. Init Google Sheet
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            SERVICE_ACCOUNT_FILE, scope
        )
        client = gspread.authorize(creds)

        sheet_key = SHEET_KEY_OR_URL.split('/d/')[1].split('/')[0]
        sheet = client.open_by_key(sheet_key)

        # Get worksheet by name or GID fallback
        try:
            worksheet = sheet.worksheet(WORKSHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = None
            for ws in sheet.worksheets():
                if ws.id == WORKSHEET_GID:
                    worksheet = ws
                    break
            if worksheet is None:
                print(f"❌ Error: Worksheet '{WORKSHEET_NAME}' (GID {WORKSHEET_GID}) not found.")
                return

        print(f"✅ Connected to worksheet: {worksheet.title}")

        all_values = worksheet.get_all_values()

        if len(all_values) < 2:
            print("No data rows found.")
            return

        # 2. Launch Browser & Process Rows
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)

            for row_idx in range(1, len(all_values)):
                row_num = row_idx + 1
                row_data = all_values[row_idx]

                url = row_data[0].strip() if len(row_data) > 0 else ''
                if not url:
                    continue

                # Check Column M (index 12): Status/Error - skip if already processed
                status_log = row_data[12] if len(row_data) > 12 else ''
                if status_log and 'Processed' in status_log:
                    print(f"Skipping Row {row_num}: Already processed.")
                    continue

                if not url.startswith(('http://', 'https://')):
                    url = 'https://' + url

                data = await scrape_ecom_store(browser, url)

                # Prepare Row Update (B -> M)
                socials_str = f"IG:{data['instagram']} FB:{data['facebook']} TT:{data['tiktok']}"
                status = data['error'] if data['error'] else f"Processed {get_current_timestamp()}"

                update_values = [
                    data['store_name'],
                    data['platform'],
                    data['email_mktg'],
                    data['sms'],
                    data['subs'],
                    data['reviews'],
                    data['loyalty'],
                    data['pixels'],
                    socials_str,
                    data['email'],
                    data['phone'],
                    status,
                ]

                try:
                    cell_range = f'B{row_num}:M{row_num}'
                    worksheet.update(cell_range, [update_values])

                    if not data['error']:
                        print(f"✅ Updated Row {row_num}: {data['store_name']} ({data['platform']})")
                    else:
                        print(f"⚠️  Updated Row {row_num} with Error: {data['error']}")
                except Exception as e:
                    print(f"❌ Failed to write to sheet: {e}")

                # Rate limiting: 5-10s between rows
                if row_idx < len(all_values) - 1:
                    delay = random.uniform(5, 10)
                    print(f"⏳ Waiting {delay:.1f}s before next request...")
                    await asyncio.sleep(delay)

            await browser.close()

    except Exception as e:
        print(f"🔥 Fatal Error: {e}")
        raise


if __name__ == '__main__':
    asyncio.run(main())
