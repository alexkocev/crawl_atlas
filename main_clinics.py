"""
Medical Clinic Lead Qualification Scraper
Scrapes clinic websites to extract qualification data including email providers,
CRM systems, practitioner counts, and service offerings.
"""

import asyncio
import random
import re
from typing import Dict
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError

from core import (
    get_email_provider,
    count_team_members,
    extract_email,
    extract_phone,
    extract_social_media,
    get_company_name,
    init_google_sheets,
    get_current_timestamp,
)


# CRM detection keywords - domain patterns
CRM_DOMAINS = {
    'cliniko.com': 'Cliniko',
    'nookal.com': 'Nookal',
    'hotdoc.com.au': 'HotDoc',
    'healthengine.com.au': 'HealthEngine',
    'halaxy.com': 'Halaxy',
    'coreplus.com.au': 'CorePlus',
    'doctolib.': 'Doctolib',  # .fr, .de, etc.
    'doctena.com': 'Doctena',
    'jameda.de': 'Jameda',
    'docplanner.': 'Docplanner',  # .com, .co.uk, etc.
    'doctoralia.': 'Doctoralia',
    'zocdoc.com': 'Zocdoc',
    'simplepractice.com': 'SimplePractice',
    'drchrono.com': 'DrChrono',
    'advancedmd.com': 'AdvancedMD',
    'athenahealth.com': 'Athenahealth',
    'mychart': 'Epic MyChart',
    'cerner': 'Cerner',
    'healthelife': 'Cerner HealtheLife',
    'zedmed.com.au': 'ZedMed',
    'splose.com': 'Splose',
    'zandamed.com': 'Zandamed',
    'wix.com': 'Wix (Website/Booking)',
    'squarespace.com': 'Squarespace',
    'medicalonline': 'Best Practice',
    'bpsoftware': 'Best Practice',
    'bestpractice': 'Best Practice',
    'genie': 'Genie Solutions',
    'gentu': 'Genie Solutions',
    'powerdiary': 'Power Diary'
}

# CRM text fingerprints
CRM_TEXT_PATTERNS = {
    'cliniko': 'Cliniko',
    'hotdoc': 'HotDoc',
    'doctolib': 'Doctolib',
    'zocdoc': 'Zocdoc',
    'simplepractice': 'SimplePractice',
    'drchrono': 'DrChrono',
    'athenahealth': 'Athenahealth',
    'mychart': 'Epic MyChart',
    'cerner': 'Cerner',
    'healthengine': 'HealthEngine',
    'halaxy': 'Halaxy',
    'coreplus': 'CorePlus',
    'nookal': 'Nookal',
    'medicalonline': 'Best Practice',
    'bpsoftware': 'Best Practice',
    'bestpractice': 'Best Practice',
    'genie': 'Genie Solutions',
    'gentu': 'Genie Solutions',
    'powerdiary': 'Power Diary'
}

# Home visit keywords
HOME_VISIT_KEYWORDS = [
    'home visit',
    'mobile service',
    'we come to you',
    'domiciliary'
]

# Practitioner title keywords
PRACTITIONER_KEYWORDS = [
    'dr.',
    'doctor',
    'physiotherapist',
    'physio',
    'therapist',
    'practitioner',
    'specialist',
    'nurse',
    'dentist'
]


async def detect_crm(page: Page) -> str:
    """Detect CRM system from page content. Checks scripts, iframes, links, and text patterns."""
    found_crms = set()  # Use set to avoid duplicates
    
    try:
        # Get HTML content
        html = await page.content()
        html_lower = html.lower()
        
        # Check for Wix indicators (wix-thunderbolt, wix.com, static.wixstatic.com)
        if 'wix-thunderbolt' in html_lower or 'wix.com' in html_lower or 'static.wixstatic.com' in html_lower:
            found_crms.add('Wix (Website/Booking)')
        
        # Check for Squarespace indicators
        if 'squarespace.com' in html_lower:
            found_crms.add('Squarespace')
        
        # 1. Check domain patterns in HTML (scripts, iframes, links, XHR endpoints)
        for domain_pattern, name in CRM_DOMAINS.items():
            if domain_pattern in html_lower:
                found_crms.add(name)
        
        # 2. Check script sources
        scripts = await page.query_selector_all('script[src]')
        for script in scripts:
            src = await script.get_attribute('src')
            if src:
                src_lower = src.lower()
                for domain_pattern, name in CRM_DOMAINS.items():
                    if domain_pattern in src_lower:
                        found_crms.add(name)
        
        # 3. Check iframe sources
        iframes = await page.query_selector_all('iframe[src]')
        for iframe in iframes:
            src = await iframe.get_attribute('src')
            if src:
                src_lower = src.lower()
                for domain_pattern, name in CRM_DOMAINS.items():
                    if domain_pattern in src_lower:
                        found_crms.add(name)
        
        # 4. Check link hrefs
        links = await page.query_selector_all('link[href], a[href]')
        for link in links:
            href = await link.get_attribute('href')
            if href:
                href_lower = href.lower()
                for domain_pattern, name in CRM_DOMAINS.items():
                    if domain_pattern in href_lower:
                        found_crms.add(name)
        
        # 5. Check text fingerprints ("powered by", vendor names, etc.)
        page_text = await page.inner_text('body')
        page_text_lower = page_text.lower()
        
        # Check for "powered by" patterns
        powered_by_pattern = r'powered\s+by\s+([a-z]+)'
        powered_matches = re.findall(powered_by_pattern, page_text_lower)
        for match in powered_matches:
            for pattern, name in CRM_TEXT_PATTERNS.items():
                if pattern in match:
                    found_crms.add(name)
        
        # Check for vendor names in text
        for pattern, name in CRM_TEXT_PATTERNS.items():
            # Look for vendor name mentions (case-insensitive word boundary)
            pattern_regex = r'\b' + re.escape(pattern) + r'\b'
            if re.search(pattern_regex, page_text_lower):
                found_crms.add(name)
        
        # 6. Check for common booking patterns
        booking_patterns = [
            ('book appointment', 'Booking System'),
            ('patient portal', 'Patient Portal'),
            ('online booking', 'Online Booking')
        ]
        
        # Check HTML for booking-related text that might indicate CRM
        for pattern_text, _ in booking_patterns:
            if pattern_text in html_lower:
                # If we found booking patterns but no CRM yet, check if there's a booking iframe/script
                booking_elements = await page.query_selector_all(
                    'iframe[src*="book"], iframe[src*="appointment"], '
                    'script[src*="book"], script[src*="appointment"]'
                )
                if booking_elements:
                    # Try to extract domain from booking elements
                    for elem in booking_elements:
                        src = await elem.get_attribute('src')
                        if src:
                            for domain_pattern, name in CRM_DOMAINS.items():
                                if domain_pattern in src.lower():
                                    found_crms.add(name)
        
        # Convert set to sorted list for consistent output
        found_crms_list = sorted(list(found_crms))
        
        if found_crms_list:
            return ", ".join(found_crms_list)
        return "Not Detected"
        
    except Exception as e:
        print(f"  Error detecting CRM: {e}")
        return "Not Detected"


def check_home_visits(html: str) -> bool:
    """Check if clinic offers home visits from HTML."""
    html_lower = html.lower()
    return any(kw in html_lower for kw in HOME_VISIT_KEYWORDS)


async def scrape_clinic(browser, url: str) -> Dict:
    """Scrape a single clinic website."""
    result = {
        'url': url,
        'clinic_name': '',
        'email_provider': 'Unknown',
        'crm': 'Not Detected',
        'practitioner_count': 0,
        'home_visits': 'NO',
        'instagram': 'No',
        'whatsapp': 'No',
        'error': None
    }
    
    # Create isolated context for each clinic
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    page = await context.new_page()
    
    try:
        print(f"\n🔍 Analyzing: {url}...")
        
        domain = urlparse(url).netloc.replace('www.', '')
        
        # Start DNS lookup in parallel (non-blocking)
        provider_task = asyncio.create_task(get_email_provider(domain))
        
        # Load homepage
        try:
            await page.goto(url, timeout=30000, wait_until='domcontentloaded')
            await page.wait_for_timeout(2000)  # Wait for dynamic content
        except PlaywrightTimeoutError:
            result['error'] = 'Timeout loading homepage'
            await context.close()
            return result
        except Exception as e:
            result['error'] = f'Error loading homepage: {str(e)}'
            await context.close()
            return result
        
        # Get clinic name
        result['clinic_name'] = await get_company_name(page, url)
        
        # Get HTML for analysis
        html = await page.content()
        
        # Wait for DNS lookup to complete
        result['email_provider'] = await provider_task
        
        # Detect CRM
        result['crm'] = await detect_crm(page)
        
        # Check for home visits
        result['home_visits'] = 'YES' if check_home_visits(html) else 'NO'
        
        # Also check services page for home visits
        if result['home_visits'] == 'NO':
            services_urls = [urljoin(url, '/services'), urljoin(url, '/service')]
            for services_url in services_urls:
                try:
                    await page.goto(services_url, timeout=10000, wait_until='domcontentloaded')
                    await page.wait_for_timeout(1000)
                    services_html = await page.content()
                    if check_home_visits(services_html):
                        result['home_visits'] = 'YES'
                        break
                except:
                    continue
        
        # Extract social media
        social = extract_social_media(html)
        result['instagram'] = social['instagram']
        result['whatsapp'] = social['whatsapp']
        
        # Extract email from mailto links
        found_emails = []
        try:
            mailto_links = await page.query_selector_all('a[href^="mailto:"]')
            for link in mailto_links:
                href = await link.get_attribute('href')
                if href:
                    clean_email = href.replace('mailto:', '').split('?')[0].strip()
                    if '@' in clean_email:
                        # Filter out common non-contact emails
                        exclude_patterns = ['example.com', 'test.com', 'placeholder', 'noreply', 'no-reply']
                        if not any(exclude in clean_email.lower() for exclude in exclude_patterns):
                            if clean_email not in found_emails:
                                found_emails.append(clean_email)
        except Exception as e:
            print(f"  ⚠️  Warning: Error extracting mailto links: {e}")
        
        # Also extract emails from visible text using existing function
        page_text = await page.inner_text('body')
        text_email = extract_email(page_text)
        if text_email and text_email not in found_emails:
            found_emails.append(text_email)
        
        # Store found emails (if any) - can be used for further processing
        if found_emails:
            result['found_emails'] = found_emails
        
        # Count practitioners (navigates to team page if found)
        result['practitioner_count'] = await count_team_members(page)
        
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)}'
        print(f"  ❌ Error: {e}")
    finally:
        await context.close()
    
    return result


async def main():
    """Main function to scrape clinics from Google Sheets."""
    # Configuration - Update these values
    SHEET_KEY_OR_URL = 'https://docs.google.com/spreadsheets/d/1y9zzp1J1Fn60UKYN0RkTsSQcHcMb1mi2cD4NH8OfAF4/edit?usp=sharing'
    SERVICE_ACCOUNT_FILE = 'yoluko-frontdesk-3d208271a3c0.json'
    
    try:
        # Initialize Google Sheets connection
        worksheet = init_google_sheets(SHEET_KEY_OR_URL, SERVICE_ACCOUNT_FILE)
        
        # Get all values from the sheet
        all_values = worksheet.get_all_values()
        
        if len(all_values) < 2:  # Header row + at least one data row
            print("No data rows found in the sheet (only header row exists)")
            return
        
        # Column mapping (0-indexed)
        # Col A = 0: Website URL
        # Col B = 1: Clinic Name
        # Col C = 2: Email Provider
        # Col D = 3: CRM System
        # Col E = 4: Practitioner Count
        # Col F = 5: Home Visits
        # Col G = 6: Instagram
        # Col H = 7: WhatsApp
        # Col I = 8: Scraping Date
        # Col J = 9: Error Log
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            processed_count = 0
            skipped_count = 0
            error_count = 0
            
            # Start from row 2 (skip header row)
            for row_idx in range(1, len(all_values)):
                row_num = row_idx + 1  # 1-indexed row number for Google Sheets
                row_data = all_values[row_idx]
                
                # Get URL from Column A (index 0)
                url = row_data[0].strip() if len(row_data) > 0 else ''
                
                # Skip empty URLs
                if not url:
                    print(f"Row {row_num}: Skipping - No URL provided")
                    skipped_count += 1
                    continue
                
                # Check Column I (index 8) - Scraping Date
                scraping_date = row_data[8].strip() if len(row_data) > 8 else ''
                
                # Skip if already scraped
                if scraping_date:
                    print(f"Row {row_num}: Skipping {url} - Already scraped on {scraping_date}")
                    skipped_count += 1
                    continue
                
                # Ensure URL has protocol
                if not url.startswith(('http://', 'https://')):
                    url = 'https://' + url
                
                print(f"\n{'='*80}")
                print(f"Processing Row {row_num}: {url}")
                print(f"{'='*80}")
                
                try:
                    # Scrape the clinic
                    result = await scrape_clinic(browser, url)
                    
                    # Prepare update values
                    # Columns B-H (indices 1-7)
                    update_values = [
                        result.get('clinic_name', ''),
                        result.get('email_provider', 'Unknown'),
                        result.get('crm', 'Not Detected'),
                        str(result.get('practitioner_count', 0)),
                        result.get('home_visits', 'NO'),
                        result.get('instagram', 'No'),
                        result.get('whatsapp', 'No')
                    ]
                    
                    # Get timestamp
                    timestamp = get_current_timestamp()
                    
                    if result.get('error'):
                        # Update error log (Column J, index 9) and timestamp (Column I) to prevent re-scraping
                        error_msg = result['error']
                        worksheet.update_cell(row_num, 9, timestamp)  # Column I = 9 (1-indexed) - Update timestamp
                        worksheet.update_cell(row_num, 10, error_msg)  # Column J = 10 (1-indexed) - Update error log
                        error_count += 1
                        print(f"❌ ERROR: {error_msg}")
                    else:
                        # Clear any previous error
                        worksheet.update_cell(row_num, 10, '')  # Column J = 10 (1-indexed)
                        
                        # Print results
                        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                        print(f"🏥 {result['clinic_name'].upper()}")
                        print(f"📧 Email Provider: {result['email_provider']}")
                        print(f"⚙️  CRM:            {result['crm']}")
                        print(f"👥 Team Size:      ~{result['practitioner_count']} members")
                        print(f"🚗 Home Visits:    {result['home_visits']}")
                        print(f"📱 Social Media:   Instagram: {result['instagram']} | WhatsApp: {result['whatsapp']}")
                        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                    
                    # Update columns B-H (indices 1-7, which are columns 2-8 in 1-indexed)
                    # Use batch_update for efficiency
                    cell_range = f'B{row_num}:H{row_num}'
                    worksheet.update(cell_range, [update_values])
                    
                    # Update scraping date (Column I, index 8, which is column 9 in 1-indexed)
                    worksheet.update_cell(row_num, 9, timestamp)
                    
                    processed_count += 1
                    print(f"✅ Row {row_num} updated successfully")
                    
                except Exception as e:
                    # Handle unexpected errors
                    error_msg = f'Unexpected error: {str(e)}'
                    timestamp = get_current_timestamp()
                    
                    # Update error log and timestamp
                    worksheet.update_cell(row_num, 9, timestamp)  # Column I = 9 (1-indexed)
                    worksheet.update_cell(row_num, 10, error_msg)  # Column J = 10 (1-indexed)
                    
                    error_count += 1
                    print(f"❌ ERROR updating row {row_num}: {error_msg}")
                
                # Random delay between updates (5-10 seconds) to avoid rate limits
                if row_idx < len(all_values) - 1:
                    delay = random.uniform(5, 10)
                    print(f"⏳ Waiting {delay:.1f} seconds before next request...\n")
                    await asyncio.sleep(delay)
            
            await browser.close()
            
            # Final summary
            print("\n" + "="*80)
            print("SCRAPING COMPLETE")
            print("="*80)
            print(f"✅ Successfully processed: {processed_count}")
            print(f"⏭️  Skipped (already scraped): {skipped_count}")
            print(f"❌ Errors: {error_count}")
            print(f"📊 Total rows checked: {len(all_values) - 1}")
            print("="*80)
    
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        raise


if __name__ == '__main__':
    asyncio.run(main())

