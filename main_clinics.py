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
    extract_full_address,
    extract_all_phones,
    extract_all_emails,
)


# Tech stack signatures - fingerprints for detection
TECH_SIGNATURES = {
    # 1. Practice Management Systems / EHR
    "pms": {
        # --- Major Australian GP & Specialist Systems ---
        "Best Practice": ["bpsoftware", "bestpractice", "medicalonline", "bp-software.com.au"],
        "MedicalDirector": ["medicaldirector", "md-software", "helix.medicaldirector", "pracsoft"],
        "Zedmed": ["zedmed", "zedmed.com.au"],
        "Genie Solutions": ["genie.com.au", "geniesol", "genie solutions"],
        "Gentu": ["gentu", "genie"],
        "Clinic to Cloud": ["clinictocloud", "clinic to cloud"],
        "Bluechip": ["bluechip", "medical director"],
        "Shexie": ["shexie", "shexie.com.au"],
        "Medilink": ["medilink", "medilink.com.au"],
        "PrimaryClinic": ["primaryclinic", "primaryclinic.com.au"],
        "Communicare": ["communicare", "telstra health"], # Common in indigenous health
        "Audit4": ["audit4", "s4s", "software4specialists"],
        "CareRight": ["careright", "clintel"],
        "Profile (Intrahealth)": ["intrahealth", "profile pms"],
        "MasterCare": ["master-care", "mastercare", "global health"],
        "Viper": ["viper", "viper.com.au"],
        
        # --- Allied Health (Cloud/SaaS) ---
        "Cliniko": ["cliniko", "app.cliniko"],
        "Halaxy": ["halaxy", "healthkit"], # HealthKit is the old name
        "Nookal": ["nookal", "nookal.com"],
        "Power Diary": ["powerdiary", "power-diary"],
        "Splose": ["splose", "splose.com"],
        "Coreplus": ["coreplus", "coreplus.com.au"],
        "Jane App": ["janeapp", "jane.app"],
        "Smartsoft Front Desk": ["smartsoft", "frontdesk.com.au", "booking.frontdesk"],
        "PracSuite": ["pracsuite", "smartsoft"], # Smartsoft's cloud version
        "Practice Pal": ["practicepal"],
        "WriteUpp": ["writeupp", "writeupp.com"],
        "Carepatron": ["carepatron", "carepatron.com"],
        "Xestro": ["xestro", "xestro.com"],
        "PPMP": ["ppmp", "ppmp.com.au"],
        "TM2 / TM3": ["tm2", "tm3", "insignia"],
        
        # --- Enterprise / Hospital / International Majors ---
        "Epic": ["epic.com", "mychart", "epic systems"],
        "Cerner": ["cerner", "healthelife", "oracle health", "millennium"],
        "MediRecords": ["medirecords", "medirecords.com"],
        "Allscripts": ["allscripts", "veradigm"],
        "Meditech": ["meditech"],
        "TrakCare": ["trakcare", "intersystems"],
        "Athenahealth": ["athenahealth", "athenaone"],
        "DrChrono": ["drchrono", "onpatient"],
        "SimplePractice": ["simplepractice", "simplepractice.com"],
        "AdvancedMD": ["advancedmd"],
        "Kareo": ["kareo", "tebra"],
        "Practice Fusion": ["practicefusion", "practice fusion"],
        "NextGen": ["nextgen", "nextgen healthcare"],
        "eClinicalWorks": ["eclinicalworks", "healow"],
        "Greenway Health": ["greenway", "prime suite"],
        
        # --- Dental Specific (Australia/Global) ---
        "Dental4Windows": ["dental4windows", "centaur software", "d4w"],
        "Exact (SOE)": ["software of excellence", "soe", "exact pms"],
        "Praktika": ["praktika"],
        "Oasis": ["oasis dental"],
        "Dentrix": ["dentrix", "henry schein"],
        "Core Practice": ["corepractice", "core practice"],
        
        # --- Niche / Other ---
        "Zandamed": ["zandamed"],
        "Doctena": ["doctena"],
        "Cosmetri": ["cosmetri"], # Aesthetics
        "Timely": ["gettimely", "timely"], # Salon/Spa/Clinic
        "Fresha": ["fresha", "shedul"], # Salon/Spa/Clinic
        "Mindbody": ["mindbody", "mindbodyonline"], # Wellness/Physio
    },
    # 2. Booking Systems
    "booking": {
        "HotDoc": ["hotdoc.com.au"],
        "HealthEngine": ["healthengine.com.au"],
        "AutoMed": ["automed.com.au"],
        "Calendly": ["calendly.com", "assets.calendly"],
        "Cliniko Booking": ["booking.cliniko.com"],
        "Halaxy Booking": ["halaxy.com/book"],
        "Power Diary Booking": ["powerdiary.com/book"],
        "HubSpot Meetings": ["meetings.hubspot.com", "meetings.hs-sites"],
        "Acuity": ["acuityscheduling.com"],
        "Setmore": ["setmore.com"],
        "Doctolib": ["doctolib.fr", "doctolib.de", "doctolib.com"],
        "Zocdoc": ["zocdoc.com"],
        "Docplanner": ["docplanner.com", "docplanner.co.uk"],
        "Doctoralia": ["doctoralia.com"],
        "Jameda": ["jameda.de"],
        "Jane App": ["janeapp.com"],
        "Fresha": ["fresha.com"],
        "Front Desk Booking": ["booking.frontdesk.com.au", "smartsoft.com.au"],
    },
    # 3. CMS / Website Builder
    "cms": {
        "WordPress": ["wp-content", "wp-includes", "wordpress"],
        "Wix": ["wix.com", "wix-thunderbolt", "static.wixstatic.com"],
        "Squarespace": ["squarespace.com", "static1.squarespace"],
        "Webflow": ["webflow.com", "assets-global.website-files"],
        "Shopify": ["cdn.shopify.com", "shopify.theme"],
        "Framer": ["framer.com", "framerusercontent.com"],
        "Drupal": ["drupal.org", "drupal"],
        "Joomla": ["joomla.org", "joomla"],
        "Ghost": ["ghost.org", "ghost"],
        "Weebly": ["weebly.com"],
    },
    # 4. Email Marketing / CRM
    "email_marketing": {
        "Mailchimp": ["chimpstatic.com", "mailchimp.com", "mc.js", "list-manage.com"],
        "Klaviyo": ["klaviyo.com", "_learnq", "static.klaviyo"],
        "ActiveCampaign": ["activecampaign.com", "trackcmp.net"],
        "Campaign Monitor": ["createsend.com", "campaignmonitor.com"],
        "HubSpot": ["hubspot.com", "hs-scripts.com", "hsforms.com"],
        "Zoho CRM": ["zoho.com/crm", "zohopublic", "zohocrm"],
        "Salesforce": ["salesforce.com", "pardot.com", "force.com"],
        "Keap": ["infusionsoft.com", "keap.com"],
        "Constant Contact": ["constantcontact.com"],
        "Sendinblue": ["sendinblue.com", "brevo.com"],
        "ConvertKit": ["convertkit.com"],
    },
    # 5. Ad Pixels / Analytics
    "pixels": {
        "Meta Pixel": ["fbevents.js", "connect.facebook.net/en_us/fbevents", "fbq("],
        "Google Ads": ["googleadservices.com", "gtag('event'", "google_conversion"],
        "Google Analytics 4": ["gtag.js", "google-analytics.com", "googletagmanager.com/gtag"],
        "Google Tag Manager": ["googletagmanager.com", "gtm.js"],
        "LinkedIn Insight": ["snap.licdn.com", "linkedin.com/insight"],
        "TikTok Pixel": ["analytics.tiktok.com", "ttq.load"],
        "Pinterest": ["pintrk(", "ct.pinterest.com"],
        "Hotjar": ["hotjar.com", "hjsetting"],
        "Microsoft Clarity": ["clarity.ms", "microsoft.com/clarity"],
        "Segment": ["segment.com", "analytics.js"],
        "Mixpanel": ["mixpanel.com"],
        "Heap": ["heap.io"],
        "Call Dynamics": ["calldynamics.com.au", "artemis", "artemisData"],
    },
    # 6. Payments
    "payments": {
        "Stripe": ["js.stripe.com", "stripe.com/v3"],
        "Square": ["squareup.com", "square.site"],
        "PayPal": ["paypal.com/sdk", "paypalobjects.com"],
        "Tyro": ["tyro.com"],
        "Pin Payments": ["pinpayments.com"],
        "Windcave": ["windcave.com", "paymentexpress"],
        "eWay": ["eway.io", "eway.com.au"],
        "Braintree": ["braintreegateway.com"],
    },
    # 7. Live Chat / Support
    "live_chat": {
        "Intercom": ["intercom.com", "widget.intercom.io", "intercomsettings"],
        "Drift": ["drift.com", "js.drift.com"],
        "Tawk.to": ["tawk.to", "embed.tawk.to"],
        "Zendesk": ["zendesk.com", "zopim.com", "zdassets.com"],
        "Crisp": ["crisp.chat", "client.crisp.chat"],
        "LiveChat": ["livechatinc.com", "__lc"],
        "WhatsApp Widget": ["wa.me", "whatsapp.com/send", "api.whatsapp"],
        "HubSpot Chat": ["hubspot-messages"],
        "Freshdesk": ["freshdesk.com"],
    },
    # 8. Reviews
    "reviews": {
        "Doctify": ["doctify.com"],
        "HealthEngine Reviews": ["healthengine.com.au"],
        "RateMDs": ["ratemds.com"],
        "Google Reviews": ["maps.googleapis.com", "place_id", "google.com/maps/embed"],
        "Trustpilot": ["trustpilot.com", "widget.trustpilot"],
        "Feefo": ["feefo.com"],
    },
    # 9. Infrastructure / CDN (also detected via headers in detect_from_headers)
    "infra": {
        "Cloudflare": ["cloudflare", "__cf_bm", "cf-ray"],
        "AWS": ["amazonaws.com", "cloudfront.net"],
        "Azure": ["azurewebsites.net", "azureedge.net"],
        "Google Cloud": ["googleapis.com", "storage.cloud.google"],
        "Fastly": ["fastly.com", "fastly.net"],
        "Vercel": ["vercel.app", "vercel-insights"],
        "Netlify": ["netlify.app", "netlify"],
    },
}

# Header-based infra detection (Server, X-Powered-By, CF-Ray, Via, X-Generator)
HEADER_SIGNATURES = {
    "Cloudflare": ["cloudflare", "cf-ray"],
    "AWS": ["amazonaws", "cloudfront"],
    "Azure": ["azure", "azurewebsites", "azureedge"],
    "Google Cloud": ["google", "gse"],
    "Fastly": ["fastly"],
    "nginx": ["nginx"],
    "Apache": ["apache"],
    "Microsoft-IIS": ["microsoft-iis", "iis"],
    "Vercel": ["vercel"],
    "Netlify": ["netlify"],
}

# Home visit keywords
HOME_VISIT_KEYWORDS = [
    'home visit',
    'mobile service',
    'we come to you',
    'domiciliary'
]

# Practitioner title keywords (used for detecting team/practitioner content)
PRACTITIONER_KEYWORDS = [
    # General
    'dr.',
    'doctor',
    'physician',
    'gp',
    'general practitioner',
    'specialist',
    'practitioner',
    'clinician',
    'surgeon',
    # Physiotherapy
    'physiotherapist',
    'physio',
    # Speech & language
    'speechie',
    'speech therapist',
    'speech pathologist',
    'speech-language pathologist',
    'slp',
    # Psychology & mental health
    'psychologist',
    'psychiatrist',
    'clinical psychologist',
    'counsellor',
    'counselor',
    'therapist',
    # Occupational therapy
    'occupational therapist',
    'ot',
    # Podiatry
    'podiatrist',
    'chiropodist',
    # Osteopathy & chiropractic
    'osteopath',
    'chiropractor',
    'chiro',
    # Nursing & midwifery
    'nurse',
    'midwife',
    'rn',
    # Dentistry
    'dentist',
    'dental',
    'orthodontist',
    # Dietetics & nutrition
    'dietitian',
    'dietician',
    'nutritionist',
    # Exercise & rehabilitation
    'exercise physiologist',
    'ep',
    'remedial massage therapist',
    'myotherapist',
    'massage therapist',
    # Allied health
    'audiologist',
    'optometrist',
    'optician',
    'pharmacist',
    'radiographer',
    'sonographer',
    'prosthetist',
    'orthotist',
    'social worker',
    # Complementary & alternative
    'naturopath',
    'acupuncturist',
    'homeopath',
]


def _normalize_for_match(text: str) -> str:
    """Normalize text for case-insensitive pattern matching."""
    return (text or "").lower()


async def detect_from_headers(response) -> dict:
    """
    Extract infra/CDN from HTTP response headers.
    Looks for: Server, X-Powered-By, CF-Ray, Via, X-Generator.
    """
    found = {}
    if not response:
        return found
    try:
        headers = await response.all_headers() if hasattr(response, "all_headers") else {}
        header_str = " ".join(f"{k}:{v}" for k, v in headers.items()).lower()
        for name, patterns in HEADER_SIGNATURES.items():
            if any(p in header_str for p in patterns):
                found[name] = True
    except Exception:
        pass
    return found


def _scan_page_for_tech(html: str, page_text: str, script_srcs: list, iframe_srcs: list, link_hrefs: list) -> dict:
    """
    Scan HTML, scripts, iframes, links, and visible text for tech signatures.
    Returns dict of category -> set of detected tool names.
    """
    results = {cat: set() for cat in TECH_SIGNATURES}
    html_lower = _normalize_for_match(html)
    text_lower = _normalize_for_match(page_text)
    all_sources = html_lower + " " + " ".join(_normalize_for_match(s) for s in script_srcs + iframe_srcs + link_hrefs)

    for category, tools in TECH_SIGNATURES.items():
        for tool_name, patterns in tools.items():
            for pattern in patterns:
                pat = _normalize_for_match(pattern)
                if pat in html_lower or pat in all_sources:
                    results[category].add(tool_name)
                    break
                # Also check visible text for vendor mentions
                if pat in text_lower and len(pat) > 4:
                    results[category].add(tool_name)
                    break

    return results


async def _collect_page_sources(page: Page) -> tuple:
    """Collect script srcs, iframe srcs, and link hrefs from page."""
    script_srcs, iframe_srcs, link_hrefs = [], [], []
    try:
        for script in await page.query_selector_all("script[src]"):
            src = await script.get_attribute("src")
            if src:
                script_srcs.append(src)
        for iframe in await page.query_selector_all("iframe[src]"):
            src = await iframe.get_attribute("src")
            if src:
                iframe_srcs.append(src)
        for link in await page.query_selector_all("link[href], a[href]"):
            href = await link.get_attribute("href")
            if href and href.startswith(("http", "//")):
                link_hrefs.append(href)
    except Exception:
        pass
    return script_srcs, iframe_srcs, link_hrefs


def _merge_tech_results(accum: dict, new: dict, header_infra: dict = None) -> None:
    """Merge new detection results into accum. In-place."""
    for cat, tools in new.items():
        accum.setdefault(cat, set()).update(tools)
    if header_infra:
        accum.setdefault("infra", set()).update(header_infra.keys())


def _tech_dict_to_flat(tech: dict) -> dict:
    """Convert category sets to flat dict with ', ' joined strings."""
    return {
        cat: ", ".join(sorted(tools)) if tools else "Not Detected"
        for cat, tools in tech.items()
    }


async def detect_tech_stack(
    page: Page,
    context,
    base_url: str,
    initial_response=None,
) -> dict:
    """
    Detect tech stack from up to 3 pages: homepage + /contact + /book (or first booking link).
    Scans HTML, script srcs, iframe srcs, link hrefs, HTTP headers, and visible text.
    Returns flat dict: {"pms": "Cliniko", "booking": "HotDoc", "cms": "WordPress", ...}
    """
    from urllib.parse import urljoin

    accum = {cat: set() for cat in TECH_SIGNATURES}

    # 1. Scan homepage (current page)
    try:
        html = await page.content()
        page_text = await page.inner_text("body") if await page.query_selector("body") else ""
        script_srcs, iframe_srcs, link_hrefs = await _collect_page_sources(page)
        page_results = _scan_page_for_tech(html, page_text, script_srcs, iframe_srcs, link_hrefs)
        header_infra = await detect_from_headers(initial_response) if initial_response else {}
        _merge_tech_results(accum, page_results, header_infra)
    except Exception as e:
        print(f"  Error scanning homepage for tech: {e}")

    # 2. Visit /contact and /book (max 2 extra pages)
    extra_urls = []
    parsed = urlparse(base_url)
    base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
    for path in ["/contact", "/contact-us", "/book", "/booking", "/appointments", "/book-online"]:
        extra_urls.append(urljoin(base, path))

    # Find first booking link on page
    try:
        booking_links = await page.query_selector_all(
            'a[href*="book"], a[href*="booking"], a[href*="appointment"]'
        )
        for link in booking_links[:3]:
            href = await link.get_attribute("href")
            if href:
                full = urljoin(base_url, href)
                if full not in extra_urls and urlparse(full).netloc == parsed.netloc:
                    extra_urls.append(full)
                    break
    except Exception:
        pass

    pages_visited = 1
    for url in extra_urls:
        if pages_visited >= 3:
            break
        try:
            resp = await page.goto(url, timeout=10000, wait_until="domcontentloaded")
            await page.wait_for_timeout(1000)
            html = await page.content()
            page_text = await page.inner_text("body") if await page.query_selector("body") else ""
            script_srcs, iframe_srcs, link_hrefs = await _collect_page_sources(page)
            page_results = _scan_page_for_tech(html, page_text, script_srcs, iframe_srcs, link_hrefs)
            header_infra = await detect_from_headers(resp) if resp else {}
            _merge_tech_results(accum, page_results, header_infra)
            pages_visited += 1
        except Exception:
            continue

    return _tech_dict_to_flat(accum)


def _ensure_sheet_headers(worksheet, tech_cats: list) -> None:
    """Ensure header row has all tech stack columns."""
    headers = [
        "Website URL", "Clinic Name", "Email Provider",
        *[c.replace("_", " ").title() for c in tech_cats],
        "Practitioner Count", "Home Visits", "Instagram", "WhatsApp",
        "Street", "City", "State", "Postcode", "Country", "Phones", "Emails",
        "Scraping Date", "Error Log"
    ]
    try:
        worksheet.update([headers], "A1:Y1")
    except Exception:
        pass


def _print_tech_summary(result: dict) -> None:
    """Print a clean tech stack summary per clinic."""
    tech_cats = list(TECH_SIGNATURES.keys())
    lines = [
        "━" * 70,
        f"🏥 {result.get('clinic_name', 'N/A').upper()}",
        f"📧 Email Provider: {result.get('email_provider', 'Unknown')}",
    ]
    for cat in tech_cats:
        val = result.get(cat, "Not Detected")
        if val != "Not Detected":
            lines.append(f"🔧 {cat.replace('_', ' ').title():20} {val}")
    lines.extend([
        f"👥 Team Size:      ~{result.get('practitioner_count', 0)} members",
        f"🚗 Home Visits:    {result.get('home_visits', 'NO')}",
        f"📱 Social:         Instagram: {result.get('instagram', 'No')} | WhatsApp: {result.get('whatsapp', 'No')}",
    ])
    addr = result.get('address', {})
    if any(addr.values()):
        lines.append(f"📍 Address:          {addr}")
    if result.get('phones'):
        lines.append(f"📞 Phones:           {', '.join(result['phones'])}")
    if result.get('emails'):
        lines.append(f"📮 Emails:           {', '.join(result['emails'])}")
    lines.append("━" * 70)
    print("\n".join(lines))


def check_home_visits(html: str) -> bool:
    """Check if clinic offers home visits from HTML."""
    html_lower = html.lower()
    return any(kw in html_lower for kw in HOME_VISIT_KEYWORDS)


async def scrape_clinic(browser, url: str) -> Dict:
    """Scrape a single clinic website."""
    tech_categories = list(TECH_SIGNATURES.keys())
    result = {
        'url': url,
        'clinic_name': '',
        'email_provider': 'Unknown',
        'practitioner_count': 0,
        'home_visits': 'NO',
        'instagram': 'No',
        'whatsapp': 'No',
        'address': {},
        'phones': [],
        'emails': [],
        'error': None
    }
    for cat in tech_categories:
        result[cat] = 'Not Detected'

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
            response = await page.goto(url, timeout=30000, wait_until='domcontentloaded')
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

        # Detect tech stack (homepage + up to 2 subpages: /contact, /book)
        tech_stack = await detect_tech_stack(page, context, url, initial_response=response)
        for k, v in tech_stack.items():
            result[k] = v

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
        
        # Extract social media (from homepage HTML)
        social = extract_social_media(html)
        result['instagram'] = social['instagram']
        result['whatsapp'] = social['whatsapp']

        # Navigate back to homepage (detect_tech_stack may have left us on /contact or /book)
        try:
            await page.goto(url, timeout=10000, wait_until='domcontentloaded')
            await page.wait_for_timeout(500)
        except Exception:
            pass

        # Get fresh HTML and page text for extraction
        html = await page.content()
        page_text = await page.inner_text('body')
        result['address'] = extract_full_address(html, page_text)
        result['phones'] = extract_all_phones(page_text)
        result['emails'] = extract_all_emails(page_text, html)
        
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
        
        # Column mapping (0-indexed) - tech stack categories each get own column
        # Col A = 0: Website URL
        # Col B = 1: Clinic Name
        # Col C = 2: Email Provider
        # Col D = 3: PMS
        # Col E = 4: Booking
        # Col F = 5: CMS
        # Col G = 6: Email Marketing
        # Col H = 7: Pixels
        # Col I = 8: Payments
        # Col J = 9: Live Chat
        # Col K = 10: Reviews
        # Col L = 11: Infra
        # Col M = 12: Practitioner Count
        # Col N = 13: Home Visits
        # Col O = 14: Instagram
        # Col P = 15: WhatsApp
        # Col Q = 16: Scraping Date
        # Col R = 17: Error Log
        tech_cats = list(TECH_SIGNATURES.keys())
        _ensure_sheet_headers(worksheet, tech_cats)

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
                
                # Check Scraping Date (Col X=23 in current format, Col Q=16 in previous, Col I=8 in legacy)
                scraping_date = (row_data[23].strip() if len(row_data) > 23 else "") or (
                    row_data[16].strip() if len(row_data) > 16 else "") or (
                    row_data[8].strip() if len(row_data) > 8 else ""
                )
                
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
                    
                    # Prepare update values (B through W)
                    tech_cats = list(TECH_SIGNATURES.keys())
                    addr = result.get('address', {})
                    # Use full_address for street when structured fields are missing
                    if addr.get('full_address') and not addr.get('street'):
                        addr = {**addr, 'street': addr['full_address']}
                    update_values = [
                        result.get('clinic_name', ''),
                        result.get('email_provider', 'Unknown'),
                        *[result.get(cat, 'Not Detected') for cat in tech_cats],
                        str(result.get('practitioner_count', 0)),
                        result.get('home_visits', 'NO'),
                        result.get('instagram', 'No'),
                        result.get('whatsapp', 'No'),
                        addr.get('street', ''),
                        addr.get('city', ''),
                        addr.get('state', ''),
                        addr.get('postcode', ''),
                        addr.get('country', ''),
                        ", ".join(result.get('phones', [])),
                        ", ".join(result.get('emails', [])),
                    ]
                    
                    # Get timestamp
                    timestamp = get_current_timestamp()
                    
                    if result.get('error'):
                        error_msg = result['error']
                        worksheet.update_cell(row_num, 24, timestamp)  # Col X (Scraping Date)
                        worksheet.update_cell(row_num, 25, error_msg)  # Col Y (Error Log)
                        error_count += 1
                        print(f"❌ ERROR: {error_msg}")
                    else:
                        worksheet.update_cell(row_num, 25, '')  # Clear error (Col Y)
                        _print_tech_summary(result)

                    # Update columns B through W (data)
                    cell_range = f'B{row_num}:W{row_num}'
                    worksheet.update([update_values], cell_range)

                    # Update scraping date (Col X)
                    worksheet.update_cell(row_num, 24, timestamp)
                    
                    processed_count += 1
                    print(f"✅ Row {row_num} updated successfully")
                    
                except Exception as e:
                    error_msg = f'Unexpected error: {str(e)}'
                    timestamp = get_current_timestamp()
                    worksheet.update_cell(row_num, 24, timestamp)  # Col X (Scraping Date)
                    worksheet.update_cell(row_num, 25, error_msg)  # Col Y (Error Log)
                    
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

