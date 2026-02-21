"""
Core shared functions for crawl_atlas scrapers.
Used by both main_clinics.py and main_ecom.py.
"""

import asyncio
import json
import re
from datetime import datetime
from typing import Dict, Optional
from urllib.parse import unquote, urljoin, urlparse

import aiohttp
import dns.resolver
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.async_api import Page


# Map snake_case provider keys to Title Case display strings.
# "unknown" and "privateemail" are kept lowercase (not in this mapping).
EMAIL_PROVIDER_DISPLAY = {
    "gmail": "Gmail",
    "gmail_direct": "Gmail (direct)",
    "ms_personal": "Microsoft Personal",
    "google_workspace": "Google Workspace",
    "microsoft_365": "Microsoft 365",
    "zoho": "Zoho",
    "proofpoint": "Proofpoint",
    "mimecast": "Mimecast",
    "godaddy": "GoDaddy",
    "fastmail": "Fastmail",
    "namecheap": "Namecheap",
    "mailgun": "Mailgun",
    "sendgrid": "SendGrid",
    "amazon_ses": "Amazon SES",
    "icloud": "iCloud",
    "yahoo": "Yahoo",
    "ventraip": "VentraIP",
    "crazy_domains": "Crazy Domains",
    "netregistry": "NetRegistry",
    "cpanel_shared": "cPanel/Shared Hosting",
}


def _format_email_provider(raw: str) -> str:
    """Format provider string to Title Case. Keeps 'unknown' and 'privateemail' lowercase."""
    if raw in ("unknown", "privateemail"):
        return raw
    parts = [p.strip() for p in raw.split(",")]
    formatted = []
    for p in parts:
        formatted.append(EMAIL_PROVIDER_DISPLAY.get(p.lower(), p.replace("_", " ").title()))
    return ", ".join(formatted)


async def get_email_provider(domain: str) -> str:
    """
    Detect email provider from MX records.
    Returns comma-separated string if multiple providers found.
    Known providers are returned in Title Case (e.g. "Google Workspace").
    "unknown" and "privateemail" remain lowercase.

    Possible return values:
      "Gmail"              — Google personal
      "Google Workspace"   — Google Workspace (business)
      "Microsoft 365"      — Microsoft 365 / Exchange Online
      "Zoho"               — Zoho Mail
      "Proofpoint"         — Proofpoint (email security/gateway)
      "Mimecast"           — Mimecast (email security gateway)
      "GoDaddy"            — GoDaddy / Secureserver hosted email
      "Fastmail"           — Fastmail
      "Namecheap"          — Namecheap Private Email
      "Mailgun"            — Mailgun (transactional)
      "SendGrid"           — SendGrid (transactional)
      "Amazon SES"         — Amazon SES
      "iCloud"             — Apple iCloud
      "Yahoo"              — Yahoo Mail
      "privateemail"       — Generic private/self-hosted (lowercase)
      "unknown"            — MX lookup failed (lowercase)
    """
    try:
        domain = (domain.replace("www.", "")
                        .replace("http://", "")
                        .replace("https://", "")
                        .strip()
                        .split('/')[0])

        answers = await asyncio.get_event_loop().run_in_executor(
            None, lambda: dns.resolver.resolve(domain, 'MX')
        )
        records = [str(r.exchange).lower() for r in answers]

        providers = []

        # Google — distinguish Workspace vs personal
        if any("google" in r or "googlemail" in r for r in records):
            # aspmx.l.google.com = Workspace; gmail-smtp-in = personal Gmail
            if any("aspmx" in r for r in records):
                providers.append("google_workspace")
            else:
                providers.append("gmail")

        # Microsoft
        if any("outlook.com" in r or "protection.outlook.com" in r
               or "mail.protection.outlook.com" in r or "office365" in r for r in records):
            providers.append("microsoft_365")

        # Proofpoint (wraps M365/Google, but worth flagging — signals security investment)
        if any("pphosted.com" in r for r in records):
            providers.append("proofpoint")

        # Mimecast
        if any("mimecast.com" in r for r in records):
            providers.append("mimecast")

        # Zoho
        if any("zoho.com" in r or "zohomail" in r for r in records):
            providers.append("zoho")

        # GoDaddy / Secureserver
        if any("secureserver.net" in r or "godaddy.com" in r for r in records):
            providers.append("godaddy")

        # Fastmail
        if any("fastmail" in r for r in records):
            providers.append("fastmail")

        # Namecheap Private Email
        if any("privateemail.com" in r for r in records):
            providers.append("namecheap")

        # Mailgun (sometimes used as primary MX)
        if any("mailgun.org" in r for r in records):
            providers.append("mailgun")

        # SendGrid
        if any("sendgrid.net" in r for r in records):
            providers.append("sendgrid")

        # Amazon SES
        if any("amazonaws.com" in r or "amazonses.com" in r for r in records):
            providers.append("amazon_ses")

        # iCloud / Apple
        if any("icloud.com" in r or "apple.com" in r for r in records):
            providers.append("icloud")

        # Yahoo
        if any("yahoodns.net" in r or "yahoo.com" in r for r in records):
            providers.append("yahoo")

        # VentraIP / Synergy Wholesale (cPanel-based AU hosting)
        if any("ventraip" in r or "synergywholesale" in r or "vendorinternet" in r
               or "cpanelemailer" in r or "mxroute" in r for r in records):
            providers.append("ventraip")

        # Crazy Domains
        if any("crazydomains" in r for r in records):
            providers.append("crazy_domains")

        # NetRegistry
        if any("netregistry" in r for r in records):
            providers.append("netregistry")

        # cPanel generic (mail.domain.com pattern = typical cPanel default)
        if any(".mail.protection.outlook.com" not in r and "google" not in r and "microsoft" not in r
               and re.search(r"^mail\.", r) for r in records):
            providers.append("cpanel_shared")

        if providers:
            return _format_email_provider(", ".join(providers))

        # Has MX records but no known provider — self-hosted or obscure
        return _format_email_provider("privateemail")

    except dns.resolver.NXDOMAIN:
        return "unknown"
    except dns.resolver.NoAnswer:
        return "unknown"
    except Exception:
        return "unknown"


def detect_email_provider_from_addresses(emails: list) -> str:
    """
    Detect email provider from actual contact email addresses found on the page.
    Catches cases like parramattadentistry.sydney@gmail.com where
    the business uses Gmail directly rather than a custom domain.
    Returns a provider string or empty string if nothing notable found.
    """
    if not emails:
        return ""
    for email in emails:
        domain = email.split("@")[-1].lower()
        if domain == "gmail.com":
            return "Gmail (direct)"
        if domain in ("outlook.com", "hotmail.com", "live.com"):
            return "Microsoft Personal"
        if domain == "yahoo.com":
            return "Yahoo (direct)"
        if domain == "icloud.com":
            return "iCloud (direct)"
    return ""


COOKIE_SIGNATURES = {
    "_shopify_y":     ("cms", "Shopify"),
    "shopify_pay":    ("cms", "Shopify"),
    "hubspotutk":     ("crm", "HubSpot"),
    "__hstc":         ("crm", "HubSpot"),
    "hs":             ("crm", "HubSpot"),  # Guard: literal "hs" cookie set by HubSpot tracking (e.g. on Wix sites)
    "__kla_id":       ("crm", "Klaviyo"),
    "__cf_bm":        ("infra", "Cloudflare"),
    "_cfuvid":        ("infra", "Cloudflare"),
    "intercom-id":    ("live_chat", "Intercom"),
    "intercom-session":("live_chat", "Intercom"),
    "__lc_cid":       ("live_chat", "LiveChat"),
    "_ga":            ("pixels", "Google Analytics 4"),
    "_gid":           ("pixels", "Google Analytics 4"),
    "_fbp":           ("pixels", "Meta Pixel"),
    "_ttp":           ("pixels", "TikTok Pixel"),
    "_hjid":          ("pixels", "Hotjar"),
    "MUID":           ("pixels", "Microsoft Clarity"),
}


def detect_from_cookies(cookies: list) -> dict:
    """
    Detect tech from browser cookies set after page load.
    Returns dict of category -> set of tool names.
    """
    found = {}
    for cookie in cookies:
        name = cookie.get("name", "")
        for cookie_key, (category, tool) in COOKIE_SIGNATURES.items():
            # Guard: short keys (len < 4) like "hs" use exact match only — startswith would match unrelated cookies
            if len(cookie_key) < 4:
                if name == cookie_key:
                    found.setdefault(category, set()).add(tool)
            elif name == cookie_key or name.startswith(cookie_key):
                found.setdefault(category, set()).add(tool)
    return found


META_GENERATOR_SIGNATURES = {
    "wordpress": ("cms", "WordPress"),
    "wix": ("cms", "Wix"),
    "squarespace": ("cms", "Squarespace"),
    "webflow": ("cms", "Webflow"),
    "drupal": ("cms", "Drupal"),
    "joomla": ("cms", "Joomla"),
    "shopify": ("cms", "Shopify"),
    "ghost": ("cms", "Ghost"),
    "weebly": ("cms", "Weebly"),
    "framer": ("cms", "Framer"),
}


def detect_from_meta_generator(html: str) -> dict:
    """
    Check <meta name='generator' content='...'> tag.
    Most reliable CMS signal — it's self-reported.
    """
    found = {}
    pattern = r'<meta[^>]+name=["\']generator["\'][^>]+content=["\'](.*?)["\']'
    match = re.search(pattern, html, re.IGNORECASE)
    if not match:
        # Also try reversed attribute order
        pattern2 = r'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']generator["\']'
        match = re.search(pattern2, html, re.IGNORECASE)
    if match:
        content = match.group(1).lower()
        for keyword, (category, tool) in META_GENERATOR_SIGNATURES.items():
            if keyword in content:
                found.setdefault(category, set()).add(tool)
    return found


JS_INLINE_SIGNATURES = {
    "_learnq":              ("crm", "Klaviyo"),
    "klaviyo":              ("crm", "Klaviyo"),
    "fbq(":                 ("pixels", "Meta Pixel"),
    "fbevents":             ("pixels", "Meta Pixel"),
    "ttq.load":             ("pixels", "TikTok Pixel"),
    "gtag(":                ("pixels", "Google Analytics 4"),
    "window.intercomsettings": ("live_chat", "Intercom"),
    "intercom(":            ("live_chat", "Intercom"),
    "drift.load":           ("live_chat", "Drift"),
    "driftt.com":           ("live_chat", "Drift"),
    "__lc =":               ("live_chat", "LiveChat"),
    "livechat":             ("live_chat", "LiveChat"),
    "mixpanel.init":        ("pixels", "Mixpanel"),
    "heap.load":            ("pixels", "Heap"),
    "hj(":                  ("pixels", "Hotjar"),
    "hotjar":               ("pixels", "Hotjar"),
    "window.clarity":       ("pixels", "Microsoft Clarity"),
    "pintrk(":              ("pixels", "Pinterest"),
    "snaptr(":              ("pixels", "Snapchat"),
    "hsq.push":             ("crm", "HubSpot"),
    "mc.js":                ("crm", "Mailchimp"),
    "mailchimp":            ("crm", "Mailchimp"),
    "activecampaign":       ("crm", "ActiveCampaign"),
    "cliniko":              ("pms_ehr", "Cliniko"),
    "halaxy":               ("pms_ehr", "Halaxy"),
    "hotdoc":               ("booking", "HotDoc"),
    "healthengine":         ("booking", "HealthEngine"),
}

JS_SKIP_DOMAINS = [
    "google", "facebook", "cloudflare", "jquery", "bootstrap",
    "cdn.jsdelivr", "unpkg.com", "cdnjs.cloudflare",
]


async def scan_external_js(script_srcs: list, base_url: str) -> dict:
    """
    Fetch up to 3 third-party JS files and scan first 100kb for inline signatures.
    Returns dict of category -> set of tool names.
    """
    found = {}
    fetched = 0
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    for src in script_srcs:
        if fetched >= 8:
            break
        if not src.startswith("http"):
            continue
        src_lower = src.lower()
        if any(skip in src_lower for skip in JS_SKIP_DOMAINS):
            continue
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    src, timeout=aiohttp.ClientTimeout(total=5), headers=headers
                ) as resp:
                    if resp.status == 200:
                        content = (await resp.text(errors="ignore"))[:100_000].lower()
                        for sig, (category, tool) in JS_INLINE_SIGNATURES.items():
                            if sig in content:
                                found.setdefault(category, set()).add(tool)
                        fetched += 1
        except Exception:
            continue

    return found


ROBOTS_SIGNATURES = {
    "/wp-admin": ("cms", "WordPress"),
    "/wp-content": ("cms", "WordPress"),
    "shopify": ("cms", "Shopify"),
    "squarespace": ("cms", "Squarespace"),
    "wix": ("cms", "Wix"),
    "webflow": ("cms", "Webflow"),
}


async def scan_robots_txt(base_url: str) -> dict:
    """
    Fetch /robots.txt and scan for CMS path patterns.
    Very reliable for WordPress (always has /wp-admin/).
    """
    found = {}
    try:
        parsed = urlparse(base_url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        headers = {"User-Agent": "Mozilla/5.0"}
        async with aiohttp.ClientSession() as session:
            async with session.get(
                robots_url, timeout=aiohttp.ClientTimeout(total=5), headers=headers
            ) as resp:
                if resp.status == 200:
                    content = (await resp.text(errors="ignore")).lower()
                    for sig, (category, tool) in ROBOTS_SIGNATURES.items():
                        if sig in content:
                            found.setdefault(category, set()).add(tool)
    except Exception:
        pass
    return found


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


# Australian state: full name / variants / cities → standard 3-letter abbreviation (uppercase)
STATE_TO_ABBREV = {
    # States (full name + abbrev)
    "new south wales": "NSW", "nsw": "NSW",
    "victoria": "VIC", "vic": "VIC",
    "queensland": "QLD", "qld": "QLD",
    "south australia": "SA", "sa": "SA",
    "western australia": "WA", "wa": "WA",
    "tasmania": "TAS", "tas": "TAS",
    "australian capital territory": "ACT", "act": "ACT",
    "northern territory": "NT", "nt": "NT",
    # Major cities (often extracted as state/region)
    "sydney": "NSW", "melbourne": "VIC", "brisbane": "QLD",
    "adelaide": "SA", "perth": "WA", "hobart": "TAS",
    "darwin": "NT", "canberra": "ACT",
    "newcastle": "NSW", "wollongong": "NSW", "gold coast": "QLD",
    "geelong": "VIC", "ballarat": "VIC", "bendigo": "VIC",
    "townsville": "QLD", "cairns": "QLD", "toowoomba": "QLD",
    "launceston": "TAS", "alice springs": "NT",
}

# Country: variants → standard name
COUNTRY_STANDARD = {
    "australia": "Australia", "australian": "Australia",
    "au": "Australia", "aus": "Australia",
}


def _standardize_state(raw: str) -> str:
    """Normalize Australian state to 3-letter abbreviation (NSW, VIC, etc.) in uppercase."""
    if not raw or not isinstance(raw, str):
        return ""
    key = raw.strip().lower()
    return STATE_TO_ABBREV.get(key, raw.strip().upper() if raw.strip() else "")


def _standardize_country(raw: str) -> str:
    """Normalize country to standard name (e.g. Australia)."""
    if not raw or not isinstance(raw, str):
        return ""
    key = raw.strip().lower()
    return COUNTRY_STANDARD.get(key, raw.strip() if raw.strip() else "")


def _standardize_phone(phone: str) -> str:
    """
    Normalize phone to +country_code format with consistent spacing.
    Australian: +61 2 1234 5678 (landline), +61 412 345 678 (mobile),
    +61 1300 123 456 (1300), +61 1800 123 456 (1800).
    Other international numbers get +XX and spaced formatting.
    Rejects invalid/short numbers (e.g. area codes like (02) alone).
    """
    if not phone or not isinstance(phone, str):
        return ""
    digits = re.sub(r"\D", "", phone)
    # Reject area-code-only or too-short numbers (e.g. "(02)", "(03)")
    if len(digits) < 8:
        return ""
    # Australian: 10 digits (0XXXXXXXXX) or 1300/1800 → add 61
    if len(digits) == 10 and digits.startswith("0"):
        digits = "61" + digits[1:]
    elif len(digits) == 10 and digits.startswith(("13", "18")):
        digits = "61" + digits
    elif len(digits) == 9 and not digits.startswith("61"):
        digits = "61" + digits
    # Australian formatting
    if digits.startswith("61") and len(digits) >= 11:
        if len(digits) == 12 and digits[2:6] in ("1300", "1800"):
            return f"+{digits[:2]} {digits[2:6]} {digits[6:9]} {digits[9:]}"
        if len(digits) == 11 and digits[2] == "4":
            return f"+{digits[:2]} {digits[2:5]} {digits[5:8]} {digits[8:]}"
        if len(digits) == 11:
            return f"+{digits[:2]} {digits[2:3]} {digits[3:7]} {digits[7:]}"
        if len(digits) == 12:
            return f"+{digits[:2]} {digits[2:]}"
    # UK +44, NZ +64, etc. - format as +XX XXX XXX XXXX
    if len(digits) >= 10 and digits[0] in ("1", "2", "3", "4", "5", "6", "7", "8", "9"):
        cc_len = 1 if digits.startswith("1") else 2
        rest = digits[cc_len:]
        if len(rest) >= 6:
            return f"+{digits[:cc_len]} {rest[:3]} {rest[3:6]} {rest[6:]}".rstrip()
        return f"+{digits[:cc_len]} {rest}"
    return re.sub(r"\s+", " ", phone.strip())


# Australian state abbreviations for address parsing
_AU_STATES = r'(?:NSW|VIC|QLD|SA|WA|TAS|ACT|NT)'
# Street type suffixes (for validation)
_STREET_TYPES = r'(?:Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Drive|Dr\.?|Boulevard|Blvd\.?|Lane|Ln\.?|Place|Pl\.?|Court|Ct\.?|Way|Parade|Pde\.?|Close|Cl\.?|Terrace|Tce\.?|Crescent|Cres\.?|Circuit|Cct\.?|Highway|Hwy\.?|Grove|Grv\.?|Cove|View|Vista)'


def _extract_street_only(address_str: str) -> str:
    """Extract just the street (number + name + type) from an address string that may contain
    suburb, state, postcode. Handles Australian format: 'Street, Suburb STATE postcode'.
    Returns empty string if the result doesn't look like a valid street."""
    if not address_str or not address_str.strip():
        return ""
    s = address_str.strip()
    # Remove Australian state + postcode from the end (e.g. ", Heidelberg VIC 3084", " SA 5110")
    s = re.sub(r',?\s+' + _AU_STATES + r'\s+\d{4}\s*$', '', s, flags=re.IGNORECASE)
    s = s.strip().rstrip(',')
    # Strategy 1: Match street pattern (number + name + type) - handles "Level 1, 101 Burgundy St"
    street_pattern = r'(\d+\s+[\w\s]+?' + _STREET_TYPES + r')'
    m = re.search(street_pattern, s, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # Strategy 2: First segment if it looks like a street (has type or starts with number)
    parts = re.split(r'[,\n]+', s)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        has_street_type = bool(re.search(_STREET_TYPES, part, re.IGNORECASE))
        starts_with_number = bool(re.match(r'\d+', part))
        if has_street_type or starts_with_number:
            return part
    return ""


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
                    raw_street = addr.get("streetAddress", "")
                    address["street"] = _extract_street_only(raw_street) if raw_street else ""
                    if not address["street"] and raw_street:
                        address["street"] = raw_street  # Keep original if extraction fails
                    address["city"] = addr.get("addressLocality", "")
                    address["state"] = _standardize_state(addr.get("addressRegion", ""))
                    address["postcode"] = addr.get("postalCode", "")
                    country = addr.get("addressCountry", "")
                    if isinstance(country, dict):
                        country = country.get("name", "")
                    address["country"] = _standardize_country(country or "")
                    if any(address.values()):
                        return address
        except Exception:
            continue
    return address


def extract_address_from_text(text: str) -> dict:
    """Fallback: regex-based address extraction from visible page text."""
    address = {}
    # Australian postcode pattern (4 digits), capture state abbreviation
    au_pattern = r'(\d+\s[\w\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Lane|Ln|Place|Pl|Court|Ct|Way|Parade|Pde|Close|Cl|Terrace|Tce)[,\s]+[\w\s]+[,\s]+(NSW|VIC|QLD|SA|WA|TAS|ACT|NT)[,?\s]+(\d{4}))'
    match = re.search(au_pattern, text, re.IGNORECASE)
    if match:
        address["full_address"] = match.group(0).strip()
        address["state"] = _standardize_state(match.group(2))
        address["postcode"] = match.group(3)

    # Standalone state mention (e.g. "Sydney, NSW" or "Melbourne VIC")
    if not address.get("state"):
        state_match = re.search(r'\b(NSW|VIC|QLD|SA|WA|TAS|ACT|NT)\b', text, re.IGNORECASE)
        if state_match:
            address["state"] = _standardize_state(state_match.group(1))

    # Postcode extraction as minimum signal
    if not address.get("postcode"):
        postcode_match = re.search(r'\b(\d{4})\b', text)
        if postcode_match:
            address["postcode"] = postcode_match.group(1)

    # Default country for Australian addresses
    if address and not address.get("country"):
        address["country"] = "Australia"

    return address


def extract_full_address(html: str, page_text: str) -> dict:
    """Try JSON-LD first, then text fallback. Returns dict with street/city/state/postcode/country (standardized)."""
    addr = extract_address_from_jsonld(html)
    if not addr:
        addr = extract_address_from_text(page_text)
    # Ensure state/country are standardized; extract state from full_address when missing
    if addr:
        if addr.get("state"):
            addr["state"] = _standardize_state(addr["state"])
        elif addr.get("full_address"):
            state_match = re.search(r"\b(NSW|VIC|QLD|SA|WA|TAS|ACT|NT)\b", addr["full_address"], re.IGNORECASE)
            if state_match:
                addr["state"] = _standardize_state(state_match.group(1))
        if addr.get("country"):
            addr["country"] = _standardize_country(addr["country"])
        elif not addr.get("country") and (addr.get("state") or addr.get("postcode")):
            addr["country"] = "Australia"
        # Clean street: strip suburb/state/postcode when present; use full_address when street missing
        if addr.get("street"):
            cleaned = _extract_street_only(addr["street"])
            addr["street"] = cleaned if cleaned else addr["street"]
        elif addr.get("full_address"):
            addr["street"] = _extract_street_only(addr["full_address"])
    return addr


def extract_all_phones(text: str, html: str = "") -> list:
    """Extract all phone numbers from text and tel: links in HTML, deduplicated and standardized (+country_code with spacing)."""
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
            raw = re.sub(r'\s+', ' ', m.group(0)).strip()
            standardized = _standardize_phone(raw)
            if standardized:
                found.add(standardized)
    # Also parse tel: from raw HTML (allow spaces so we capture full numbers like "(02) 9332 2531")
    for match in re.finditer(r'href=["\']tel:([^"\'#]+)', html, re.IGNORECASE):
        raw = unquote(match.group(1).strip())
        standardized = _standardize_phone(raw)
        if standardized:
            found.add(standardized)
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
        "instagram": "yes" if "instagram.com" in html_lower else "no",
        "whatsapp": "yes" if "wa.me" in html_lower or "whatsapp" in html_lower else "no",
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


# Known external booking vendor domains
EXTERNAL_BOOKING_DOMAINS = [
    "centaurportal.com",
    "hotdoc.com.au",
    "healthengine.com.au",
    "cliniko.com",
    "halaxy.com",
    "powerdiary.com",
    "nookal.com",
    "coreplus.com.au",
    "splose.com",
    "janeapp.com",
    "acuityscheduling.com",
    "calendly.com",
    "setmore.com",
    "zocdoc.com",
    "doctolib.com",
    "docplanner.com",
    "automed.com.au",
    "frontdesk.com.au",
    "booking.frontdesk.com.au",
    "mindbodyonline.com",
    "fresha.com",
    "gettimely.com",
    "simplepractice.com",
    "practicebetter.io",
    "meetings.hubspot.com",
    "meetings.hs-sites.com",
    "connect.nookal.com",
    "portal.nookal.com",
    "clinics.janeapp.com",
    "secure.cliniko.com",
    "booking.cliniko.com",
    "book.hotdoc.com.au",
    "booking.healthengine.com.au",
    "medirecords.com",
]

# Link text / href patterns that suggest a booking link
BOOKING_LINK_PATTERNS = [
    "book", "booking", "appointment", "schedule",
    "reserve", "book now", "book online", "make a booking",
    "request appointment", "book an appointment",
]


async def detect_booking_type(page, base_url: str) -> dict:
    """
    Detect whether clinic booking is embedded, external_vendor, or not_detected.

    Logic:
    1. Find booking links on the current page (button text + href patterns)
    2. For each candidate link:
       - If href domain != clinic domain AND matches known vendor → external_vendor
       - If href is a subdomain of clinic domain (booking.myclinic.com.au) → embedded
       - If iframe src matches known vendor → external_vendor (embedded iframe)
       - If iframe src is own subdomain → embedded
    3. Return first confident match.

    Returns:
        {
            "booking_type": "embedded" | "external_vendor" | "not_detected",
            "booking_vendor": str,   # e.g. "HotDoc", "Cliniko", "" if embedded
            "booking_url": str       # the detected booking URL
        }
    """
    from urllib.parse import urljoin, urlparse

    result = {
        "booking_type": "not_detected",
        "booking_vendor": "",
        "booking_url": "",
    }

    try:
        clinic_domain = urlparse(base_url).netloc.lower().replace("www.", "")
        clinic_root = ".".join(clinic_domain.split(".")[-2:])  # e.g. "myclinic.com.au"

        # ----------------------------------------------------------------
        # Step 1: Check iframes first — most reliable signal
        # An iframe from an external vendor = External (even if visually embedded)
        # An iframe from own subdomain = Embedded
        # ----------------------------------------------------------------
        iframes = await page.query_selector_all("iframe[src]")
        for iframe in iframes:
            src = await iframe.get_attribute("src") or ""
            if not src:
                continue
            src_lower = src.lower()
            iframe_domain = urlparse(src).netloc.lower().replace("www.", "")

            # Check against known vendors
            for vendor_domain in EXTERNAL_BOOKING_DOMAINS:
                if vendor_domain in src_lower:
                    vendor_name = _vendor_name_from_domain(vendor_domain)
                    result.update({
                        "booking_type": "external_vendor",
                        "booking_vendor": vendor_name,
                        "booking_url": src,
                    })
                    return result

            # Own subdomain iframe = Embedded
            if clinic_root in iframe_domain and iframe_domain != clinic_domain:
                result.update({
                    "booking_type": "embedded",
                    "booking_vendor": "",
                    "booking_url": src,
                })
                return result

        # ----------------------------------------------------------------
        # Step 2: Scan booking links (buttons, nav, CTAs)
        # ----------------------------------------------------------------
        all_links = await page.query_selector_all("a[href]")
        booking_candidates = []

        for link in all_links:
            try:
                href = (await link.get_attribute("href") or "").strip()
                text = (await link.inner_text()).lower().strip()

                if not href or href.startswith(("mailto:", "tel:", "#")):
                    continue

                is_booking_link = any(kw in text or kw in href.lower() for kw in BOOKING_LINK_PATTERNS)
                if is_booking_link:
                    full_url = urljoin(base_url, href)
                    booking_candidates.append((text, full_url))
            except Exception:
                continue

        # ----------------------------------------------------------------
        # Step 3: Classify each booking candidate
        # ----------------------------------------------------------------
        for text, candidate_url in booking_candidates:
            candidate_domain = urlparse(candidate_url).netloc.lower().replace("www.", "")

            # External vendor check
            for vendor_domain in EXTERNAL_BOOKING_DOMAINS:
                if vendor_domain in candidate_domain:
                    vendor_name = _vendor_name_from_domain(vendor_domain)
                    result.update({
                        "booking_type": "external_vendor",
                        "booking_vendor": vendor_name,
                        "booking_url": candidate_url,
                    })
                    return result

            # Subdomain of clinic = Embedded (e.g. booking.myclinic.com.au)
            if clinic_root in candidate_domain and candidate_domain != clinic_domain:
                result.update({
                    "booking_type": "embedded",
                    "booking_vendor": "",
                    "booking_url": candidate_url,
                })
                return result

            # Same domain with /book path = Embedded
            if candidate_domain == clinic_domain and any(
                kw in urlparse(candidate_url).path.lower()
                for kw in ["/book", "/booking", "/appointment"]
            ):
                result.update({
                    "booking_type": "embedded",
                    "booking_vendor": "",
                    "booking_url": candidate_url,
                })
                return result

        # ----------------------------------------------------------------
        # Step 4: Fallback — scan raw HTML for vendor domain fingerprints
        # (catches JS-injected booking, onclick handlers, data-* attrs)
        # ----------------------------------------------------------------
        try:
            raw_html = await page.content()
            raw_lower = raw_html.lower()
            for vendor_domain in EXTERNAL_BOOKING_DOMAINS:
                if vendor_domain in raw_lower:
                    vendor_name = _vendor_name_from_domain(vendor_domain)
                    result.update({
                        "booking_type": "external_vendor",
                        "booking_vendor": vendor_name,
                        "booking_url": f"detected in HTML: {vendor_domain}",
                    })
                    return result
        except Exception:
            pass

    except Exception as e:
        result["booking_vendor"] = f"error: {str(e)[:60]}"

    # Ensure booking_vendor is explicitly "not_detected" when no vendor was found
    if result["booking_type"] == "not_detected" and not result["booking_vendor"]:
        result["booking_vendor"] = "not_detected"

    return result


def _vendor_name_from_domain(domain: str) -> str:
    """Map a vendor domain to a clean display name."""
    mapping = {
        "centaurportal.com": "D4W eAppointments",
        "hotdoc.com.au": "HotDoc",
        "healthengine.com.au": "HealthEngine",
        "cliniko.com": "Cliniko",
        "halaxy.com": "Halaxy",
        "powerdiary.com": "Power Diary",
        "nookal.com": "Nookal",
        "coreplus.com.au": "CorePlus",
        "splose.com": "Splose",
        "janeapp.com": "Jane App",
        "acuityscheduling.com": "Acuity",
        "calendly.com": "Calendly",
        "setmore.com": "Setmore",
        "zocdoc.com": "Zocdoc",
        "doctolib.com": "Doctolib",
        "docplanner.com": "Docplanner",
        "automed.com.au": "AutoMed",
        "frontdesk.com.au": "Front Desk",
        "mindbodyonline.com": "Mindbody",
        "fresha.com": "Fresha",
        "gettimely.com": "Timely",
        "hubspot.com": "HubSpot Meetings",
        "simplepractice.com": "SimplePractice",
        "medirecords.com": "MediRecords",
    }
    for key, name in mapping.items():
        if key in domain:
            return name
    return domain


MULTI_LOCATION_NAV_KEYWORDS = [
    "our clinics", "our locations", "find a clinic", "find us",
    "all locations", "clinic locations", "find a location",
    "our practices", "our centres", "our centers",
    "locations near you", "find your clinic",
]

MULTI_LOCATION_URL_PATHS = [
    "/locations", "/our-clinics", "/find-us", "/clinics",
    "/our-locations", "/find-a-clinic", "/practices",
    "/our-centres", "/our-centers", "/find-a-practice",
]


async def detect_multi_location(page, base_url: str, phones: list, postcodes: list) -> dict:
    """
    Detect whether a clinic is a multi-location group using 3 methods.

    Method B runs first (zero extra page visits) — uses already-scraped phones/postcodes.
    Method C scans nav links on current page.
    Method A visits a locations page and counts address blocks (last resort).

    Returns:
        {
            "multi_location": "YES" / "NO",
            "location_count_estimate": int,
            "detection_method": str
        }
    """
    result = {
        "multi_location": "no",
        "location_count_estimate": 1,
        "detection_method": "none",
    }

    parsed = urlparse(base_url)
    base = f"{parsed.scheme or 'https'}://{parsed.netloc}"

    # --- Method B: phone/postcode repetition (free, no page visit) ---
    unique_postcodes = set(postcodes) if postcodes else set()
    unique_phones = set(phones) if phones else set()

    if len(unique_postcodes) > 1 or len(unique_phones) > 3:
        result["multi_location"] = "yes"
        result["location_count_estimate"] = max(len(unique_postcodes), len(unique_phones) // 2)
        result["detection_method"] = "phone/postcode repetition"
        return result  # short-circuit, no extra page needed

    # --- Method C: nav/menu keyword scan (free, current page) ---
    nav_triggered = False
    try:
        nav_links = await page.query_selector_all(
            "nav a, header a, [class*='menu'] a, [class*='nav'] a"
        )
        for link in nav_links:
            try:
                text = (await link.inner_text()).lower().strip()
                href = (await link.get_attribute("href") or "").lower()
                if any(kw in text or kw in href for kw in MULTI_LOCATION_NAV_KEYWORDS):
                    result["multi_location"] = "yes"
                    result["detection_method"] = f"nav keyword: '{text}'"
                    nav_triggered = True
                    break
            except Exception:
                continue
    except Exception:
        pass

    # --- Method A: visit locations page, count address blocks ---
    locations_found = 0
    visited_path = None

    for path in MULTI_LOCATION_URL_PATHS:
        candidate_url = urljoin(base, path)
        try:
            await page.goto(candidate_url, timeout=10000, wait_until="domcontentloaded")
            await page.wait_for_timeout(1000)

            # Skip if redirected back to homepage
            if urlparse(page.url).path in ("", "/", "/home"):
                continue

            page_text = await page.inner_text("body") if await page.query_selector("body") else ""

            au_postcodes = re.findall(r'\b\d{4}\b', page_text)
            unique_on_page = set(au_postcodes)
            state_mentions = re.findall(r'\b(?:NSW|VIC|QLD|SA|WA|TAS|ACT|NT)\b', page_text, re.IGNORECASE)
            locations_found = max(len(unique_on_page), len(state_mentions))
            visited_path = path

            if locations_found >= 1:
                break
        except Exception:
            continue

    if locations_found > 1:
        result["multi_location"] = "yes"
        result["location_count_estimate"] = locations_found
        result["detection_method"] = f"locations page ({visited_path})"
    elif locations_found == 1 and not nav_triggered:
        result["location_count_estimate"] = 1

    # If Method C flagged yes but Method A found no count
    if result["multi_location"] == "yes" and result["location_count_estimate"] <= 1:
        result["location_count_estimate"] = 2

    return result


# Category keyword buckets — order matters for tie-breaking priority
CATEGORY_KEYWORDS = {
    "Dental": [
        "dentist", "dental", "orthodontist", "endodontist", "periodontist",
        "oral health", "teeth whitening", "implant", "cosmetic dentistry",
        "tooth", "denture", "braces", "invisalign", "root canal",
    ],
    "GP / General Practice": [
        "general practice", "general practitioner", "gp clinic", "family doctor",
        "family medicine", "bulk billing", "medicare", "primary care",
        "walk-in clinic", "medical centre", "medical center",
    ],
    "Physio / Rehab": [
        "physiotherapy", "physiotherapist", "physio", "rehabilitation",
        "sports injury", "exercise physiology", "exercise physiologist",
        "hydrotherapy", "musculoskeletal", "sports medicine",
        "pilates", "dry needling", "manual therapy",
    ],
    "Allied Health": [
        "speech therapy", "speech pathology", "speech pathologist",
        "occupational therapy", "occupational therapist",
        "psychology", "psychologist", "counselling", "counseling",
        "dietitian", "dietician", "nutritionist",
        "podiatry", "podiatrist", "chiropodist",
        "audiology", "audiologist",
        "myotherapy", "remedial massage",
        "osteopath", "osteopathy",
        "chiropractic", "chiropractor",
        "naturopath", "naturopathy",
    ],
    "Specialist": [
        "specialist", "cardiologist", "cardiology",
        "dermatologist", "dermatology", "skin specialist",
        "oncologist", "oncology", "cancer centre",
        "urologist", "urology",
        "neurologist", "neurology",
        "gastroenterologist", "gastroenterology",
        "endocrinologist", "endocrinology",
        "ophthalmologist", "ophthalmology", "eye specialist",
        "gynaecologist", "gynaecology", "obstetrics",
        "paediatrician", "paediatrics",
        "rheumatologist", "rheumatology",
        "psychiatrist", "psychiatry",
        "orthopaedic", "orthopedic",
        "plastic surgeon", "cosmetic surgeon",
    ],
}

# Weight multipliers: title/h1 matches count more than body text
FIELD_WEIGHTS = {
    "title": 4,
    "meta_description": 3,
    "h1": 3,
    "body": 1,
}


def classify_clinic_category(html: str, page_text: str) -> dict:
    """
    Classify clinic into primary category using weighted keyword matching.
    Checks <title>, <meta description>, <h1>, and visible body text.
    Returns: {"primary_category": str, "confidence_score": float, "scores": dict}
    """
    # Extract fields to score separately
    def get_field(pattern, text, flags=re.IGNORECASE | re.DOTALL):
        m = re.search(pattern, text, flags)
        return m.group(1).lower() if m else ""

    fields = {
        "title":            get_field(r'<title[^>]*>(.*?)</title>', html),
        "meta_description": get_field(r'<meta[^>]*name=["\']description["\'][^>]*content=["\'](.*?)["\']', html),
        "h1":               get_field(r'<h1[^>]*>(.*?)</h1>', html),
        "body":             (page_text or "").lower(),
    }

    scores = {cat: 0.0 for cat in CATEGORY_KEYWORDS}

    for field_name, field_text in fields.items():
        weight = FIELD_WEIGHTS.get(field_name, 1)
        for category, keywords in CATEGORY_KEYWORDS.items():
            for kw in keywords:
                if kw.lower() in field_text:
                    scores[category] += weight

    # Normalize to 0-100 confidence
    total = sum(scores.values())
    if total == 0:
        return {
            "primary_category": "Unknown",
            "confidence_score": 0.0,
            "all_scores": scores,
        }

    top_cat = max(scores, key=scores.get)
    top_score = scores[top_cat]
    second_score = sorted(scores.values(), reverse=True)[1] if len(scores) > 1 else 0

    # Tie = top two within 20% of each other
    if second_score > 0 and top_score / second_score < 1.2:
        top_cat = "Mixed / Multidisciplinary"

    confidence = round((top_score / total) * 100, 1)

    return {
        "primary_category": top_cat,
        "confidence_score": confidence,
        "all_scores": scores,
    }


# Stack priority: higher index = preferred when multiple tools detected in same category.
# Categories NOT listed (e.g. pixels) are left as-is — all values shown.
STACK_PRIORITY = {
    "cms": [
        "Joomla", "Umbraco", "Ghost", "Weebly", "Drupal", "Sitecore",
        "Framer", "Webflow", "Squarespace", "Wix", "HubSpot CMS", "Shopify",
        "WordPress",
    ],
    "pms_ehr": [
        "MedicalDirector", "Genie Solutions", "Clinic to Cloud", "ProCare",
        "Titanium", "Bluechip", "Shexie", "Medilink", "PrimaryClinic",
        "Communicare", "MasterCare", "Audit4", "Profile (Intrahealth)",
        "Zedmed", "Best Practice", "AutoMed", "MediRecords", "MediRecords (inferred)",
        "MediRecords (Clinical CRM)", "Dental4Windows", "Exact (SOE)", "Praktika",
        "Oasis Dental", "Dentrix", "Core Practice", "Epic", "Cerner",
        "Allscripts", "Meditech", "TrakCare", "Athenahealth", "DrChrono",
        "AdvancedMD", "Kareo", "Practice Fusion", "NextGen", "eClinicalWorks",
        "Greenway Health", "Salesforce Health Cloud", "Mindbody", "Fresha",
        "Timely", "Cosmetri", "Zandamed",
        "Nookal", "Jane App", "Halaxy", "Power Diary", "Splose", "Coreplus",
        "Practice Better", "SimplePractice", "Carepatron", "WriteUpp", "PracSuite",
        "TM2 / TM3", "Smartsoft Front Desk", "Xestro", "PPMP",
        "Genea",
    ],
    "crm": [
        "Mailgun", "Send App", "MediRecords (Clinical CRM)", "MediRecords",
        "Campaign Monitor", "Brevo", "ConvertKit", "Constant Contact",
        "Podium", "Birdeye", "PatientPop", "Keap", "Pipedrive", "Zoho CRM",
        "ActiveCampaign", "Mailchimp", "Klaviyo", "Salesforce",
        "LeadConnector", "GoHighLevel", "HubSpot",
    ],
    "infra": [
        "cPanel/Apache", "Apache", "nginx", "LiteSpeed", "VentraIP", "cPanel",
        "Cloudflare", "WP Engine", "Kinsta",
    ],
}


def _deduplicate_tech(result: dict) -> dict:
    """
    Deduplicate known duplicate/sibling pairs before apply_stack_priority.
    - LeadConnector + GoHighLevel in crm → keep only GoHighLevel
    - Wix + WordPress in cms → keep only WordPress (Wix embeds wp paths; WP wins)
    - Cliniko + Cliniko Booking → merge to Cliniko
    - MediRecords + MediRecords (Clinical CRM) → merge to MediRecords
    """
    # (category, drop, keep, both_only): both_only=True = only remove drop when both present
    rules = [
        ("crm", "LeadConnector", "GoHighLevel", False),
        ("crm", "MediRecords (Clinical CRM)", "MediRecords", False),
        ("cms", "Wix", "WordPress", True),  # Wix embeds wp paths; only when both
        ("pms_ehr", "Cliniko Booking", "Cliniko", False),
        ("pms_ehr", "MediRecords (Clinical CRM)", "MediRecords", False),
        ("booking", "Cliniko Booking", "Cliniko", False),
    ]
    for category, drop, keep, both_only in rules:
        val = result.get(category, "not_detected")
        if val == "not_detected" or not val:
            continue
        parts = [p.strip() for p in str(val).split(",") if p.strip()]
        if drop in parts and keep in parts:
            parts = [p for p in parts if p != drop]
        elif drop in parts and not both_only:
            parts = [keep if p == drop else p for p in parts]
        new_val = ", ".join(dict.fromkeys(parts)) if parts else "not_detected"
        result[category] = new_val
    return result


def apply_stack_priority(category: str, values: list) -> list:
    """
    Sort multi-value list using STACK_PRIORITY so the 'heaviest' tool appears first,
    but DO NOT drop any tools.
    Higher index in priority list = preferred (sorted to the front).
    Returns original list unchanged if category not in STACK_PRIORITY or category is 'pixels'.
    """
    if not values or category not in STACK_PRIORITY or category == "pixels":
        return values

    priority_list = STACK_PRIORITY[category]

    def get_weight(val):
        try:
            return -priority_list.index(val)  # negative so highest index sorts first
        except ValueError:
            return 0  # unknown tools go to the end

    return sorted(values, key=get_weight)


def apply_stack_priority_to_result(result: dict) -> dict:
    """
    Apply stack priority SORTING to each tech category in result.
    Keeps all detected tools — most important tool appears first.
    Skips pixels (always show all, no sorting needed).
    For infra: Cloudflare always listed first, remaining hosts sorted by priority.
    """
    for category in list(result.keys()):
        if category == "pixels":
            continue
        if category not in STACK_PRIORITY:
            continue

        val = result.get(category, "not_detected")
        if val == "not_detected" or not val:
            continue

        parts = [p.strip() for p in str(val).split(",") if p.strip()]
        if len(parts) <= 1:
            continue

        if category == "infra":
            cloudflare = "Cloudflare"
            others = [p for p in parts if p != cloudflare]
            if cloudflare in parts and others:
                sorted_hosts = apply_stack_priority("infra", others)
                result[category] = f"{cloudflare}, " + ", ".join(sorted_hosts)
            else:
                result[category] = ", ".join(apply_stack_priority("infra", parts))
            continue

        sorted_parts = apply_stack_priority(category, parts)
        result[category] = ", ".join(sorted_parts)

    return result


def _test_apply_stack_priority() -> None:
    r1 = apply_stack_priority("cms", ["Joomla", "Umbraco", "WordPress"])
    assert r1 == ["WordPress", "Umbraco", "Joomla"], f"cms sort: got {r1}"
    print(f"  apply_stack_priority('cms', [...]) -> {r1} OK")

    r2 = apply_stack_priority("crm", ["Mailchimp", "HubSpot", "Klaviyo"])
    assert r2[0] == "HubSpot", f"crm: HubSpot should be first, got {r2}"
    print(f"  apply_stack_priority('crm', [...]) -> {r2} OK")

    r3 = apply_stack_priority("pixels", ["Meta Pixel", "Google Analytics 4"])
    assert r3 == ["Meta Pixel", "Google Analytics 4"], f"pixels unchanged: got {r3}"
    print(f"  apply_stack_priority('pixels', [...]) -> (unchanged) OK")

    print("  All _test_apply_stack_priority assertions passed.")
