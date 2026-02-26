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
# "not_detected" and "privateemail" are kept lowercase (not in this mapping).
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
    """Format provider string to Title Case. Keeps 'not_detected' and 'privateemail' lowercase."""
    if raw in ("not_detected", "privateemail"):
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
    "not_detected" and "privateemail" remain lowercase.

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
      "not_detected"       — MX lookup failed (lowercase)
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
        return "not_detected"
    except dns.resolver.NoAnswer:
        return "not_detected"
    except Exception:
        return "not_detected"


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


FRAMEWORK_COOKIE_SIGNATURES = {
    # Laravel
    "XSRF-TOKEN":           ("cms", "Laravel (Custom)"),
    "_token":               ("cms", "Laravel (Custom)"),
    # Django
    "csrftoken":            ("cms", "Django (Custom)"),
    "sessionid":            ("cms", "Django (Custom)"),
    # Ruby on Rails
    "_rails_session":       ("cms", "Rails (Custom)"),
    # ASP.NET
    "ASP.NET_SessionId":    ("cms", "ASP.NET (Custom)"),
    "__RequestVerificationToken": ("cms", "ASP.NET (Custom)"),
    # Next.js / Vercel
    "__next_hmr_cb":        ("cms", "Next.js (Custom)"),
    "next-auth.session-token": ("cms", "Next.js (Custom)"),
    # Phoenix / Elixir
    "_csrf_token":          ("cms", "Phoenix (Custom)"),
}

# Session cookie patterns that confirm Laravel when combined with XSRF-TOKEN
LARAVEL_SESSION_PATTERN = re.compile(r'^[a-z0-9_]+_session$')


def detect_framework_from_cookies(cookies: list) -> dict:
    """
    Detect backend framework from session/CSRF cookie naming conventions.
    Only fires when NO known CMS (WordPress, Wix, Squarespace, Shopify etc.)
    is already detected — avoids false positives on WP sites that also set
    XSRF-TOKEN via a plugin.
    Returns dict of category -> set of tool names.
    """
    found = {}
    cookie_names = [c.get("name", "") for c in cookies]

    for name in cookie_names:
        # Direct match
        if name in FRAMEWORK_COOKIE_SIGNATURES:
            cat, tool = FRAMEWORK_COOKIE_SIGNATURES[name]
            found.setdefault(cat, set()).add(tool)
        # Laravel session cookie pattern: appname_session
        if LARAVEL_SESSION_PATTERN.match(name) and "XSRF-TOKEN" in cookie_names:
            found.setdefault("cms", set()).add("Laravel (Custom)")

    return found


CSP_SIGNATURES = {
    "mkt.dynamics.com":         ("crm", "Microsoft Dynamics 365"),
    "dynamics.com":             ("crm", "Microsoft Dynamics 365"),
    "cxondemand.com":           ("crm", "NICE CXone"),
    "salesforce.com":           ("crm", "Salesforce"),
    "pardot.com":               ("crm", "Salesforce"),
    "marketo.net":              ("crm", "Adobe Marketo"),
    "eloqua.com":               ("crm", "Oracle Eloqua"),
    "hsforms.com":              ("crm", "HubSpot"),
    "hs-scripts.com":           ("crm", "HubSpot"),
    "klaviyo.com":              ("crm", "Klaviyo"),
    "mypurecloud.com":          ("live_chat", "Genesys"),
    "mypurecloud.com.au":       ("live_chat", "Genesys"),
    "genesys.com":              ("live_chat", "Genesys"),
    "zopim.com":                ("live_chat", "Zendesk"),
    "zdassets.com":             ("live_chat", "Zendesk"),
    "intercom.io":              ("live_chat", "Intercom"),
    "tawk.to":                  ("live_chat", "Tawk.to"),
    "formstack.com":            ("forms", "Formstack"),
    "wpforms.com":              ("forms", "WPForms"),
    "typeform.com":             ("forms", "Typeform"),
    "jotform.com":              ("forms", "JotForm"),
    "stripe.com":               ("payments", "Stripe"),
    "authorize.net":            ("payments", "Authorize.net"),
    "medipass.com.au":          ("payments", "Medipass"),
    "coviu.com":                ("telehealth", "Coviu"),
    "zoom.us":                  ("telehealth", "Zoom"),
    "doxy.me":                  ("telehealth", "Doxy.me"),
}


def parse_csp_header(header_value: str) -> dict:
    """
    Scan Content-Security-Policy header value for known vendor domains.
    Returns dict of category -> set of tool names.
    High-value signal: enterprise tools whitelist their own domains in CSP
    even when their scripts load lazily or are behind GTM.
    """
    found = {}
    if not header_value:
        return found
    header_lower = header_value.lower()
    for domain, (category, tool) in CSP_SIGNATURES.items():
        if domain in header_lower:
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
    "divi":      ("cms", "WordPress (Divi)"),
    "elementor": ("cms", "WordPress (Elementor)"),
    "avada":     ("cms", "WordPress (Avada)"),
    "beaver builder": ("cms", "WordPress (Beaver Builder)"),
    "wpbakery":  ("cms", "WordPress (WPBakery)"),
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
    # CMS
    "/wp-admin":        ("cms",   "WordPress"),
    "/wp-content":      ("cms",   "WordPress"),
    "shopify":          ("cms",   "Shopify"),
    "squarespace":      ("cms",   "Squarespace"),
    "wix":              ("cms",   "Wix"),
    "webflow":          ("cms",   "Webflow"),
    # WordPress plugins (Disallow paths)
    "/wp-content/uploads/wpforms/":          ("forms",  "WPForms"),
    "/wp-content/uploads/gravity_forms/":    ("forms",  "Gravity Forms"),
    "/wp-content/plugins/contact-form-7/":   ("forms",  "Contact Form 7"),
    "/wp-content/plugins/elementor/":        ("forms",  "Elementor Forms"),
    "/wp-content/plugins/woocommerce/":      ("payments", "WooCommerce"),
    # Hosting
    "wpstaq":           ("infra", "WPStaq"),
    "wpengine":         ("infra", "WP Engine"),
    "kinsta":           ("infra", "Kinsta"),
    "siteground":       ("infra", "SiteGround"),
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
    """Extract social media presence from HTML. Only detects actionable links/embeds."""
    html_lower = html.lower()

    # Instagram: must be a linked profile, not just a mention
    has_instagram = bool(re.search(
        r'href=["\'][^"\']*instagram\.com/[^"\']{2,}',
        html_lower
    ))

    # WhatsApp: must be an actual click-to-chat link or widget embed
    # Matches: wa.me/..., api.whatsapp.com/send, whatsapp://send, widget embeds
    has_whatsapp = bool(re.search(
        r'(wa\.me/|api\.whatsapp\.com/send|whatsapp://send|'
        r'href=["\'][^"\']*whatsapp\.com/[^"\']*[?&]phone=)',
        html_lower
    ))

    return {
        "instagram": "yes" if has_instagram else "no",
        "whatsapp":  "yes" if has_whatsapp else "no",
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


def init_google_sheets(sheet_key_or_url: str, service_account_file: str = 'service_account.json', worksheet_name: str = None):
    """
    Initialize Google Sheets connection using service account credentials.
    Pass the appropriate sheet key/URL for each scraper (e.g. clinics vs ecom).

    Args:
        sheet_key_or_url: Google Sheet key (from URL) or full URL
        service_account_file: Path to service account JSON file
        worksheet_name: Optional tab name (e.g. 'main_clinics'). If None, uses first tab.

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
                service_account_email = service_account_data.get('client_email', 'not_detected')
                print(f"🔑 Service account: {service_account_email}")
        except Exception:
            service_account_email = "not_detected"

        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            service_account_file, scope
        )
        client = gspread.authorize(credentials)
        sheet = client.open_by_key(sheet_key)
        worksheet = sheet.worksheet(worksheet_name) if worksheet_name else sheet.sheet1

        print(f"✅ Connected to Google Sheet: {sheet.title}" + (f" (tab: {worksheet_name})" if worksheet_name else ""))
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
