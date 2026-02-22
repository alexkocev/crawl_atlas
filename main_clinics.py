"""
Medical Clinic Lead Qualification Scraper
Scrapes clinic websites to extract qualification data including email providers,
CRM systems, practitioner counts, and service offerings.
"""

import asyncio
import json
import random
import re
from typing import Dict
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError

from core import (
    get_email_provider,
    detect_email_provider_from_addresses,
    detect_from_cookies,
    detect_from_meta_generator,
    scan_robots_txt,
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


# -----------------------------------------------------------------------------
# CLINIC-SPECIFIC: Booking, multi-location, category, team count, tech priority
# -----------------------------------------------------------------------------

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

BOOKING_LINK_PATTERNS = [
    "book", "booking", "appointment", "schedule",
    "reserve", "book now", "book online", "make a booking",
    "request appointment", "book an appointment",
]


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
        # ----------------------------------------------------------------
        iframes = await page.query_selector_all("iframe[src]")
        for iframe in iframes:
            src = await iframe.get_attribute("src") or ""
            if not src:
                continue
            src_lower = src.lower()
            iframe_domain = urlparse(src).netloc.lower().replace("www.", "")

            for vendor_domain in EXTERNAL_BOOKING_DOMAINS:
                if vendor_domain in src_lower:
                    vendor_name = _vendor_name_from_domain(vendor_domain)
                    result.update({
                        "booking_type": "external_vendor",
                        "booking_vendor": vendor_name,
                        "booking_url": src,
                    })
                    return result

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

            for vendor_domain in EXTERNAL_BOOKING_DOMAINS:
                if vendor_domain in candidate_domain:
                    vendor_name = _vendor_name_from_domain(vendor_domain)
                    result.update({
                        "booking_type": "external_vendor",
                        "booking_vendor": vendor_name,
                        "booking_url": candidate_url,
                    })
                    return result

            if clinic_root in candidate_domain and candidate_domain != clinic_domain:
                result.update({
                    "booking_type": "embedded",
                    "booking_vendor": "",
                    "booking_url": candidate_url,
                })
                return result

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

        # Step 4: Fallback — scan raw HTML for vendor domain fingerprints
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

    if result["booking_type"] == "not_detected" and not result["booking_vendor"]:
        result["booking_vendor"] = "not_detected"

    return result


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
        return result

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

    if result["multi_location"] == "yes" and result["location_count_estimate"] <= 1:
        result["location_count_estimate"] = 2

    return result


async def count_team_members(page: Page) -> int:
    """Count team members by looking for team page and common patterns."""
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

    if second_score > 0 and top_score / second_score < 1.2:
        top_cat = "Mixed / Multidisciplinary"

    confidence = round((top_score / total) * 100, 1)

    return {
        "primary_category": top_cat,
        "confidence_score": confidence,
        "all_scores": scores,
    }


# Stack priority: higher index = preferred when multiple tools detected in same category.
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
    """
    rules = [
        ("crm", "LeadConnector", "GoHighLevel", False),
        ("crm", "MediRecords (Clinical CRM)", "MediRecords", False),
        ("cms", "Wix", "WordPress", True),
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


def _apply_stack_priority(category: str, values: list) -> list:
    """Sort multi-value list using STACK_PRIORITY. Higher index = preferred."""
    if not values or category not in STACK_PRIORITY or category == "pixels":
        return values

    priority_list = STACK_PRIORITY[category]

    def get_weight(val):
        try:
            return -priority_list.index(val)
        except ValueError:
            return 0

    return sorted(values, key=get_weight)


def apply_stack_priority_to_result(result: dict) -> dict:
    """
    Apply stack priority SORTING to each tech category in result.
    Keeps all detected tools — most important tool appears first.
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
                sorted_hosts = _apply_stack_priority("infra", others)
                result[category] = f"{cloudflare}, " + ", ".join(sorted_hosts)
            else:
                result[category] = ", ".join(_apply_stack_priority("infra", parts))
            continue

        sorted_parts = _apply_stack_priority(category, parts)
        result[category] = ", ".join(sorted_parts)

    return result


# Tech stack signatures - fingerprints for detection
# Category order: pms_ehr → booking → cms → crm → payments → telehealth →
# forms → pixels → live_chat → reviews → infra (12 cats, 11 output cols excl. booking)
#
# Trace (vibenaturalhealth.com.au): With _collect_page_sources now including relative
# link hrefs, all 4 previously missed stacks are detected:
# 1. Contact Form 7: contact-form-7/includes in script/link URLs
# 2. Elementor Forms: send-app-elementor-form-tracker in plugin script path
# 3. Trustindex: loader-feed.js (CDN) + trustindex-feed-instagram-widget (relative CSS)
# 4. Send App: send-app-cf7-form-tracker, send-app-elementor-form-tracker, plugins/send-app
TECH_SIGNATURES = {
    # ─────────────────────────────────────────────────────────────────────
    # 1. PRACTICE MANAGEMENT SYSTEMS (PMS / EHR)
    # ─────────────────────────────────────────────────────────────────────
    "pms_ehr": {
        # ── Allied Health Cloud SaaS ──
        "Cliniko":               ["cliniko.com", "app.cliniko.com", "secure.cliniko.com", "booking.cliniko.com", "telehealth.cliniko.com"],
        "Nookal":                ["nookal.com", "connect.nookal.com", "portal.nookal.com"],
        "Jane App":              ["janeapp.com", "jane.app", "clinics.janeapp.com"],
        "Halaxy":                ["halaxy.com"],
        "Power Diary":           ["powerdiary.com"],
        "Splose":                ["splose.com"],
        "Coreplus":              ["coreplus.com.au"],
        "Practice Better":       ["practicebetter.io", "practicebetter.com"],
        "SimplePractice":        ["simplepractice.com", "telehealth.simplepractice.com"],
        "Carepatron":            ["carepatron.com"],
        "WriteUpp":              ["writeupp.com"],
        "PracSuite":             ["pracsuite.com"],
        "TM2 / TM3":             ["tm2online.com", "tm3online.com", "tmonline.net"],
        "Smartsoft Front Desk":  ["frontdesk.com.au", "smartsoft.com.au", "booking.frontdesk"],
        "Xestro":                ["xestro.com"],
        "PPMP":                  ["ppmp.com.au"],

        # ── GP / Specialist / Multi-site ──
        "Best Practice":         ["bpsoftware.com.au", "bp-software.com.au", "bestpracticesoftware", "medicalonline", "bp premier", "best practice software", "bp software"],
        "AutoMed":               ["automed.com.au", "ams connect", "amsconnect", "automed systems"],
        "MedicalDirector":       ["medicaldirector.com", "helix.medicaldirector", "pracsoft.com", "portal.medicaldirector"],
        "Zedmed":                ["zedmed.com.au"],
        "Genie Solutions":       ["genie.com.au", "geniesolutions", "gentu.com.au"],
        "Clinic to Cloud":       ["clinictocloud.com", "clinic-to-cloud"],
        "ProCare":               ["procare.com.au", "procarehealth.com.au"],
        "Titanium":              ["titaniumhealthcare.com.au", "titanium-software.com.au"],
        "Bluechip":              ["bluechip.com.au", "bluechipmedical"],
        "Shexie":                ["shexie.com.au"],
        "Medilink":              ["medilink.com.au"],
        "PrimaryClinic":         ["primaryclinic.com.au"],
        "Communicare":           ["communicare.com.au", "telstrahealth.com/communicare"],
        "MasterCare":            ["mastercare.com.au", "globalhealth.com.au/mastercare"],
        "Audit4":                ["audit4.com", "software4specialists.com"],
        "Profile (Intrahealth)": ["intrahealth.com", "profile-pms"],

        # ── Dental ──
        "Dental4Windows":        ["centaurportal.com", "centaur-software.com", "centaurportal.com/d4w",
                                 "dental4windows", "d4w/org-", "dental4windows.com", "centaursoftware.com.au"],
        "Exact (SOE)":           ["software-of-excellence.com", "exact-dental.com", "soeortho.com"],
        "Praktika":              ["praktika.com.au"],
        "Oasis Dental":         ["oasisdental.com"],
        "Dentrix":               ["dentrix.com", "henryschein.com/dentrix"],
        "Core Practice":         ["corepractice.com.au"],

        # ── Enterprise / Hospital ──
        "Epic":                  ["epic.com", "mychart.com", "app.epic.com"],
        "Cerner":                ["cerner.com", "healthelife.com.au", "oracle.com/health"],
        "Allscripts":            ["allscripts.com", "veradigm.com"],
        "Meditech":              ["meditech.com"],
        "TrakCare":              ["trakcare.com", "intersystems.com/trakcare"],
        "MediRecords":           ["medirecords.com", "medirecords"],
        "Athenahealth":          ["athenahealth.com", "athenaone.com"],
        "DrChrono":              ["drchrono.com", "onpatient.com"],
        "AdvancedMD":            ["advancedmd.com"],
        "Kareo":                 ["kareo.com", "tebra.com"],
        "Practice Fusion":       ["practicefusion.com"],
        "NextGen":               ["nextgen.com", "nextgenhealth.com"],
        "eClinicalWorks":        ["eclinicalworks.com", "healow.com"],
        "Greenway Health":       ["greenwayhealth.com"],

        # ── Enterprise / Specialist portals ──
        "Salesforce Health Cloud": ["salesforce.com", "force.com", "health-cloud", "salesforceiq", "lightning.force.com"],
        "Genea":                   ["genea.com.au", "genea kinnect", "geneakinnect", "powered by genea"],

        # ── Niche / Wellness ──
        "Mindbody":              ["mindbodyonline.com", "mindbody.io"],
        "Fresha":                ["fresha.com", "shedul.com"],
        "Timely":                ["gettimely.com"],
        "Cosmetri":              ["cosmetri.com"],
        "Zandamed":              ["zandamed.com"],
    },

    # ─────────────────────────────────────────────────────────────────────
    # 2. BOOKING SYSTEMS
    # ─────────────────────────────────────────────────────────────────────
    "booking": {
        "HotDoc":                ["cdn.hotdoc.com.au", "hotdoc-widgets.min.js", "hotdoc-widget",
                                 "hotdoc.com.au/medical-centres", "book.hotdoc.com.au"],
        "HealthEngine":          ["healthengine.com.au", "booking.healthengine.com.au"],
        "AutoMed":               ["automed.com.au", "ams connect", "amsconnect.com.au"],
        "Cliniko Booking":       ["booking.cliniko.com", "secure.cliniko.com"],
        "Nookal Booking":        ["connect.nookal.com", "portal.nookal.com"],
        "Jane App Booking":      ["clinics.janeapp.com"],
        "Halaxy Booking":        ["halaxy.com/book"],
        "Power Diary Booking":   ["powerdiary.com/book"],
        "Front Desk Booking":    ["booking.frontdesk.com.au"],
        "HubSpot Meetings":      ["meetings.hubspot.com", "meetings.hs-sites"],
        "Calendly":              ["calendly.com", "assets.calendly"],
        "Acuity":                ["acuityscheduling.com"],
        "Setmore":               ["setmore.com"],
        "Doctolib":              ["doctolib.fr", "doctolib.de", "doctolib.com"],
        "Zocdoc":                ["zocdoc.com"],
        "Docplanner":            ["docplanner.com"],
        "Doctoralia":            ["doctoralia.com"],
        "Fresha":                ["fresha.com"],
        "Mindbody":              ["mindbodyonline.com"],
        "Timely":                ["gettimely.com"],
        "FormAssembly":          ["tfaforms.net", "tfaforms.com", "formassembly.com", "request.*consultation", "consultation.*request"],
        "Genea Kinnect":         ["genea-kinnect", "geneakinnect", "kinnect.genea"],
        "D4W eAppointments":     ["centaurportal.com/d4w", "centaurportal.com", "d4w/org-",
                                 "practiceid=", "centaur.*appointment", "d4w.*appointment"],
        "MediRecords Booking":  ["medirecords"],
    },

    # ─────────────────────────────────────────────────────────────────────
    # 3. CMS / WEBSITE BUILDER
    # ─────────────────────────────────────────────────────────────────────
    "cms": {
        "WordPress":   ["wp-content", "wp-includes", "wp-json", "/wp-json/wp/v2/", "wordpress"],
        "Drupal":      ["sites/default/files", "drupalSettings", "core/misc/drupal.js", "drupal.org"],
        "Wix":         ["wix.com", "wix-thunderbolt", "static.wixstatic.com", "static.parastorage.com", "wix-code"],
        "Squarespace": ["squarespace.com", "static1.squarespace", "squarespace.com/universal/scripts"],
        "Webflow":     ["webflow.com", "assets-global.website-files", "cdn.prod.website-files.com", "data-wf-domain"],
        "Framer":      ["framer.com", "framerusercontent.com"],
        "Joomla":      ["joomla.org", "joomla"],
        "Ghost":       ["ghost.org", "ghost"],
        "Weebly":      ["weebly.com"],
        "Sitecore":    ["sitecore.net", "sitecore.com", "-/media/", "/-/jssmedia/"],
        "Umbraco":     ["umbraco.com", "umbraco", "/umbraco/"],
        "HubSpot CMS": ["hs-sites.com", "hubspotpagebuilder.com", "hs-scripts.com/cms"],
        "Shopify":     ["cdn.shopify.com", "shopify.theme"],
    },

    # ─────────────────────────────────────────────────────────────────────
    # 4. CRM, EMAIL MARKETING & PATIENT ENGAGEMENT
    # ─────────────────────────────────────────────────────────────────────
    "crm": {
        # ── Full CRM platforms ──
        "HubSpot":           ["hubspot.com", "hs-scripts.com", "hsforms.com",
                              "js.hs-scripts.com", "hs-analytics.net",
                              "data-hubspot"],
        "Salesforce":        ["salesforce.com", "pardot.com", "force.com",
                              "salesforceliveagent", "exacttarget.com",
                              "marketingcloud.com", "salesforce-communities",
                              "tfaforms.net"],
        "Zoho CRM":          ["zoho.com/crm", "zohopublic", "salesiq.zoho.com",
                              "zohocrm", "campaigns.zoho"],
        "Pipedrive":         ["pipedrive.com", "pipedriveassets.com"],
        "Keap":              ["infusionsoft.com", "keap.com"],
        "PatientPop":        ["patientpop.com"],
        "Podium":            ["podium.com", "podium-widget"],
        "Birdeye":           ["birdeye.com", "birdeye.io", "birdeyecdn"],
        # ── Email marketing / automation ──
        "ActiveCampaign":    ["activecampaign.com", "trackcmp.net",
                              "activehosted.com", "acsbapp.com"],
        "Mailchimp":         ["chimpstatic.com", "mailchimp.com", "mc.js",
                              "list-manage.com", "mcjs", "data-mailchimp"],
        "Klaviyo":           ["klaviyo.com", "_learnq", "static.klaviyo",
                              "klaviyo_forms"],
        "Campaign Monitor":  ["createsend.com", "campaignmonitor.com"],
        "Brevo":             ["sendinblue.com", "brevo.com"],
        "ConvertKit":        ["convertkit.com"],
        "Constant Contact":  ["constantcontact.com"],
        "GoHighLevel":       ["leadconnectorhq.com", "msgsndr.com", "link.msgsndr.com",
                             "widgets.leadconnectorhq.com", "highlevel.com",
                             "gohighlevel.com", "highlevel-chat"],
        "LeadConnector":     ["leadconnectorhq.com", "link.msgsndr.com", "msgsndr.com"],
        # plugins/send-app, send-app-cf7-form-tracker, send-app-elementor-form-tracker: catch WP plugin assets (CF7/Elementor form trackers); relative paths now in link_hrefs
        "Send App":          ["plugins/send-app", "send-app-cf7-form-tracker", "send-app-elementor-form-tracker"],
        "MediRecords (Clinical CRM)": ["medirecords"],
    },

    # ─────────────────────────────────────────────────────────────────────
    # 5. PAYMENTS & BILLING
    # ─────────────────────────────────────────────────────────────────────
    "payments": {
        "Stripe":   ["js.stripe.com", "stripe.com/v3", "api.stripe.com"],
        "Square":   ["squareup.com", "sq-payment-form", "square.com"],
        "PayPal":   ["paypal.com", "paypalobjects.com"],
        "Xero":     ["xero.com", "xero-widget"],
        "MYOB":     ["myob.com"],
        "Tyro":     ["tyro.com"],
        "Medipass": ["medipass.com.au", "medipass-connect"],
        "Hicaps":   ["hicaps.com.au"],
        "Windcave": ["windcave.com", "paymentexpress.com"],
    },

    # ─────────────────────────────────────────────────────────────────────
    # 7. TELEHEALTH / VIRTUAL CARE
    # ─────────────────────────────────────────────────────────────────────
    "telehealth": {
        "AutoMed":                ["ams connect", "amsconnect", "automed telehealth"],
        "Cliniko Telehealth":     ["telehealth.cliniko.com"],
        "Healthdirect Video":     ["healthdirect.gov.au/video-call", "vcc.healthdirect.org.au"],
        "Zoom":                   ["zoom.us", "zoom.com"],
        "Coviu":                  ["coviu.com"],
        "Whereby":                ["whereby.com", "appear.in"],
        "Doxy.me":                ["doxy.me"],
        "SimplePractice Telehealth": ["telehealth.simplepractice.com"],
        "MedAdvisor":             ["medadvisor.com.au"],
        "Hello Home Doctor":       ["hello home doctor", "134 100"],
    },

    # ─────────────────────────────────────────────────────────────────────
    # 8. FORMS & INTAKE
    # ─────────────────────────────────────────────────────────────────────
    "forms": {
        # contact-form-7/includes: catches /wp-content/plugins/contact-form-7/includes/js/index.js, /includes/css/styles.css (was missed when only absolute URLs were collected)
        "Contact Form 7":  ["plugins/contact-form-7", "contact-form-7", "wpcf7", "cf7", "contact-form-7/includes"],
        # send-app-elementor-form-tracker: catches send-app-elementor-form-tracker.js inside wp-content/plugins/send-app/ (Elementor Forms integration)
        # Guard: avoid bare "elementor" — too broad, matches Wix's feature-elementory-support, send-app tracker URL substrings
        "Elementor Forms": ["elementor-pro", "elementor/assets", "plugins/elementor", "elementor-frontend", "/elementor/modules/forms", "send-app-elementor-form-tracker"],
        "Gravity Forms":   ["gravityforms", "gform_", "gravity-forms"],
        "WPForms":         ["wpforms", "wpforms-form"],
        "Ninja Forms":     ["ninja-forms", "nf-form"],
        "Snapforms":      ["snapforms.com.au", "snapforms"],
        "AutoMed Forms":  ["ams form", "automed form", "new patient ams"],
        "Typeform":  ["typeform.com", "embed.typeform.com"],
        "JotForm":   ["jotform.com", "form.jotform.com"],
        "Halaxy Forms": ["halaxy.com/form", "halaxy.com/eform"],
        "Google Forms": ["docs.google.com/forms", "forms.gle"],
        "Paperform":  ["paperform.co"],
        "Cognito Forms": ["cognitoforms.com"],
        "FormAssembly": ["tfaforms.net", "tfaforms.com", "formassembly.com", "fa-form", "wFORMS"],
        "GoHighLevel Forms": ["link.msgsndr.com/js/form_embed", "msgsndr.com/js/form",
                             "leadconnectorhq.com/form", "highlevel.*form"],
    },

    # ─────────────────────────────────────────────────────────────────────
    # 9. AD PIXELS & ANALYTICS
    # ─────────────────────────────────────────────────────────────────────
    "pixels": {
        "Meta Pixel":         ["fbevents.js", "connect.facebook.net/en_us/fbevents", "fbq("],
        "Google Ads":         ["googleadservices.com", "google_conversion"],
        "Google Analytics 4": ["gtag.js", "google-analytics.com", "googletagmanager.com/gtag", "gtag/js?id=G-"],
        "Google Tag Manager": ["googletagmanager.com", "gtm.js"],
        "LinkedIn Insight":   ["snap.licdn.com", "linkedin.com/insight"],
        "TikTok Pixel":       ["analytics.tiktok.com", "ttq.load"],
        "Pinterest":          ["pintrk(", "ct.pinterest.com"],
        "Snapchat":           ["sc-static.net", "snaptr("],
        "Hotjar":             ["hotjar.com", "hjsetting"],
        "Microsoft Clarity":  ["clarity.ms", "microsoft.com/clarity"],
        "Segment":            ["segment.com", "analytics.js"],
        "Mixpanel":           ["mixpanel.com"],
        "Heap":               ["heap.io"],
        "Call Dynamics":      ["calldynamics.com.au", "artemisData"],
    },

    # ─────────────────────────────────────────────────────────────────────
    # 10. LIVE CHAT & PATIENT MESSAGING
    # ─────────────────────────────────────────────────────────────────────
    "live_chat": {
        "Intercom":       ["intercom.com", "widget.intercom.io", "intercomsettings"],
        "Drift":          ["drift.com", "js.drift.com"],
        "Tawk.to":        ["tawk.to", "embed.tawk.to"],
        "Zendesk":        ["zendesk.com", "zopim.com", "zdassets.com"],
        "Crisp":          ["crisp.chat", "client.crisp.chat"],
        "LiveChat":       ["livechatinc.com", "__lc"],
        "Freshchat":      ["freshchat.com", "wchat.freshchat.com"],
        "HubSpot Chat":   ["hubspot-messages"],
        "WhatsApp Widget":["wa.me", "whatsapp.com/send", "api.whatsapp"],
        "Podium Chat":    ["podium.com", "podiumwidget"],
        "Birdeye Chat":   ["birdeye.com"],
        "Tidio":          ["tidio.com", "tidiochat"],
        "GoHighLevel Chat": ["widgets.leadconnectorhq.com/chat-widget",
                            "leadconnectorhq.com/chat", "msgsndr.com/chat"],
    },

    # ─────────────────────────────────────────────────────────────────────
    # 11. REVIEWS & REPUTATION
    # ─────────────────────────────────────────────────────────────────────
    "reviews": {
        "Google Reviews":        ["aggregaterating", "g.co/kgs", "ratingvalue", "ratingcount",
                                 "maps.googleapis.com", "google.com/maps/embed", "place_id",
                                 "goo.gl/maps", "g.page"],
        # loader-feed.js: CDN script (cdn.trustindex.io/loader-feed.js); trustindex-feed-instagram-widget: relative CSS /wp-content/uploads/trustindex-feed-instagram-widget.css (was missed before relative link hrefs fix)
        "Trustindex":            ["cdn.trustindex.io", "trustindex-feed", "loader-feed.js", "trustindex-feed-instagram-widget"],
        "Doctify":               ["doctify.com"],
        "HealthEngine Reviews":  ["healthengine.com.au"],
        "RateMDs":               ["ratemds.com"],
        "Trustpilot":            ["trustpilot.com", "widget.trustpilot"],
        "Podium Reviews":        ["podium.com", "podium-widget"],
        "Birdeye Reviews":       ["birdeye.com"],
        "Feefo":                 ["feefo.com"],
        "Whitecoat":             ["whitecoat.com.au"],
    },

    # ─────────────────────────────────────────────────────────────────────
    # 11. INFRASTRUCTURE / CDN / HOSTING
    # Signals here = the CLINIC'S OWN hosting stack, not third-party vendors.
    # Primary detection comes from HTTP headers via detect_from_headers().
    # HTML-based patterns below are conservative — only match highly specific
    # fingerprints that indicate direct use, not third-party asset loading.
    # ─────────────────────────────────────────────────────────────────────
    "infra": {
        # Cloudflare: cookie/header fingerprints (set by detect_from_headers too)
        "Cloudflare":   ["__cf_bm", "cf-ray", "cloudflare-nginx"],
        # Vercel: only vercel.app subdomains = actually hosted on Vercel
        "Vercel":       ["vercel.app"],
        # Netlify: only netlify.app subdomains or netlify identity scripts
        "Netlify":      ["netlify.app", "netlify-identity-widget"],
        # AWS: only match the site's OWN S3/CloudFront, not third-party scripts.
        # Use a long-form match to reduce false positives.
        "AWS":          ["s3.amazonaws.com", "s3-ap-southeast-2.amazonaws.com",
                         "s3-us-east-1.amazonaws.com"],
        # Azure: only actual Azure-hosted sites
        "Azure":        ["azurewebsites.net", "azureedge.net"],
        # Google Cloud: excluded — too many false positives (GCS/storage URLs from embeds, CDN)
        # Kinsta, WP Engine, Flywheel — common managed WordPress hosts in AU
        "Kinsta":       ["kinsta.cloud", "kinstacdn.com"],
        "WP Engine":    ["wpengine.com", "wpenginepowered.com"],
        "Flywheel":     ["flywheelstaging.com", "flywheelsites.com"],
        "Pantheon":     ["pantheonsite.io"],
        "HealthLink":   ["healthlink.net", "healthlink edi", "edi:", "healthlink secure"],
        "VentraIP":     ["ventraip.com.au", "synergywholesale.com", "cpanel", "ventra"],
        "cPanel":       ["cpanel", "whm.cpanel", "cpsess"],
        "Crazy Domains":["crazydomains.com.au"],
        "NetRegistry":  ["netregistry.com.au"],
        "GreenGeeks":   ["greengeeks.com"],
        "SiteGround":   ["siteground.com", "sgcpanel"],
    },
}

# Visible text signatures — phrases that appear in page copy (not scripts/URLs).
# Uses regex for flexible matching. Catches tools referenced in copy but not exposed via scripts/iframes.
VISIBLE_TEXT_SIGNATURES = {
    "pms_ehr": {
        "Best Practice":    ["best practice software", "bp premier", "proficiency in best practice"],
        "AutoMed":         ["ams connect", "automed systems", "ams connect app"],
        "MedicalDirector": ["medical director", "helix"],
        "Salesforce":       ["salesforce", "health cloud", "salesforce health cloud"],
        "Genea":            ["powered by genea", "genea kinnect", "genea world"],
        "Dental4Windows":  ["dental4windows", "d4w", "centaur portal", "centaur software"],
    },
    "booking": {
        "AutoMed":         ["book.*ams connect", "ams connect app", "appointments through.*ams"],
        "HotDoc":          ["hotdoc widget", "book online.*hotdoc", "hotdoc.*book online",
                           "cdn.hotdoc.com.au", "hotdoc.com.au", "book.*hotdoc", "hotdoc.*book", "hotdoc telehealth", "quick consult"],
        "HealthEngine":    ["healthengine.com.au", "book.*healthengine"],
        "FormAssembly":    ["request a consultation", "request.*appointment.*form", "tfaforms"],
        "Genea Kinnect":   ["genea kinnect", "kinnect app", "manage.*appointments.*kinnect"],
        "D4W eAppointments": ["centaurportal", "d4w.*book", "book.*d4w", "centaur.*book"],
    },
    "telehealth": {
        "AutoMed":         ["telehealth.*ams connect", "ams connect.*telehealth", "download.*ams connect.*video"],
        "HotDoc":          ["telehealth.*hotdoc", "hotdoc.*telehealth", "hotdoc.*video consult", "phone consult.*hotdoc", "video consult.*hotdoc"],
        "Coviu":           ["coviu", "video.*coviu"],
        "Zoom":            ["zoom.*telehealth", "telehealth.*zoom"],
        "Hello Home Doctor": ["hello home doctor", "134 100"],
    },
    "forms": {
        "Contact Form 7":  ["contact form 7", "wpcf7", "powered by contact form 7"],
        "Snapforms":       ["snapforms"],
        "AutoMed Forms":   ["new patient.*ams", "ams.*new patient", "registration.*ams connect"],
        "HotDoc Forms":    ["repeat prescription.*hotdoc", "specialist referral.*hotdoc", "hotdoc.*quick consult", "quick consult.*hotdoc"],
        "FormAssembly":    ["tfaforms", "formassembly", "request a consultation"],
        "GoHighLevel Forms": ["highlevel.*form", "leadconnector.*form"],
    },
    "crm": {
        "Salesforce":      ["salesforce", "formassembly", "tfaforms"],
        "GoHighLevel":     ["msgsndr", "leadconnector", "gohighlevel",
                           "appointment reminder.*highlevel", "newsletter.*highlevel"],
    },
    "live_chat": {
        "GoHighLevel Chat": ["chat.*highlevel", "leadconnector.*chat"],
    },
    "reviews": {
        "Google Reviews":  ["google reviews", "google maps", "write a review.*google", "review us on google"],
    },
    "infra": {
        "HealthLink":      ["healthlink edi", "edi:", "healthlink secure messaging"],
        "VentraIP":        ["ventraip", "synergy wholesale", "hosted by ventraip",
                            "parked.*ventraip"],
        "cPanel":          ["cpanel", "webmail", "cPanel Email"],
    },
}


def scan_visible_text_for_tech(page_text: str) -> dict:
    """
    Scan lowercased visible page text for tool name mentions.
    Catches tools that are referenced in copy but not exposed via scripts/iframes.
    Returns dict of category -> set of tool names.
    """
    found = {}
    text_lower = page_text.lower()
    for category, tools in VISIBLE_TEXT_SIGNATURES.items():
        for tool_name, phrases in tools.items():
            for phrase in phrases:
                if re.search(phrase, text_lower):
                    found.setdefault(category, set()).add(tool_name)
                    break
    return found


def apply_co_occurrence_rules(result: dict) -> dict:
    """
    Infer additional tools ONLY when they belong to the exact same vendor ecosystem.
    Never guess based on market share or integrations.
    Appends ' (inferred)' so end-users know it wasn't a direct script detection.
    """
    SAFE_CO_OCCURRENCE_RULES = [
        # (trigger_category, trigger_value, infer_category, infer_value)
        ("booking", "HotDoc",                       "telehealth", "HotDoc (inferred)"),
        ("booking", "HotDoc",                       "forms",      "HotDoc Forms (inferred)"),
        ("booking", "AutoMed",                      "telehealth", "AutoMed (inferred)"),
        ("booking", "AutoMed",                      "forms",      "AutoMed Forms (inferred)"),
        ("cms",     "Wix",                          "crm",        "Wix (inferred)"),
        ("booking", "D4W eAppointments",            "pms_ehr",    "Dental4Windows (inferred)"),
        ("crm",     "GoHighLevel",                  "forms",      "GoHighLevel Forms (inferred)"),
        ("crm",     "GoHighLevel",                  "live_chat",  "GoHighLevel Chat (inferred)"),
        ("crm",     "MediRecords (Clinical CRM)",   "pms_ehr",    "MediRecords (inferred)"),
        ("booking", "MediRecords Booking",          "pms_ehr",    "MediRecords (inferred)"),
        ("pms_ehr", "MediRecords",                  "telehealth", "MediRecords Native Telehealth (inferred)"),
    ]

    for trigger_cat, trigger_val, infer_cat, infer_val in SAFE_CO_OCCURRENCE_RULES:
        current_trigger = result.get(trigger_cat, "not_detected")
        if trigger_cat == "booking":
            booking_vendor = result.get("booking_vendor", "") or ""
            current_trigger = current_trigger if current_trigger != "not_detected" else booking_vendor
        current_infer = result.get(infer_cat, "not_detected")
        if trigger_val.lower() in str(current_trigger).lower() and current_infer == "not_detected":
            result[infer_cat] = infer_val

    # REMOVED: HotDoc → Best Practice (risky market share guess)
    # REMOVED: WordPress + GP → HotDoc (risky market share guess)
    return result


# Tools that function as BOTH a booking system and a PMS.
# If one is detected and the other is missing, we can safely infer the missing one.
# Do NOT include pure booking aggregators here (HotDoc, HealthEngine, Calendly, Acuity).
BOOKING_IS_ALSO_PMS = {
    "AutoMed",
    "Cliniko",
    "Nookal",
    "Jane App",
    "Halaxy",
    "Power Diary",
    "Splose",
    "SimplePractice",
    "Practice Better",
    "Coreplus",
    "Mindbody",
    "Fresha",
    "Timely",
    "Front Desk",
    "Carepatron",
    "WriteUpp",
    "PracSuite",
    "D4W eAppointments",   # Centaur Portal = direct extension of Dental4Windows PMS
}

# Header-based infra detection (Server, X-Powered-By, CF-Ray, Via, X-Generator)
# Excluded: Fastly, Google Cloud — too many false positives (CDN/proxy headers from third-party assets)
HEADER_SIGNATURES = {
    "Cloudflare":    ["cloudflare", "cf-ray"],
    "AWS":           ["amazonaws", "cloudfront", "x-amz"],
    "Azure":         ["azure", "azurewebsites", "azureedge"],
    "nginx":         ["nginx"],
    "Apache":        ["apache"],
    "Microsoft-IIS": ["microsoft-iis", "iis"],
    "Vercel":        ["vercel"],
    "Netlify":       ["netlify"],
    "Kinsta":        ["kinsta"],
    "WP Engine":     ["wpengine"],
    "LiteSpeed":     ["litespeed"],            # common in AU shared hosting
    "VentraIP":      ["ventraip", "synergy", "cpanel"],
    "cPanel/Apache": ["apache", "cpanel"],
    "Parked Domain": ["parking", "parked-domain", "domain-for-sale"],
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
    all_sources = html_lower + " " + " ".join(
        _normalize_for_match(s) for s in script_srcs + iframe_srcs + link_hrefs
    )

    for category, tools in TECH_SIGNATURES.items():
        for tool_name, patterns in tools.items():
            for pattern in patterns:
                pat = _normalize_for_match(pattern)

                # GUARD: skip patterns too short to be reliable
                if len(pat) < 6:
                    continue

                # Prefer URL/script context over raw HTML body
                if pat in all_sources:
                    results[category].add(tool_name)
                    break

                # Only match in visible text if pattern is long enough to be specific
                if pat in text_lower and len(pat) >= 8:
                    results[category].add(tool_name)
                    break

    # Meta generator tag detection
    meta_hits = detect_from_meta_generator(html)
    for cat, tools in meta_hits.items():
        results.setdefault(cat, set()).update(tools)

    # WordPress plugin path detection (script/link srcs contain wp-content/plugins/)
    plugin_signatures = {
        "Contact Form 7": ["plugins/contact-form-7", "contact-form-7", "wpcf7"],
        # Guard: avoid bare "elementor" — too broad, matches Wix's feature-elementory-support on non-WP sites
        "Elementor Forms": ["elementor-pro", "elementor/assets", "plugins/elementor", "elementor-frontend", "/elementor/modules/forms", "send-app-elementor-form-tracker"],
        "Gravity Forms":  ["gravityforms"],
        "WPForms":        ["wpforms"],
        "Yoast SEO":      ["wordpress-seo"],
        "WooCommerce":    ["woocommerce"],
    }
    all_srcs_str = " ".join(s.lower() for s in script_srcs + link_hrefs)
    if "wp-content/plugins" in all_srcs_str:
        for plugin_name, slugs in plugin_signatures.items():
            if any(slug in all_srcs_str for slug in slugs):
                results.setdefault("forms", set()).add(plugin_name)

    # MediRecords booking on /book-now/ subpage (inferred): internal links to /book-now/
    # without MediRecords iframe on current page = widget likely isolated to that subpage
    iframe_srcs_lower = " ".join(s.lower() for s in iframe_srcs)
    link_hrefs_lower = " ".join(s.lower() for s in link_hrefs)
    if "/book-now/" in link_hrefs_lower and "medirecords" not in iframe_srcs_lower:
        results.setdefault("booking", set()).add("MediRecords booking likely on /book-now/ subpage (inferred)")

    return results


async def _collect_page_sources(page: Page) -> tuple:
    """
    Collect script srcs, iframe srcs, and link hrefs from page.
    Script srcs: URLs from script tags (used for TECH_SIGNATURES matching).
    Link hrefs: ALL hrefs (relative + absolute) — relative paths like
    /wp-content/uploads/trustindex-feed-instagram-widget.css were previously
    dropped and caused Trustindex/CF7/CSS-based signatures to be missed.
    """
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
            if href and href.strip():
                h = href.strip()
                # Include relative + absolute; exclude non-URL values
                if h != "#" and not h.startswith(("mailto:", "tel:", "javascript:")):
                    link_hrefs.append(h)
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
        cat: ", ".join(sorted(tools)) if tools else "not_detected"
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
    Returns flat dict: {"pms_ehr": "Cliniko", "booking": "HotDoc", "cms": "WordPress", ...}
    """
    from urllib.parse import urljoin

    accum = {cat: set() for cat in TECH_SIGNATURES}
    all_script_srcs = []

    # Fire robots.txt fetch in background before subpage visits
    robots_task = asyncio.create_task(scan_robots_txt(base_url))

    # 1. Scan homepage (current page)
    try:
        html = await page.content()
        page_text = await page.inner_text("body") if await page.query_selector("body") else ""
        script_srcs, iframe_srcs, link_hrefs = await _collect_page_sources(page)
        all_script_srcs.extend(script_srcs)
        page_results = _scan_page_for_tech(html, page_text, script_srcs, iframe_srcs, link_hrefs)
        header_infra = await detect_from_headers(initial_response) if initial_response else {}
        _merge_tech_results(accum, page_results, header_infra)
        text_hits = scan_visible_text_for_tech(page_text)
        _merge_tech_results(accum, text_hits)
    except Exception as e:
        print(f"  Error scanning homepage for tech: {e}")

    # 2. Visit /contact, /book, /about, /services (max 4 extra pages, 5 total)
    extra_urls = []
    parsed = urlparse(base_url)
    base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
    for path in [
        "/contact", "/contact-us",
        "/book", "/booking", "/book-online", "/appointments",
        "/about", "/about-us",
        "/services", "/our-services",
    ]:
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
        if pages_visited >= 5:
            break
        try:
            resp = await page.goto(url, timeout=10000, wait_until="domcontentloaded")
            await page.wait_for_timeout(1000)
            html = await page.content()
            page_text = await page.inner_text("body") if await page.query_selector("body") else ""
            script_srcs, iframe_srcs, link_hrefs = await _collect_page_sources(page)
            all_script_srcs.extend(script_srcs)
            page_results = _scan_page_for_tech(html, page_text, script_srcs, iframe_srcs, link_hrefs)
            header_infra = await detect_from_headers(resp) if resp else {}
            _merge_tech_results(accum, page_results, header_infra)
            text_hits = scan_visible_text_for_tech(page_text)
            _merge_tech_results(accum, text_hits)
            pages_visited += 1
        except Exception:
            continue

    # Merge robots.txt results before returning
    robots_hits = await robots_task
    _merge_tech_results(accum, robots_hits)

    WIX_FORMS_THIRD_PARTY = [
        "jotform", "typeform", "gravityforms", "contact-form-7", "wpforms",
        "formassembly", "snapforms", "elementor",
    ]
    if "Wix" in accum.get("cms", set()):
        # Extra guard: if WordPress is ALSO detected, it's a WP site —
        # Wix signals are false positives from third-party CDN assets.
        # Do not add Wix Forms (native) in that case.
        if "WordPress" not in accum.get("cms", set()):
            all_srcs_str = " ".join(s.lower() for s in all_script_srcs)
            if not any(fp in all_srcs_str for fp in WIX_FORMS_THIRD_PARTY):
                accum.setdefault("forms", set()).add("Wix Forms (native)")

    # Guard: Wix sites cannot use WordPress plugins — remove false-positive WP form tools
    # (Elementor Forms, CF7, etc. can be falsely matched by Wix's elementory-support, generic strings)
    WP_FORM_TOOLS = {"Elementor Forms", "Contact Form 7", "Gravity Forms", "WPForms"}
    if "Wix" in accum.get("cms", set()):
        accum["forms"] = accum.get("forms", set()) - WP_FORM_TOOLS

    return _tech_dict_to_flat(accum)


def _get_tech_cats_for_sheet() -> list:
    """Tech categories for sheet output, with cms immediately before booking_type."""
    cats = [c for c in TECH_SIGNATURES.keys() if c != "booking"]
    if "cms" in cats:
        cats = [c for c in cats if c != "cms"] + ["cms"]
    return cats


def _ensure_sheet_headers(worksheet, tech_cats: list) -> None:
    """
    Write snake_case header row. tech_cats excludes 'booking'.
    Column layout (32 cols):
      A  website_url
      B  clinic_name
      C  clinic_category
      D  multi_location
      E  location_count
      F  email_provider
      G  pms_ehr
      H  crm             ← merged crm + email_marketing
      I  payments
      J  telehealth
      K  forms
      L  pixels
      M  live_chat
      N  reviews
      O  infra
      P  cms
      Q  booking_type
      R  booking_vendor
      S  street
      T  city
      U  state
      V  postcode
      W  country
      X  phones
      Y  emails
      Z  practitioner_count
      AA home_visits
      AB instagram
      AD whatsapp
      AE scraping_date
      AF error_log
    """
    headers = [
        "website_url",
        "clinic_name",
        "clinic_category",
        "multi_location",
        "location_count",
        "email_provider",
        *tech_cats,
        "booking_type",
        "booking_vendor",
        "street",
        "city",
        "state",
        "postcode",
        "country",
        "phones",
        "emails",
        "practitioner_count",
        "home_visits",
        "instagram",
        "whatsapp",
        "scraping_date",
        "error_log",
    ]
    try:
        worksheet.update([headers], "A1:AE1")
    except Exception:
        pass


def _print_tech_summary(result: dict) -> None:
    """Print a clean tech stack summary per clinic."""
    tech_cats = [c for c in TECH_SIGNATURES.keys() if c != "booking"]
    lines = [
        "━" * 70,
        f"🏥 {result.get('clinic_name', 'N/A').upper()}",
        f"🏷️  Category:          {result.get('primary_category', 'not_detected')}",
        f"📍 Multi-Location:    {result.get('multi_location', 'no')} "
        f"(~{result.get('location_count_estimate', 1)} locations) "
        f"[{result.get('location_detection_method', 'none')}]",
    ]
    booking_type = result.get("booking_type", "not_detected")
    booking_vendor = result.get("booking_vendor", "")

    lines.append(f"📅 Booking Type:     {booking_type}")
    booking_vendor_display = booking_vendor or "not_detected"
    lines.append(f"📅 Booking Vendor:   {booking_vendor_display}")
    lines.append(f"📧 Email Provider: {result.get('email_provider', 'not_detected')}")
    for cat in tech_cats:
        val = result.get(cat, "not_detected")
        if val != "not_detected":
            lines.append(f"🔧 {cat:20} {val}")
    lines.extend([
        f"👥 Team Size:      ~{result.get('practitioner_count', 0)} members",
        f"🚗 Home Visits:    {result.get('home_visits', 'no')}",
        f"📱 Social:         Instagram: {result.get('instagram', 'no')} | WhatsApp: {result.get('whatsapp', 'no')}",
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


def infer_pms_booking(result: dict) -> dict:
    """
    Cross-infer PMS ↔ booking for dual-purpose tools (e.g. Cliniko, JaneApp, Halaxy).
    Adds ' (inferred)' so users know it was deduced, not directly detected.
    """
    pms = result.get("pms_ehr", "not_detected")
    booking_vendor = result.get("booking_vendor", "")
    booking_type = result.get("booking_type", "not_detected")

    if pms == "Not Detected": pms = "not_detected"
    if booking_type == "Not Detected": booking_type = "not_detected"

    pms_clean = pms if pms != "not_detected" else ""
    booking_clean = booking_vendor.strip()
    if booking_vendor == "not_detected":
        booking_clean = ""

    # Case 1: PMS detected (dual-purpose), booking vendor missing → infer booking
    if pms_clean and pms_clean in BOOKING_IS_ALSO_PMS and not booking_clean:
        result["booking_vendor"] = pms_clean if "(inferred)" in pms_clean else f"{pms_clean} (inferred)"
        result["booking_type"] = booking_type if booking_type != "not_detected" else "embedded"

    # Case 2: Booking detected (dual-purpose), PMS missing → infer PMS
    elif booking_clean and booking_clean in BOOKING_IS_ALSO_PMS and pms == "not_detected":
        result["pms_ehr"] = booking_clean if "(inferred)" in booking_clean else f"{booking_clean} (inferred)"

    return result


async def scrape_clinic(browser, url: str) -> Dict:
    """Scrape a single clinic website."""
    tech_categories = list(TECH_SIGNATURES.keys())
    result = {
        "url":                      url,
        "clinic_name":              "",
        "primary_category":         "not_detected",
        "confidence_score":         0.0,
        "email_provider":           "not_detected",
        "multi_location":           "no",
        "location_count_estimate":  1,
        "location_detection_method": "none",
        "booking_type":             "not_detected",
        "booking_vendor":           "",
        "booking_url":              "",
        "practitioner_count":       0,
        "home_visits":              "no",
        "instagram":                "no",
        "whatsapp":                 "no",
        "address":                  {},
        "phones":                   [],
        "emails":                   [],
        "error":                    None,
    }
    for cat in tech_categories:
        result[cat] = "not_detected"

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

        # Network request interception — catches dynamically loaded booking, pixels, chat
        network_hits = set()
        NETWORK_WATCH_DOMAINS = {
            # ── Booking / PMS ──
            "cdn.hotdoc.com.au":       ("booking", "HotDoc"),
            "hotdoc-widgets.min.js":   ("booking", "HotDoc"),
            "hotdoc.com.au":           ("booking", "HotDoc"),
            "book.hotdoc.com.au":      ("booking", "HotDoc"),
            "hotdoc.com.au/medical":   ("booking", "HotDoc"),
            "healthengine.com.au":     ("booking", "HealthEngine"),
            "cliniko.com":             ("pms_ehr", "Cliniko"),
            "halaxy.com":              ("pms_ehr", "Halaxy"),
            "powerdiary.com":          ("pms_ehr", "Power Diary"),
            "nookal.com":              ("pms_ehr", "Nookal"),
            "janeapp.com":             ("pms_ehr", "Jane App"),
            "splose.com":              ("pms_ehr", "Splose"),
            "calendly.com":            ("booking", "Calendly"),
            "acuityscheduling.com":    ("booking", "Acuity"),
            "automed.com.au":          ("booking", "AutoMed"),
            "mindbodyonline.com":      ("pms_ehr", "Mindbody"),
            "fresha.com":              ("pms_ehr", "Fresha"),
            "gettimely.com":           ("pms_ehr", "Timely"),
            "simplepractice.com":      ("pms_ehr", "SimplePractice"),
            "practicebetter.io":       ("pms_ehr", "Practice Better"),
            "frontdesk.com.au":        ("pms_ehr", "Front Desk"),
            # ── PMS / EHR portals ──
            "clientsecure.me":         ("pms_ehr", "SimplePractice"),
            "mychart.com":             ("pms_ehr", "Epic"),
            "athenahealth.com":        ("pms_ehr", "Athenahealth"),
            "drchrono.com":            ("pms_ehr", "DrChrono"),
            "eclinicalworks.com":      ("pms_ehr", "eClinicalWorks"),
            # ── CRM ──
            "weve.to":                 ("crm", "Weave"),
            # ── Forms / Intake ──
            "intakeq.com":             ("forms", "IntakeQ"),
            "tfaforms.net":            ("forms", "FormAssembly"),
            # ── Booking ──
            "zocdoc.com":              ("booking", "Zocdoc"),
            "doctolib.com":            ("booking", "Doctolib"),
            "setmore.com":             ("booking", "Setmore"),
            # ── Pixels ──
            "connect.facebook.net":    ("pixels", "Meta Pixel"),
            "analytics.tiktok.com":    ("pixels", "TikTok Pixel"),
            "googletagmanager.com":    ("pixels", "Google Tag Manager"),
            "googleadservices.com":    ("pixels", "Google Ads"),
            "snap.licdn.com":          ("pixels", "LinkedIn Insight"),
            "ct.pinterest.com":        ("pixels", "Pinterest"),
            "clarity.ms":              ("pixels", "Microsoft Clarity"),
            "hotjar.com":              ("pixels", "Hotjar"),
            # ── Telehealth ──
            "zoom.us":                 ("telehealth", "Zoom"),
            "coviu.com":               ("telehealth", "Coviu"),
            "vcc.healthdirect.org.au": ("telehealth", "Healthdirect Video"),
            "telehealth.cliniko.com":  ("telehealth", "Cliniko Telehealth"),
            # ── Live Chat ──
            "widget.intercom.io":      ("live_chat", "Intercom"),
            "js.drift.com":            ("live_chat", "Drift"),
            "embed.tawk.to":           ("live_chat", "Tawk.to"),
            "zdassets.com":            ("live_chat", "Zendesk"),
            "client.crisp.chat":       ("live_chat", "Crisp"),
            "wchat.freshchat.com":     ("live_chat", "Freshchat"),
            # ── CRM / Email Marketing ──
            "hs-scripts.com":          ("crm", "HubSpot"),
            "pardot.com":              ("crm", "Salesforce"),
            "salesiq.zoho.com":        ("crm", "Zoho CRM"),
            "pipedriveassets.com":     ("crm", "Pipedrive"),
            "podium.com":              ("crm", "Podium"),
            "birdeye.com":             ("reviews", "Birdeye"),
            "klaviyo.com":             ("crm", "Klaviyo"),
            "chimpstatic.com":         ("crm", "Mailchimp"),
            "trackcmp.net":            ("crm", "ActiveCampaign"),
            # ── Payments ──
            "js.stripe.com":           ("payments", "Stripe"),
            "squareup.com":            ("payments", "Square"),
            "medipass.com.au":         ("payments", "Medipass"),
            # ── Forms ──
            "typeform.com":            ("forms", "Typeform"),
            "jotform.com":             ("forms", "JotForm"),
            # ── Centaur Portal / D4W ──
            "centaurportal.com":       ("booking", "D4W eAppointments"),
            # ── GoHighLevel / LeadConnector ──
            "widgets.leadconnectorhq.com": ("crm", "GoHighLevel"),
            "link.msgsndr.com":        ("forms", "GoHighLevel Forms"),
            "msgsndr.com":             ("crm", "GoHighLevel"),
            "gohighlevel.com":          ("crm", "GoHighLevel"),
            "cdn.trustindex.io":       ("reviews", "Trustindex"),
            "trustindex.io":           ("reviews", "Trustindex"),
            "plugins/send-app":        ("crm", "Send App"),
            "medirecords":             ("crm", "MediRecords (Clinical CRM)"),
            # Mailgun / LeadConnector transactional email
            "mailgun.org":              ("crm", "Mailgun"),
            "mg.mail":                  ("crm", "Mailgun"),
        }

        def on_request(request):
            req_url = request.url.lower()
            for domain, (category, name) in NETWORK_WATCH_DOMAINS.items():
                if domain in req_url:
                    network_hits.add((category, name))

        page.on("request", on_request)

        cookie_hits = {}
        # Load homepage
        try:
            response = await page.goto(url, timeout=30000, wait_until='domcontentloaded')
            await page.wait_for_timeout(2000)  # Wait for dynamic content
            cookies = await context.cookies()
            cookie_hits = detect_from_cookies(cookies)
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

        booking_result = await detect_booking_type(page, url)
        result["booking_type"] = booking_result["booking_type"]
        result["booking_vendor"] = booking_result["booking_vendor"]
        result["booking_url"] = booking_result["booking_url"]

        # Wait for DNS lookup to complete
        result["email_provider"] = await provider_task

        # Detect tech stack (homepage + up to 2 subpages: /contact, /book)
        tech_stack = await detect_tech_stack(page, context, url, initial_response=response)
        for k, v in tech_stack.items():
            result[k] = v

        # Merge network hits from request interception
        for category, name in network_hits:
            current = result.get(category, "not_detected")
            if current == "not_detected":
                result[category] = name
            elif name not in current:
                result[category] = current + f", {name}"

        # Merge cookie-based detection
        for category, tools in cookie_hits.items():
            for name in tools:
                current = result.get(category, "not_detected")
                if current == "not_detected":
                    result[category] = name
                elif name not in current:
                    result[category] = current + f", {name}"

        # Check for home visits
        result["home_visits"] = "yes" if check_home_visits(html) else "no"

        # Also check services page for home visits
        if result["home_visits"] == "no":
            services_urls = [urljoin(url, '/services'), urljoin(url, '/service')]
            for services_url in services_urls:
                try:
                    await page.goto(services_url, timeout=10000, wait_until='domcontentloaded')
                    await page.wait_for_timeout(1000)
                    services_html = await page.content()
                    if check_home_visits(services_html):
                        result["home_visits"] = "yes"
                        break
                except:
                    continue
        
        # Extract social media (from homepage HTML)
        social = extract_social_media(html)
        result["instagram"] = social["instagram"]
        result["whatsapp"] = social["whatsapp"]

        # Navigate back to homepage (detect_tech_stack may have left us on /contact or /book)
        try:
            await page.goto(url, timeout=10000, wait_until='domcontentloaded')
            await page.wait_for_timeout(500)
        except Exception:
            pass

        # Get fresh HTML and page text for extraction
        html = await page.content()
        page_text = await page.inner_text('body')
        text_hits = scan_visible_text_for_tech(page_text)
        for category, tools in text_hits.items():
            for name in tools:
                current = result.get(category, "not_detected")
                if current == "not_detected":
                    result[category] = name
                elif name not in current:
                    result[category] = current + f", {name}"
        category_result = classify_clinic_category(html, page_text)
        result["primary_category"] = category_result["primary_category"]
        if result["primary_category"] == "Unknown":
            result["primary_category"] = "not_detected"
        result["confidence_score"] = category_result.get("confidence_score", 0.0)
        result['address'] = extract_full_address(html, page_text)
        result['phones'] = extract_all_phones(page_text, html)
        result['emails'] = extract_all_emails(page_text, html)

        # Secondary email provider detection from contact addresses (Gmail direct, etc.)
        direct_provider = detect_email_provider_from_addresses(result.get("emails", []))
        if direct_provider:
            existing = result.get("email_provider", "not_detected")
            if existing in ("not_detected", "privateemail", ""):
                result["email_provider"] = direct_provider
            elif direct_provider not in existing:
                result["email_provider"] = existing + f", {direct_provider}"
        # Consolidate: if Google Workspace MX + Gmail (direct) address, drop redundant Gmail (direct)
        if "Google Workspace" in result.get("email_provider", "") and "Gmail (direct)" in result.get("email_provider", ""):
            result["email_provider"] = result["email_provider"].replace(", Gmail (direct)", "").replace("Gmail (direct), ", "")

        # Multi-location detection (after phones/address extraction)
        page_postcodes = re.findall(r'\b\d{4}\b', page_text)
        addr_postcode = result.get('address', {}).get('postcode', '')
        all_postcodes = list(set(page_postcodes + ([addr_postcode] if addr_postcode else [])))

        location_result = await detect_multi_location(
            page,
            url,
            phones=result.get('phones', []),
            postcodes=all_postcodes,
        )
        result['multi_location'] = location_result['multi_location']
        result['location_count_estimate'] = location_result['location_count_estimate']
        result['location_detection_method'] = location_result['detection_method']

        # Count practitioners (navigates to team page if found)
        result['practitioner_count'] = await count_team_members(page)

        # Cross-infer PMS ↔ booking and stamp source fields
        result = infer_pms_booking(result)

    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)}'
        print(f"  ❌ Error: {e}")
    finally:
        await context.close()

    # Infer additional tools from co-occurrence patterns (runs after infer_pms_booking)
    result = apply_co_occurrence_rules(result)

    # Deduplicate known pairs (LeadConnector/GoHighLevel, Wix/WordPress, etc.)
    result = _deduplicate_tech(result)

    # Reduce multi-value categories to single preferred stack (runs last; nothing after)
    result = apply_stack_priority_to_result(result)

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
        
        # Column mapping (33 cols): A website_url ... AF scraping_date, AG error_log
        tech_cats_output = _get_tech_cats_for_sheet()
        _ensure_sheet_headers(worksheet, tech_cats_output)

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
                
                # Check Scraping Date (Col AD=30 = index 29 current, legacy: AF=32, AG=33, AE=30, AA=26, AC=28, Y=24, X=23, Q=16, I=8)
                scraping_date = (row_data[29].strip() if len(row_data) > 29 else "") or (
                    row_data[31].strip() if len(row_data) > 31 else "") or (
                    row_data[32].strip() if len(row_data) > 32 else "") or (
                    row_data[26].strip() if len(row_data) > 26 else "") or (
                    row_data[30].strip() if len(row_data) > 30 else "") or (
                    row_data[28].strip() if len(row_data) > 28 else "") or (
                    row_data[24].strip() if len(row_data) > 24 else "") or (
                    row_data[23].strip() if len(row_data) > 23 else "") or (
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
                    
                    # Prepare update values (B through AE)
                    tech_cats_output = _get_tech_cats_for_sheet()
                    tech_vals = [result.get(cat, "not_detected") for cat in tech_cats_output]
                    addr = result.get("address", {})
                    update_values = [
                        result.get("clinic_name", ""),
                        result.get("primary_category", "not_detected"),  # internal key, header is clinic_category
                        result.get("multi_location", "no"),
                        str(result.get("location_count_estimate", 1)),
                        result.get("email_provider", "not_detected"),
                        *tech_vals,
                        result.get("booking_type", "not_detected"),
                        result.get("booking_vendor", "") or "not_detected",
                        addr.get("street", ""),
                        addr.get("city", ""),
                        addr.get("state", ""),
                        addr.get("postcode", ""),
                        addr.get("country", ""),
                        ", ".join(result.get("phones", [])),
                        ", ".join(result.get("emails", [])),
                        str(result.get("practitioner_count", 0)),
                        result.get("home_visits", "no"),
                        result.get("instagram", "no"),
                        result.get("whatsapp", "no"),
                    ]
                    
                    # Get timestamp
                    timestamp = get_current_timestamp()
                    
                    if result.get('error'):
                        error_msg = result['error']
                        worksheet.update_cell(row_num, 30, timestamp)   # AD = scraping_date
                        worksheet.update_cell(row_num, 31, error_msg)   # AE = error_log
                        error_count += 1
                        print(f"❌ ERROR: {error_msg}")
                    else:
                        worksheet.update_cell(row_num, 31, '')  # AE = clear error
                        _print_tech_summary(result)

                    # Update data columns B through AD
                    cell_range = f"B{row_num}:AC{row_num}"
                    worksheet.update([update_values], cell_range)

                    # Scraping date and error log
                    worksheet.update_cell(row_num, 30, timestamp)   # AD = scraping_date
                    if not result.get('error'):
                        worksheet.update_cell(row_num, 31, "")      # AE = clear error
                    
                    processed_count += 1
                    print(f"✅ Row {row_num} updated successfully")
                    
                except Exception as e:
                    error_msg = f'Unexpected error: {str(e)}'
                    timestamp = get_current_timestamp()
                    worksheet.update_cell(row_num, 30, timestamp)   # AD = scraping_date
                    worksheet.update_cell(row_num, 31, error_msg)   # AE = error_log
                    
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

