import math
import requests
import json
import time
import os
import csv
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()


class QuotaExceeded(Exception):
    pass


QUOTA_STATUSES = {"RESOURCE_EXHAUSTED"}
QUOTA_REASONS = {"BILLING_DISABLED", "QUOTA_EXCEEDED", "RATE_LIMIT_EXCEEDED"}


def is_quota_error(error_body):
    """Returns True only for billing/quota exhaustion, not IP or key errors."""
    if error_body.get("status") in QUOTA_STATUSES:
        return True
    for detail in error_body.get("details", []):
        if detail.get("reason") in QUOTA_REASONS:
            return True
    return False


def is_ip_restriction(error_body):
    for detail in error_body.get("details", []):
        r = detail.get("reason", "")
        if "IP" in r or "API_KEY_HTTP_REFERRER_BLOCKED" in r:
            return True
    return "IP address restriction" in error_body.get("message", "")


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
API_KEY = os.environ.get("GOOGLE_PLACES_KEY")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

PLACE_IDS_FILE    = DATA_DIR / "place_ids.txt"
PROGRESS_FILE     = DATA_DIR / "search_progress.json"
DETAILS_DONE_FILE = DATA_DIR / "details_done.txt"
OUTPUT_CSV        = DATA_DIR / "clinics_australia.csv"

TEXT_SEARCH_URL   = "https://places.googleapis.com/v1/places:searchText"
PLACE_DETAILS_URL = "https://places.googleapis.com/v1/places/{}"

# ── Test mode ────────────────────────────────────────────────────────────────
# Set MAX_ITERATIONS to None for a full production run.
# In test mode, TEST_CITY_CENTERS is used so results are guaranteed.
MAX_ITERATIONS = None   # ← set to None for a full run

RUN_PHASE_1 = False   # set to False to skip Phase 1 and go straight to Phase 2
RUN_PHASE_2 = True   # set to False to run Phase 1 only

TEST_CITY_CENTERS = [
    (-33.8688, 151.2093, 5000),  # Sydney
    (-37.8136, 144.9631, 5000),  # Melbourne
    (-27.4698, 153.0251, 5000),  # Brisbane
    (-31.9505, 115.8605, 5000),  # Perth
    (-34.9285, 138.6007, 5000),  # Adelaide
    (-42.8821, 147.3272, 5000),  # Hobart
    (-12.4634, 130.8456, 5000),  # Darwin
    (-35.2809, 149.1300, 5000),  # Canberra
]
# ─────────────────────────────────────────────────────────────────────────────
# https://developers.google.com/maps/documentation/javascript/place-types
PLACE_TYPES = [
    "chiropractor",
    # "dental_clinic",
    "dentist",
    "doctor", # (Captures solo GPs and specialists)
    # "drugstore",
    # "general_hospital",
    # "hospital",
    # "massage",
    # "massage_spa",
    "medical_center", # (Captures large GP super-clinics)
    "medical_clinic", # (Captures most multidisciplinary and allied health)
    # "medical_lab",
    # "pharmacy",
    "physiotherapist",
    # "sauna",
    # "skin_care_clinic",
    # "spa",
    # "tanning_studio",
    # "wellness_center",
    # "yoga_studio",
]




# ── Phase 2 field mask ───────────────────────────────────────────────────────
# Cost tiers triggered:
#   Enterprise tier  ($17/1k): rating, userRatingCount, regularOpeningHours,
#                               websiteUri, nationalPhoneNumber
#   Pro tier         ($5/1k):  displayName, primaryType, businessStatus,
#                               googleMapsUri, location
#   Essentials tier  ($2/1k):  id, formattedAddress, addressComponents
#
# paymentOptions and reservable are intentionally EXCLUDED — they push cost
# up to the Enterprise + Atmosphere tier ($25/1k), saving ~$400 on 50k clinics.
# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 billing: the single most expensive field sets the price for the ENTIRE request.
# Billing tier: Enterprise ($17/1k)
#
# Since Enterprise is the ceiling, all Pro and Essentials fields are FREE to add.
# Excluded only: Enterprise + Atmosphere fields ($25/1k) — reservable, paymentOptions, reviews, etc.
#
# Essentials ($2/1k) — free given Enterprise billing:
#   id, formattedAddress, addressComponents, shortFormattedAddress,
#   plusCode, types, viewport
#
# Pro ($5/1k) — free given Enterprise billing:
#   displayName, primaryType, primaryTypeDisplayName, businessStatus,
#   pureServiceAreaBusiness, googleMapsUri, googleMapsLinks,
#   location, timeZone, utcOffsetMinutes, accessibilityOptions
#
# Enterprise ($17/1k) ← billing ceiling:
#   rating, userRatingCount, regularOpeningHours,
#   nationalPhoneNumber, internationalPhoneNumber, websiteUri,
#   priceLevel
FIELD_MASK = ",".join([
    # ── Essentials ───────────────────────────────────────────────
    "id",
    "formattedAddress",
    "shortFormattedAddress",
    "addressComponents",
    "plusCode",             # Google Plus Code (short address alternative)
    "types",               # all place types assigned (e.g. doctor, establishment)
    # ── Pro ──────────────────────────────────────────────────────
    "displayName",
    "primaryType",
    "primaryTypeDisplayName",   # human-readable type (e.g. "Medical Clinic")
    "businessStatus",
    "pureServiceAreaBusiness",  # True = no physical address (mobile business)
    "googleMapsUri",
    "location",
    "timeZone",            # IANA timezone (e.g. "Australia/Sydney")
    "utcOffsetMinutes",    # UTC offset in minutes
    "accessibilityOptions", # wheelchair access fields
    # ── Enterprise ── sets billing tier ($17/1k) ─────────────────
    "rating",
    "userRatingCount",
    "regularOpeningHours",          # normal weekly hours (consolidated into opening_hours)
    "nationalPhoneNumber",
    "internationalPhoneNumber",
    "websiteUri",
    "priceLevel",          # PRICE_LEVEL_FREE / INEXPENSIVE / MODERATE / EXPENSIVE
])

CSV_FIELDNAMES = [
    # Identity
    "name", "place_id", "cid",
    # Type
    "type", "type_label", "all_types",
    # Contact
    "phone", "phone_international", "website",
    # Address
    "street", "city", "state", "postal_code", "country",
    "formatted_address", "short_address", "plus_code",
    # Location
    "latitude", "longitude", "timezone", "utc_offset_minutes",
    # Status & quality
    "business_status", "rating", "reviews", "price_level",
    "has_physical_address",
    # Accessibility
    "wheelchair_entrance", "wheelchair_parking",
    "wheelchair_restroom", "wheelchair_seating",
    # Hours
    "opening_hours",
]


# ─────────────────────────────────────────────
# PHASE 1 HELPERS
# ─────────────────────────────────────────────

def generate_au_grid():
    """
    Two-tier hex-grid covering Australia efficiently:

    - 5km radius  around major population centres (captures dense urban clinics)
    - 45km radius everywhere else (captures rural/regional clinics, skips empty desert)

    Why two tiers?
    - A flat 5km grid over all of Australia = ~258k tiles = ~$5,700 in API costs.
    - Most of that covers uninhabited desert with 0 clinics.
    - A flat 45km grid misses most clinics in cities (max 60 results per tile).
    - Two-tier gives full coverage at a fraction of the cost (~$300-400 estimated).

    Population centres use a 120km "influence radius" — anything within 120km of
    a major city gets the dense 5km grid; everything else gets 45km tiles.
    """
    # Major Australian population centres with their influence radius (km).
    # Any tile whose centre falls within this radius gets the dense 5km grid.
    # Order = scrape priority (Sydney first, smaller regionals last).
    # Regional cities added to prevent 60-result truncation on 100km sparse tiles.
    POPULATION_CENTRES = [
        # (lat, lng, influence_radius_km)

        # ── Major metros ─────────────────────────────────────────────────────
        (-33.8688, 151.2093, 80),   # Sydney
        (-37.8136, 144.9631, 80),   # Melbourne
        (-27.4698, 153.0251, 60),   # Brisbane
        (-31.9505, 115.8605, 60),   # Perth
        (-34.9285, 138.6007, 50),   # Adelaide
        (-42.8821, 147.3272, 30),   # Hobart
        (-12.4634, 130.8456, 30),   # Darwin
        (-35.2809, 149.1300, 30),   # Canberra

        # ── Large regional cities (pop > 100k — high truncation risk) ────────
        (-32.7330, 151.5540, 25),   # Newcastle
        (-34.4278, 150.8931, 25),   # Wollongong
        (-27.9690, 153.3980, 25),   # Gold Coast
        (-26.6500, 153.0667, 25),   # Sunshine Coast
        (-19.2590, 146.8169, 25),   # Townsville
        (-16.9186, 145.7781, 25),   # Cairns
        (-38.1499, 144.3617, 25),   # Geelong

        # ── Mid-size regional cities (pop 50k–100k) ──────────────────────────
        (-37.5622, 143.8503, 20),   # Ballarat
        (-36.7724, 144.7793, 20),   # Bendigo
        (-27.5598, 151.9507, 20),   # Toowoomba
        (-35.1082, 147.3598, 20),   # Wagga Wagga
        (-36.3760, 145.4081, 20),   # Shepparton
        (-34.1808, 150.6043, 20),   # Penrith (western Sydney overflow)
        (-33.3427, 149.1006, 20),   # Orange/Bathurst
        (-28.6474, 153.6020, 20),   # Lismore/Northern Rivers
        (-23.7000, 133.8807, 20),   # Alice Springs
        (-31.9522, 141.4655, 15),   # Broken Hill
        (-29.6813, 153.0699, 15),   # Coffs Harbour
        (-30.3328, 153.1151, 15),   # Port Macquarie
        (-33.7490, 150.6866, 15),   # Parramatta (western Sydney)
        (-32.9283, 151.7817, 15),   # Lake Macquarie
        (-34.7487, 149.7238, 15),   # Goulburn
        (-36.1218, 146.8955, 15),   # Albury-Wodonga
        (-37.8290, 146.1165, 15),   # Traralgon/Latrobe Valley
        (-38.3850, 146.3167, 15),   # Sale
        (-26.4122, 153.0413, 15),   # Noosa

        # ── Remote/outback hubs ──────────────────────────────────────────────
        (-17.9644, 122.2312, 15),   # Broome
        (-20.7256, 116.8455, 15),   # Karratha
        (-23.3500, 119.7300, 15),   # Newman
        (-33.6500, 138.6300, 15),   # Port Augusta
        (-32.4936, 137.7611, 15),   # Port Pirie
        (-34.9200, 138.5989, 15),   # Whyalla
        (-25.0278, 130.9722, 10),   # Uluru region
    ]

    DENSE_RADIUS_M  = 5_000    # 5km for urban areas
    SPARSE_RADIUS_M = 100_000  # 100km for rural/regional

    lat_min, lat_max = -43.7, -10.5
    lng_min, lng_max = 113.3, 153.6

    def in_population_zone(lat, lng):
        for clat, clng, radius_km in POPULATION_CENTRES:
            # Fast Euclidean approximation (good enough for zone check)
            dlat = (lat - clat) * 111.0
            dlng = (lng - clng) * 111.0 * math.cos(math.radians(clat))
            if math.sqrt(dlat**2 + dlng**2) <= radius_km:
                return True
        return False

    # ── Build dense (5km) tiles for urban zones ──────────────────────────────
    dense_centers = set()
    lat_step = (DENSE_RADIUS_M * 1.5) / 111_000
    row = 0
    lat = lat_min
    while lat <= lat_max:
        lng_offset = (row % 2) * (lat_step / 2)
        lng_step = lat_step / math.cos(math.radians(lat))
        lng = lng_min + lng_offset
        while lng <= lng_max:
            if in_population_zone(lat, lng):
                dense_centers.add((round(lat, 4), round(lng, 4), DENSE_RADIUS_M))
            lng += lng_step
        lat += lat_step
        row += 1

    # ── Build sparse (45km) tiles for rural zones ────────────────────────────
    sparse_centers = set()
    lat_step = (SPARSE_RADIUS_M * 1.5) / 111_000
    row = 0
    lat = lat_min
    while lat <= lat_max:
        lng_offset = (row % 2) * (lat_step / 2)
        lng_step = lat_step / math.cos(math.radians(lat))
        lng = lng_min + lng_offset
        while lng <= lng_max:
            if not in_population_zone(lat, lng):
                sparse_centers.add((round(lat, 4), round(lng, 4), SPARSE_RADIUS_M))
            lng += lng_step
        lat += lat_step
        row += 1

    # ── Priority sort ────────────────────────────────────────────────────────
    # Sort dense tiles so the most important cities are searched first.
    # If the $200 limit is hit mid-run, you'll have Sydney/Melbourne/Brisbane
    # fully scraped before touching smaller cities or rural areas.
    #
    # Priority is defined by order in POPULATION_CENTRES — put your most
    # important cities first in that list to control the scrape order.
    #
    # Method: for each dense tile, find its nearest population centre and use
    # that centre's index as the sort key. Ties broken by distance to that centre
    # (closer tiles = searched first, so city CBDs come before outer suburbs).

    def tile_priority(tile):
        lat, lng, _ = tile
        best_idx, best_dist = 0, float("inf")
        for idx, (clat, clng, _) in enumerate(POPULATION_CENTRES):
            dlat = (lat - clat) * 111.0
            dlng = (lng - clng) * 111.0 * math.cos(math.radians(clat))
            dist = math.sqrt(dlat**2 + dlng**2)
            if dist < best_dist:
                best_dist = dist
                best_idx  = idx
        return (best_idx, best_dist)   # (city rank, distance from CBD)

    sorted_dense  = sorted(dense_centers,  key=tile_priority)
    sorted_sparse = sorted(sparse_centers, key=tile_priority)  # rural: big cities' regions first too

    all_centers = sorted_dense + sorted_sparse
    dense_count  = len(dense_centers)
    sparse_count = len(sparse_centers)
    total        = len(all_centers)

    print(f"  Grid breakdown:")
    print(f"    Dense  (5km):  {dense_count:>6,} tiles  (urban — searched first, CBD inward)")
    print(f"    Sparse (45km): {sparse_count:>6,} tiles  (rural/regional — searched after cities)")
    print(f"    Total:         {total:>6,} tiles")
    print(f"  Search order: Sydney → Melbourne → Brisbane → Perth → Adelaide → ...")

    return all_centers


def text_search_ids(lat, lng, place_type, radius_m, debug=False):
    """
    Uses Text Search (New) to get up to 60 place IDs per location (3 pages × 20).

    Why Text Search instead of Nearby Search?
    - Nearby Search (New) bills at "Nearby Search Pro" ($32/1k) even for IDs only.
    - Text Search (New) has an "IDs Only" tier ($2/1k) — 16x cheaper.
    - Text Search supports pagination (nextPageToken); Nearby Search does not.
    - Net result: 3x more results per tile at 1/16th the cost.
    """
    query = place_type.replace("_", " ")   # e.g. "medical_clinic" → "medical clinic"

    payload = {
        "textQuery": query,
        "includedType": place_type,
        # Text Search (New) uses locationBias for circles.
        # locationRestriction only accepts rectangles in Text Search — unlike Nearby Search.
        "locationBias": {
            "circle": {
                "center": {"latitude": lat, "longitude": lng},
                "radius": float(radius_m),
            }
        },
    }
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "places.id,nextPageToken",  # IDs Only SKU
    }

    found_ids = []

    while True:
        resp = requests.post(TEXT_SEARCH_URL, json=payload, headers=headers, timeout=15)
        data = resp.json()

        if debug:
            print(f"    HTTP {resp.status_code} | page ids so far: {len(found_ids)}")
            print(f"    Response preview: {json.dumps(data)[:300]}")

        if "error" in data:
            err = data["error"]
            if is_quota_error(err):
                raise QuotaExceeded(err.get("message", ""))
            if is_ip_restriction(err):
                print(f"\n🔒  IP RESTRICTION: Your API key blocks this IP.")
                print(f"    Fix: GCP Console → Credentials → your key → Application restrictions → None")
                exit(1)
            print(f"  [ERROR] {lat},{lng} {place_type}: {err.get('message', '')}")
            break

        for place in data.get("places", []):
            found_ids.append(place["id"])

        next_token = data.get("nextPageToken")
        if not next_token:
            break

        # Pass token for next page
        payload["pageToken"] = next_token
        time.sleep(0.1)   # brief pause between pages

    if debug:
        print(f"    → {len(found_ids)} total place IDs returned")

    return found_ids


def collect_all_place_ids(centers, place_types, max_iterations=None):
    """
    Phase 1: iterate over grid centres and collect unique place IDs via Text Search.
    Saves progress every 100 searches — safely resumable after crashes.

    In test mode, replaces the grid with TEST_CITY_CENTERS so you get real data.
    """
    MAX_WORKERS = 4

    if max_iterations is not None:
        centers = TEST_CITY_CENTERS
        print(f"  [TEST MODE] Using {len(centers)} city centres instead of full grid")

    seen = set()
    if PLACE_IDS_FILE.exists():
        lines = PLACE_IDS_FILE.read_text().splitlines()
        seen = set(line for line in lines if line)
        if seen:
            print(f"  Resuming Phase 1: {len(seen)} place IDs already on disk")

    done_searches = set()
    if PROGRESS_FILE.exists():
        done_searches = set(json.loads(PROGRESS_FILE.read_text()))

    lock = threading.Lock()

    # Build list of pending work
    pending = []
    for lat, lng, radius_m in centers:
        for ptype in place_types:
            key = f"{lat},{lng},{radius_m},{ptype}"
            if key not in done_searches:
                pending.append((lat, lng, radius_m, ptype, key))

    if max_iterations is not None:
        pending = pending[:max_iterations]
        print(f"  [TEST MODE] Limiting to {len(pending)} searches")

    print(f"  {len(pending)} searches remaining ({len(done_searches)} already done)")

    completed_count = 0

    def do_search(args):
        lat, lng, radius_m, ptype, key = args
        ids = text_search_ids(lat, lng, ptype, radius_m)
        return ids, lat, lng, radius_m, ptype, key

    with open(PLACE_IDS_FILE, "a", buffering=1) as out:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(do_search, args): args for args in pending}
            for future in as_completed(futures):
                try:
                    ids, lat, lng, radius_m, ptype, key = future.result()
                except QuotaExceeded:
                    executor.shutdown(wait=False, cancel_futures=True)
                    print(f"\n💳  QUOTA EXCEEDED — progress saved.")
                    with lock:
                        PROGRESS_FILE.write_text(json.dumps(list(done_searches)))
                    return seen
                except Exception as e:
                    print(f"  [EXCEPTION] {e}")
                    continue

                with lock:
                    new_ids = [pid for pid in ids if pid not in seen]
                    seen.update(new_ids)
                    for pid in new_ids:
                        out.write(pid + "\n")
                    done_searches.add(key)
                    completed_count += 1
                    print(f"  [{completed_count:>5}] ({lat:>8}, {lng:>9}) r={radius_m//1000:>2}km"
                          f" {ptype:<22} → {len(ids):>2} results, {len(new_ids):>2} new"
                          f" | total unique: {len(seen)}")
                    if completed_count % 10 == 0:
                        PROGRESS_FILE.write_text(json.dumps(list(done_searches)))

    PROGRESS_FILE.write_text(json.dumps(list(done_searches)))
    print(f"\nPhase 1 done. Total unique place IDs: {len(seen)}")
    return seen


# ─────────────────────────────────────────────
# PHASE 2 HELPERS
# ─────────────────────────────────────────────

def parse_address_components(components):
    """Extract street, city, state, postal code, country from address components."""
    result = {"street": "", "city": "", "state": "", "postal_code": "", "country": ""}
    street_number, route = "", ""

    for comp in components:
        types      = comp.get("types", [])
        long_name  = comp.get("longText", "")
        short_name = comp.get("shortText", long_name)

        if "street_number" in types:
            street_number = long_name
        elif "route" in types:
            route = long_name
        elif "locality" in types:
            result["city"] = long_name
        elif "administrative_area_level_1" in types:
            result["state"] = short_name   # "NSW" not "New South Wales"
        elif "postal_code" in types:
            result["postal_code"] = long_name
        elif "country" in types:
            result["country"] = long_name

    result["street"] = f"{street_number} {route}".strip()
    return result


def parse_opening_hours(opening_hours):
    """Collapse weekday descriptions into one pipe-separated string (CSV-safe)."""
    if not opening_hours:
        return ""
    return " | ".join(opening_hours.get("weekdayDescriptions", []))


def extract_cid(google_maps_uri):
    """Pull the CID from a Google Maps URI if present."""
    if google_maps_uri and "cid=" in google_maps_uri:
        return google_maps_uri.split("cid=")[-1].split("&")[0]
    return ""


def fetch_place_details(place_id):
    """GET full place details for one place ID."""
    url = PLACE_DETAILS_URL.format(place_id)
    headers = {
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": FIELD_MASK,
    }
    resp = requests.get(url, headers=headers, timeout=15)
    data = resp.json()
    if "error" in data:
        err = data["error"]
        if is_quota_error(err):
            raise QuotaExceeded(err.get("message", ""))
        if is_ip_restriction(err):
            print(f"\n🔒  IP RESTRICTION: Fix in GCP Console → Credentials → Application restrictions → None")
            exit(1)
    return data


def place_to_row(data):
    """Flatten a Place Details response into a CSV-ready dict."""
    addr    = parse_address_components(data.get("addressComponents", []))
    loc     = data.get("location", {})
    maps_uri = data.get("googleMapsUri", "")

    # pureServiceAreaBusiness: True = mobile/no storefront, False = real physical address
    pure_service = data.get("pureServiceAreaBusiness")
    has_physical = "" if pure_service is None else ("false" if pure_service else "true")

    accessibility = data.get("accessibilityOptions", {})
    plus_code = data.get("plusCode", {}).get("globalCode", "")
    all_types = ",".join(data.get("types", []))
    tz = data.get("timeZone", {})
    timezone_id = tz if isinstance(tz, str) else tz.get("id", "")

    return {
        # Identity
        "name":                     data.get("displayName", {}).get("text", ""),
        "place_id":                 data.get("id", ""),
        "cid":                      extract_cid(maps_uri),
        # Type
        "type":                     data.get("primaryType", ""),
        "type_label":               data.get("primaryTypeDisplayName", {}).get("text", ""),
        "all_types":                all_types,
        # Contact
        "phone":                    data.get("nationalPhoneNumber", ""),
        "phone_international":      data.get("internationalPhoneNumber", ""),
        "website":                  data.get("websiteUri", ""),
        # Address
        "street":                   addr["street"],
        "city":                     addr["city"],
        "state":                    addr["state"],
        "postal_code":              addr["postal_code"],
        "country":                  addr["country"],
        "formatted_address":        data.get("formattedAddress", ""),
        "short_address":            data.get("shortFormattedAddress", ""),
        "plus_code":                plus_code,
        # Location
        "latitude":                 loc.get("latitude", ""),
        "longitude":                loc.get("longitude", ""),
        "timezone":                 timezone_id,
        "utc_offset_minutes":       data.get("utcOffsetMinutes", ""),
        # Status & quality
        "business_status":          data.get("businessStatus", ""),
        "rating":                   data.get("rating", ""),
        "reviews":                  data.get("userRatingCount", ""),
        "price_level":              data.get("priceLevel", ""),
        "has_physical_address":     has_physical,
        # Accessibility
        "wheelchair_entrance":      accessibility.get("wheelchairAccessibleEntrance", ""),
        "wheelchair_parking":       accessibility.get("wheelchairAccessibleParking", ""),
        "wheelchair_restroom":      accessibility.get("wheelchairAccessibleRestroom", ""),
        "wheelchair_seating":       accessibility.get("wheelchairAccessibleSeating", ""),
        # Hours
        "opening_hours":            parse_opening_hours(data.get("regularOpeningHours")),
    }


def fetch_all_details(max_iterations=None):
    """
    Phase 2: fetch full details for every unique place ID from Phase 1.
    Appends to CSV and tracks completed IDs so runs are safely resumable.
    """
    if not PLACE_IDS_FILE.exists():
        print("  place_ids.txt not found — run Phase 1 first.")
        return

    all_ids = [line for line in PLACE_IDS_FILE.read_text().splitlines() if line]
    if not all_ids:
        print("  place_ids.txt is empty — Phase 1 found no results.")
        return

    done_ids = set()
    if DETAILS_DONE_FILE.exists():
        done_ids = set(DETAILS_DONE_FILE.read_text().splitlines())

    remaining = [pid for pid in all_ids if pid not in done_ids]
    if max_iterations is not None:
        remaining = remaining[:max_iterations]
        print(f"  [TEST MODE] Limiting to {len(remaining)} fetches")
    print(f"  Total: {len(all_ids)} | Done: {len(done_ids)} | Remaining: {len(remaining)}")

    write_header = not OUTPUT_CSV.exists()

    MAX_WORKERS_P2 = 2   # Phase 2 can go higher, each request is independent

    def fetch_one(place_id):
        return place_id, fetch_place_details(place_id)

    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as csvfile, \
         open(DETAILS_DONE_FILE, "a", buffering=1) as donef:

        writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDNAMES)
        if write_header:
            writer.writeheader()

        lock = threading.Lock()
        completed = [0]   # list so inner scope can mutate it

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_P2) as executor:
            futures = {executor.submit(fetch_one, pid): pid for pid in remaining}
            for future in as_completed(futures):
                try:
                    place_id, data = future.result()
                except QuotaExceeded:
                    executor.shutdown(wait=False, cancel_futures=True)
                    print(f"\n💳  QUOTA EXCEEDED — progress saved.")
                    with lock:
                        csvfile.flush()
                    return
                except Exception as e:
                    print(f"  [EXCEPTION] {e}")
                    continue

                with lock:
                    if "error" not in data:
                        writer.writerow(place_to_row(data))
                    else:
                        print(f"  [ERROR] {place_id}: {data['error']['message']}")
                    donef.write(place_id + "\n")
                    completed[0] += 1
                    if completed[0] % 100 == 0:
                        csvfile.flush()
                        print(f"  Fetched: {completed[0]}/{len(remaining)}")

    size_kb = OUTPUT_CSV.stat().st_size // 1024
    print(f"\nPhase 2 done. Output: {OUTPUT_CSV} ({size_kb} KB)")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # ── Sanity check API key ─────────────────────────────────────
    if not API_KEY:
        print("❌  ERROR: GOOGLE_PLACES_KEY is not set.")
        print("    Make sure your .env file contains:  GOOGLE_PLACES_KEY=your_key")
        exit(1)
    print(f"✅  API key loaded: {API_KEY[:8]}...{API_KEY[-4:]}")

    # ── Smoke test ───────────────────────────────────────────────
    print("\n🔍  Smoke test: 'dentist' near Sydney CBD via Text Search...")
    try:
        test_ids = text_search_ids(-33.8688, 151.2093, "dentist", radius_m=5000, debug=True)
    except QuotaExceeded as e:
        print(f"💳  QUOTA EXCEEDED during smoke test: {e}")
        exit(1)
    if test_ids:
        print(f"✅  Smoke test passed — got {len(test_ids)} place IDs\n")
    else:
        print("❌  Smoke test returned 0 results. Check API key & billing.\n")
        exit(1)

    centers = generate_au_grid()
    total_searches = len(centers) * len(PLACE_TYPES)
    print(f"\nTotal searches (full run): {total_searches:,}")
    print(f"Estimated Phase 1 cost (IDs Only @ $2/1k): ~${total_searches * 0.002:.0f}\n")

    if MAX_ITERATIONS is not None:
        print(f"⚠️  TEST MODE — max_iterations={MAX_ITERATIONS} per phase")
        print(f"   Phase 1 uses {len(TEST_CITY_CENTERS)} city centres (not the full grid)\n")

    if RUN_PHASE_1:
        print("=== PHASE 1: Collecting Place IDs ===")
        collect_all_place_ids(centers, PLACE_TYPES, max_iterations=MAX_ITERATIONS)
    else:
        print("⏭️  Skipping Phase 1 (RUN_PHASE_1=False)")

    if RUN_PHASE_2:
        print("\n=== PHASE 2: Fetching Full Details ===")
        fetch_all_details(max_iterations=MAX_ITERATIONS)
    else:
        print("⏭️  Skipping Phase 2 (RUN_PHASE_2=False)")