# Crawl Atlas

A data intelligence engine that extracts structured business signals from websites.

**Repo:** [github.com/alexkocev/crawl_atlas](https://github.com/alexkocev/crawl_atlas) Uses Playwright, rotating proxies (Bright Data), and smart extraction logic for both **one-shot datasets** and **recurring migration signals**.

## Target Markets

- **Clinics / Healthcare** — Website URL, clinic name, email provider, CRM/scheduling systems, practitioner count, Instagram/WhatsApp presence. (Initially Australia, then global.)
- **Ecommerce** — Platform detection, email & SMS marketing tools, subscription apps, reviews & loyalty tools, tracking pixels (Meta, TikTok), social links, contact info. (Initially US Shopify beauty and related verticals.)

## Product Modes

- **One-Shot Extracts** — Instant datasets (CSV/JSON) for lead lists, prospecting, agency segmentation.
- **Recurring Signals** — Weekly snapshots to detect migrations (e.g. Mailchimp → Klaviyo), new installs, traffic changes.

## Codebase Structure

- **`core.py`** — All functions shared between the different scraping engines (Playwright helpers, extraction utilities, proxy handling, etc.).
- **`main_ecom.py`**, **`main_clinics.py`**, etc. — Each file contains the entry point / running functions and all other logic specific to that scraping engine (e.g. clinic-specific extraction, ecommerce-specific detection).

## Clinics Pipeline

All clinic data lives in one [Google Sheet](https://docs.google.com/spreadsheets/d/1y9zzp1J1Fn60UKYN0RkTsSQcHcMb1mi2cD4NH8OfAF4/edit?gid=477879008#gid=477879008) with three tabs:

| Tab | Source | What it is |
|-----|--------|------------|
| **outscraper_clinics** | Outscraper | Raw Google Maps data (name, website, phone, address, reviews, booking links, etc.) |
| **main_clinics** | `main_clinics.py` | Crawled website data (email provider, CRM, booking stack, practitioner count, telehealth, etc.) |
| **all_clinics** | `mergerWithOutscraper.js` | Clean, merged output — combines both sources, filters OPERATIONAL only |

**Flow:**

1. **outscraper_clinics** — All Outscraper data is already in this sheet.
2. **Collect URLs** — Extract website URLs from `outscraper_clinics`.
3. **main_clinics** — Run `main_clinics.py` to crawl those URLs and populate `main_clinics`.
4. **all_clinics** — Run `mergerWithOutscraper.js` (Apps Script: Extensions → Apps Script, then **Merge Clinics** menu) to clean and merge → `all_clinics`.

## Resources

### Data for clinics (main sheet with outscraper_clinics, main_clinics, all_clinics): 
- [Google Sheet](https://docs.google.com/spreadsheets/d/1y9zzp1J1Fn60UKYN0RkTsSQcHcMb1mi2cD4NH8OfAF4/edit?gid=477879008#gid=477879008)

### Leads for clinics: 
- [Google Sheet](https://docs.google.com/spreadsheets/d/1RE4eTQjhkdrt0Mpx1NrJbU7dOGKPzIgqjeCMW5h2X40/edit?gid=405407655#gid=405407655)

### Leads for ecom: 
- [Google Sheet](https://docs.google.com/spreadsheets/d/1TNmF1jfk2fwNATon0NlrX6dqtRgeHGOzYBzPQhCjzFk/edit?usp=drive_link)





## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

For Google Sheets integration, create a service account, enable the Sheets API, and share your sheet with the service account email. See `yoluko-frontdesk-3d208271a3c0.json` for the configured key.

## Usage

```bash
python main.py
```


---
# ROADMAP

### 1- Scrap all fields, increase completion, only on 400 clinics


### 2- Find leads and first clients

### 3- Scrap entire AU market
Clinic URLs come from the **outscraper_clinics** tab (all Outscraper data is in the sheet). Outscraper search terms:

### Search terms: 
Medical Centre
General Practitioner
Aboriginal Health Service
Dentist
Orthodontist
Dental Clinic
Physiotherapist
Chiropractor
Podiatrist
Psychologist
Osteopath
Occupational Therapist
Speech Pathologist
Cardiologist
Dermatologist
Psychiatrist
Radiology
Paediatrician
Allied Health Services
Dietitian


### fields to keep

website
name
booking_appointment_link
subtypes
type
phone
street
city
state
postal_code
country
latitude
longitude
reviews
rating
business_status
working_hours_csv_compatible
booking_appointment_link
verified
about
place_id
cid





# alex
handles multilocation: check if other locations with same URLs from outscraper. 2 fields built via mergerWithOutscraper but what about practitioner_count??
verified should be true or false, no 1 or 0



improve: practitioner_count 
and telehealth
and biiling type

replace yes / no with TRUE and FALSE

clean url to remove / at teh end





6) r= 5km chiropractor           → 60 results,  0 new | total unique: 11551
  [   47] (-38.2946,   145.326) r= 5km medical_clinic         → 60 results,  0 new | total unique: 11551
  [   48] (-38.2946,   145.326) r= 5km doctor                 → 60 results,  0 new | total unique: 11551
  [   49] (-38.2946,   145.326) r= 5km medical_center         → 60 results,  0 new | total unique: 11551
  [   50] (-38.2946,   145.326) r= 5km physiotherapist        → 60 results,  1 new | total unique: 11552
  [   51] (-38