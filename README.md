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

## Resources

- [Google Sheet](https://docs.google.com/spreadsheets/d/1y9zzp1J1Fn60UKYN0RkTsSQcHcMb1mi2cD4NH8OfAF4/edit?gid=0#gid=0)

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
alex: 
fix location count
