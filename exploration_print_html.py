"""
Simple script to scrape a single URL and print all scraping data.
Usage: python print_html.py <URL>
Or run without arguments to paste URL interactively.
"""

import asyncio
import sys
from urllib.parse import urlparse

import dns.resolver
from playwright.async_api import async_playwright

# Import scraping functions from main.py
from main import (
    scrape_clinic,
    extract_email,
    extract_phone,
)


async def get_dns_records(domain: str) -> dict:
    """Get all DNS records for a domain."""
    domain = domain.replace("www.", "")
    records = {
        'MX': [],
        'A': [],
        'AAAA': [],
        'TXT': [],
        'CNAME': [],
        'NS': []
    }
    
    # Run DNS lookups in executor to avoid blocking event loop
    loop = asyncio.get_event_loop()
    
    # MX records
    try:
        answers = await loop.run_in_executor(
            None, lambda: dns.resolver.resolve(domain, 'MX')
        )
        records['MX'] = [f"{r.preference} {r.exchange}" for r in answers]
    except Exception as e:
        records['MX'] = [f"Error: {str(e)}"]
    
    # A records
    try:
        answers = await loop.run_in_executor(
            None, lambda: dns.resolver.resolve(domain, 'A')
        )
        records['A'] = [str(r) for r in answers]
    except Exception as e:
        records['A'] = [f"Error: {str(e)}"]
    
    # AAAA records (IPv6)
    try:
        answers = await loop.run_in_executor(
            None, lambda: dns.resolver.resolve(domain, 'AAAA')
        )
        records['AAAA'] = [str(r) for r in answers]
    except Exception as e:
        records['AAAA'] = [f"Error: {str(e)}"]
    
    # TXT records
    try:
        answers = await loop.run_in_executor(
            None, lambda: dns.resolver.resolve(domain, 'TXT')
        )
        records['TXT'] = [str(r).strip('"') for r in answers]
    except Exception as e:
        records['TXT'] = [f"Error: {str(e)}"]
    
    # CNAME records
    try:
        answers = await loop.run_in_executor(
            None, lambda: dns.resolver.resolve(domain, 'CNAME')
        )
        records['CNAME'] = [str(r) for r in answers]
    except Exception as e:
        records['CNAME'] = [f"Error: {str(e)}"]
    
    # NS records
    try:
        answers = await loop.run_in_executor(
            None, lambda: dns.resolver.resolve(domain, 'NS')
        )
        records['NS'] = [str(r) for r in answers]
    except Exception as e:
        records['NS'] = [f"Error: {str(e)}"]
    
    return records


async def print_scraping_data(url: str):
    """Scrape a URL and print all available data."""
    print(f"\n{'='*80}")
    print(f"🔍 Scraping: {url}")
    print(f"{'='*80}\n")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        try:
            # Scrape the clinic
            result = await scrape_clinic(browser, url)
            
            # Get HTML content for additional extraction
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()
            
            try:
                await page.goto(url, timeout=30000, wait_until='domcontentloaded')
                await page.wait_for_timeout(2000)
                html = await page.content()
                page_text = await page.inner_text('body')
            except Exception as e:
                html = ""
                page_text = ""
                print(f"⚠️  Warning: Could not fetch HTML content: {e}\n")
            finally:
                await context.close()
            
            # Extract additional data from HTML/text
            email = extract_email(page_text)
            phone = extract_phone(page_text)
            
            # Get DNS records
            domain = urlparse(url).netloc.replace('www.', '')
            dns_records = await get_dns_records(domain)
            
            # Print all scraping data
            print(f"\n{'━'*80}")
            print(f"📊 SCRAPING RESULTS")
            print(f"{'━'*80}\n")
            
            print(f"🌐 URL: {result.get('url', url)}")
            print(f"🏥 Clinic Name: {result.get('clinic_name', 'N/A')}")
            print(f"\n📧 Email Provider: {result.get('email_provider', 'Unknown')}")
            if email:
                print(f"   📮 Contact Email: {email}")
            
            print(f"\n⚙️  CRM System: {result.get('crm', 'Not Detected')}")
            
            print(f"\n👥 Practitioner Count: {result.get('practitioner_count', 0)}")
            
            print(f"\n🚗 Home Visits: {result.get('home_visits', 'NO')}")
            
            print(f"\n📱 Social Media:")
            print(f"   Instagram: {result.get('instagram', 'No')}")
            print(f"   WhatsApp: {result.get('whatsapp', 'No')}")
            
            if phone:
                print(f"\n📞 Phone: {phone}")
            
            # Print DNS records
            print(f"\n{'━'*80}")
            print(f"🌐 DNS RECORDS")
            print(f"{'━'*80}\n")
            print(f"Domain: {domain}\n")
            
            if dns_records['MX']:
                print(f"📧 MX Records (Mail Exchange):")
                for mx in dns_records['MX']:
                    print(f"   {mx}")
            else:
                print(f"📧 MX Records: None found")
            
            print()
            
            if dns_records['A']:
                print(f"🔢 A Records (IPv4):")
                for a in dns_records['A']:
                    print(f"   {a}")
            else:
                print(f"🔢 A Records: None found")
            
            print()
            
            if dns_records['AAAA']:
                print(f"🔢 AAAA Records (IPv6):")
                for aaaa in dns_records['AAAA']:
                    print(f"   {aaaa}")
            else:
                print(f"🔢 AAAA Records: None found")
            
            print()
            
            if dns_records['TXT']:
                print(f"📝 TXT Records:")
                for txt in dns_records['TXT']:
                    print(f"   {txt}")
            else:
                print(f"📝 TXT Records: None found")
            
            print()
            
            if dns_records['CNAME']:
                print(f"🔗 CNAME Records:")
                for cname in dns_records['CNAME']:
                    print(f"   {cname}")
            else:
                print(f"🔗 CNAME Records: None found")
            
            print()
            
            if dns_records['NS']:
                print(f"🌍 NS Records (Name Servers):")
                for ns in dns_records['NS']:
                    print(f"   {ns}")
            else:
                print(f"🌍 NS Records: None found")
            
            if result.get('error'):
                print(f"\n❌ Error: {result.get('error')}")
            
            # Print HTML content for analysis
            print(f"\n{'━'*80}")
            print(f"📄 HTML CONTENT (for analysis)")
            print(f"{'━'*80}\n")
            print(html)
            
            print(f"\n{'━'*80}")
            print(f"📝 PAGE TEXT (for analysis)")
            print(f"{'━'*80}\n")
            print(page_text)
            
            print(f"\n{'='*80}")
            print(f"✅ Scraping complete!")
            print(f"{'='*80}\n")
            
        finally:
            await browser.close()


def main():
    """Main entry point."""
    # Get URL from command line or prompt
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        print("Enter URL to scrape (or paste and press Enter):")
        url = input().strip()
    
    # Validate URL
    if not url:
        print("❌ Error: No URL provided")
        sys.exit(1)
    
    # Ensure URL has protocol
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Run async scraping
    asyncio.run(print_scraping_data(url))


if __name__ == '__main__':
    main()
