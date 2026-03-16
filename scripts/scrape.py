"""
scrape.py — Download all 418 NOC 2021 unit group pages from the ESDC website.

Uses Playwright (non-headless) because the ESDC site uses JavaScript rendering.
Results cached to data/html/ — only needs to run once.

Usage:
    python scripts/scrape.py [--force]   # --force re-scrapes already-cached pages
"""

import argparse
import time
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
HTML_DIR = ROOT / "data" / "html"
HTML_DIR.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(Path(__file__).parent))
from noc_list import NOC_UNIT_GROUPS

BASE_URL = "https://noc.esdc.gc.ca/Occupations/OccupationDetail?code={code}&version=noc2021v1_0"


def scrape(force: bool = False):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: Playwright not installed. Run: pip install playwright && playwright install chromium")
        sys.exit(1)

    to_scrape = []
    for code, title, boc, teer in NOC_UNIT_GROUPS:
        dest = HTML_DIR / f"{code}.html"
        if not dest.exists() or force:
            to_scrape.append((code, title))

    if not to_scrape:
        print(f"All {len(NOC_UNIT_GROUPS)} pages already cached in data/html/. Use --force to re-scrape.")
        return

    print(f"Scraping {len(to_scrape)} NOC pages (cached: {len(NOC_UNIT_GROUPS) - len(to_scrape)})...")
    print("NOTE: Browser will open visibly — ESDC blocks headless scraping.\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.set_default_timeout(30_000)

        # Accept cookies / dismiss any overlays on first load
        page.goto("https://noc.esdc.gc.ca/", wait_until="networkidle")
        time.sleep(1)
        for selector in ["button#onetrust-accept-btn-handler", "[aria-label='Accept all cookies']"]:
            try:
                page.click(selector, timeout=2000)
                break
            except Exception:
                pass

        errors = []
        for i, (code, title) in enumerate(to_scrape):
            dest = HTML_DIR / f"{code}.html"
            url = BASE_URL.format(code=code)
            try:
                page.goto(url, wait_until="networkidle")
                # Wait for the main content section
                page.wait_for_selector("main, #mainContent, .occupation-detail", timeout=15_000)
                html = page.content()
                dest.write_text(html, encoding="utf-8")
                pct = (i + 1) / len(to_scrape) * 100
                print(f"  [{i+1:3d}/{len(to_scrape)}] {pct:5.1f}%  {code}  {title[:55]}")
                time.sleep(0.4)  # polite delay
            except Exception as e:
                print(f"  ERROR {code} {title}: {e}")
                errors.append(code)

        browser.close()

    print(f"\nDone. {len(to_scrape) - len(errors)} saved, {len(errors)} errors.")
    if errors:
        print(f"Failed codes: {errors}")
        print("Re-run with --force to retry failed pages.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Re-scrape already-cached pages")
    args = parser.parse_args()
    scrape(force=args.force)
