"""
process.py — Convert scraped ESDC NOC HTML pages to clean Markdown.

Reads from data/html/{code}.html → writes to data/pages/{code}.md

Usage:
    python scripts/process.py
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
HTML_DIR = ROOT / "data" / "html"
PAGES_DIR = ROOT / "data" / "pages"
PAGES_DIR.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(Path(__file__).parent))
from noc_list import NOC_UNIT_GROUPS, BOC_NAMES

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 not installed. Run: pip install beautifulsoup4 lxml")
    sys.exit(1)


def html_to_markdown(html: str, code: str, title: str) -> str:
    """Extract the relevant occupation content and convert to clean Markdown."""
    soup = BeautifulSoup(html, "lxml")

    # Remove nav, header, footer, scripts, styles
    for tag in soup.select("header, footer, nav, script, style, .navbar, .breadcrumb, "
                            ".cookie-banner, #onetrust-banner-sdk, .site-footer"):
        tag.decompose()

    # Try to find the main content area
    main = (
        soup.find("main")
        or soup.find(id="mainContent")
        or soup.find(class_="occupation-detail")
        or soup.find(class_="container")
        or soup.body
    )

    if not main:
        return f"# {code} {title}\n\n(No content extracted)\n"

    lines = [f"# {code} — {title}\n"]

    def process_element(el):
        """Recursively convert elements to Markdown text."""
        result = []
        for child in el.children:
            if hasattr(child, "name"):
                tag = child.name
                text = child.get_text(separator=" ", strip=True)

                if not text:
                    continue

                if tag in ("h1", "h2"):
                    result.append(f"\n## {text}\n")
                elif tag == "h3":
                    result.append(f"\n### {text}\n")
                elif tag in ("h4", "h5", "h6"):
                    result.append(f"\n#### {text}\n")
                elif tag == "p":
                    result.append(f"\n{text}\n")
                elif tag in ("ul", "ol"):
                    items = child.find_all("li", recursive=False)
                    for item in items:
                        item_text = item.get_text(separator=" ", strip=True)
                        result.append(f"- {item_text}")
                    result.append("")
                elif tag == "li":
                    pass  # handled above
                elif tag in ("table",):
                    # Convert table to simple text
                    rows = child.find_all("tr")
                    for row in rows:
                        cells = row.find_all(["td", "th"])
                        row_text = " | ".join(c.get_text(strip=True) for c in cells)
                        if row_text.strip():
                            result.append(row_text)
                    result.append("")
                elif tag in ("div", "section", "article"):
                    result.extend(process_element(child))
                elif tag in ("strong", "b", "em", "i", "span", "a"):
                    # inline — skip, parent will pick up via get_text
                    pass
            # text nodes: skip (parents use get_text)
        return result

    content_lines = process_element(main)

    # Deduplicate and clean up blank lines
    seen = set()
    cleaned = []
    prev_blank = False
    for line in content_lines:
        stripped = line.strip()
        if not stripped:
            if not prev_blank:
                cleaned.append("")
            prev_blank = True
            continue
        prev_blank = False
        # Skip repeated identical lines (nav items, etc.)
        if stripped in seen and len(stripped) < 80:
            continue
        seen.add(stripped)
        cleaned.append(line)

    lines.extend(cleaned)
    return "\n".join(lines).strip() + "\n"


def main():
    html_files = {f.stem: f for f in HTML_DIR.glob("*.html")}
    noc_map = {code: (title, boc, teer) for code, title, boc, teer in NOC_UNIT_GROUPS}

    missing = [code for code, *_ in NOC_UNIT_GROUPS if code not in html_files]
    if missing:
        print(f"WARNING: {len(missing)} HTML files missing. Run scripts/scrape.py first.")
        print(f"  Missing: {missing[:10]}{'...' if len(missing) > 10 else ''}")

    to_process = [(code, title) for code, title, *_ in NOC_UNIT_GROUPS if code in html_files]
    print(f"Processing {len(to_process)} HTML files → Markdown...")

    ok = errors = 0
    for code, title in to_process:
        dest = PAGES_DIR / f"{code}.md"
        try:
            html = html_files[code].read_text(encoding="utf-8", errors="replace")
            md = html_to_markdown(html, code, title)
            dest.write_text(md, encoding="utf-8")
            ok += 1
        except Exception as e:
            print(f"  ERROR {code}: {e}")
            errors += 1

    print(f"Done. {ok} Markdown files written to data/pages/, {errors} errors.")


if __name__ == "__main__":
    main()
