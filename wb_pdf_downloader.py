from __future__ import annotations

import csv
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import fitz  # PyMuPDF
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# =========================================================
# CONFIG
# =========================================================

START_URL = (
    "https://ieg.worldbankgroup.org/ieg-search-icrr"
    "?search_api_fulltext=rural+road"
    "&field_region%5B%5D=28"
    "&field_region%5B%5D=32"
    "&field_year%5B%5D=2010"
    "&field_year%5B%5D=2011"
    "&field_year%5B%5D=2012"
    "&field_year%5B%5D=2013"
    "&field_year%5B%5D=2014"
    "&field_year%5B%5D=2015"
    "&field_year%5B%5D=2016"
    "&field_year%5B%5D=2017"
    "&field_year%5B%5D=2018"
    "&field_year%5B%5D=2019"
    "&field_year%5B%5D=2020"
    "&field_year%5B%5D=2021"
)

OUT_DIR = Path(r"F:\D\Makereatu AI\ieg_icrr_rural_roads")
PDF_DIR = OUT_DIR / "pdfs"
META_CSV = OUT_DIR / "metadata.csv"
JSONL_PATH = OUT_DIR / "records.jsonl"

# If True, only keep likely Southeast Asia results and exclude China
FILTER_SOUTHEAST_ASIA_EX_CHINA = True

SEA_COUNTRIES = {
    "brunei", "cambodia", "indonesia", "lao", "laos", "malaysia",
    "myanmar", "philippines", "singapore", "thailand", "timor-leste",
    "timor leste", "viet nam", "vietnam"
}

EXCLUDE_COUNTRIES = {"china", "people's republic of china", "prc"}

HEADLESS = True
PAGE_LOAD_TIMEOUT = 40
SLEEP_BETWEEN_PAGES = 2.0


# =========================================================
# SELENIUM SETUP
# =========================================================

def make_driver(headless: bool = True) -> webdriver.Chrome:
    options = ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1600,2000")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


# =========================================================
# HELPERS
# =========================================================

def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def safe_filename(name: str, max_len: int = 180) -> str:
    name = re.sub(r'[<>:"/\\|?*]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name

def collect_links_from_html(html: str, base_url: str) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: Set[str] = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        full = urljoin(base_url, href)
        links.add(full)

    return links

def is_report_url(url: str) -> bool:
    return "ieg.worldbankgroup.org/reports/" in url

def is_pdf_url(url: str) -> bool:
    u = url.lower()
    return (
        u.endswith(".pdf")
        or "documents1.worldbank.org" in u
        or "documents.worldbank.org" in u
        or "/pdf/" in u
    )

def likely_sea_not_china(text: str) -> bool:
    t = clean_text(text).lower()
    has_sea = any(c in t for c in SEA_COUNTRIES)
    has_excluded = any(c in t for c in EXCLUDE_COUNTRIES)
    return has_sea and not has_excluded

def get_page_title_and_text(driver: webdriver.Chrome) -> Tuple[str, str]:
    title = clean_text(driver.title)
    try:
        body_text = clean_text(driver.find_element(By.TAG_NAME, "body").text)
    except Exception:
        body_text = ""
    return title, body_text

def wait_for_page_ready(driver: webdriver.Chrome, timeout: int = 20) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )

def find_next_page(driver: webdriver.Chrome) -> Optional[str]:
    """
    Try to find a next-page link in a robust way.
    """
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    # Common patterns
    candidates = []

    for a in soup.select("a[href]"):
        text = clean_text(a.get_text(" ", strip=True)).lower()
        href = a.get("href", "")
        rel = " ".join(a.get("rel", [])).lower()

        if "next" in text or rel == "next" or "pagination__next" in " ".join(a.get("class", [])):
            candidates.append(urljoin(driver.current_url, href))

    # Deduplicate and avoid current page
    for c in candidates:
        if c and c != driver.current_url:
            return c

    return None


# =========================================================
# SCRAPING
# =========================================================

def scrape_search_results(driver: webdriver.Chrome, start_url: str) -> Tuple[Set[str], Set[str]]:
    report_urls: Set[str] = set()
    pdf_urls: Set[str] = set()
    seen_pages: Set[str] = set()

    current = start_url
    page_no = 0

    while current and current not in seen_pages:
        page_no += 1
        seen_pages.add(current)
        print(f"[search] page {page_no}: {current}")

        driver.get(current)
        wait_for_page_ready(driver)
        time.sleep(SLEEP_BETWEEN_PAGES)

        html = driver.page_source
        links = collect_links_from_html(html, driver.current_url)

        for link in links:
            if is_report_url(link):
                report_urls.add(link)
            elif is_pdf_url(link):
                pdf_urls.add(link)

        nxt = find_next_page(driver)
        current = nxt

    return report_urls, pdf_urls


def scrape_report_page(driver: webdriver.Chrome, url: str) -> Dict:
    print(f"[report] {url}")
    driver.get(url)
    wait_for_page_ready(driver)
    time.sleep(1.5)

    title, body_text = get_page_title_and_text(driver)
    links = collect_links_from_html(driver.page_source, driver.current_url)

    report_pdfs = sorted({u for u in links if is_pdf_url(u)})
    keep = True

    if FILTER_SOUTHEAST_ASIA_EX_CHINA:
        keep = likely_sea_not_china(title + " " + body_text)

    return {
        "report_url": url,
        "title": title,
        "keep": keep,
        "pdf_urls": report_pdfs,
        "body_excerpt": body_text[:3000],
    }


# =========================================================
# DOWNLOAD + PDF EXTRACTION
# =========================================================

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0.0.0 Safari/537.36"
        )
    })
    return s

def guess_pdf_name(record_title: str, pdf_url: str) -> str:
    parsed = urlparse(pdf_url)
    basename = os.path.basename(parsed.path) or "document.pdf"
    if not basename.lower().endswith(".pdf"):
        basename += ".pdf"

    if record_title:
        stem = safe_filename(record_title)
        return f"{stem}.pdf"
    return safe_filename(basename)

def download_pdf(session: requests.Session, pdf_url: str, out_path: Path) -> bool:
    try:
        r = session.get(pdf_url, timeout=60, stream=True)
        r.raise_for_status()
        content_type = r.headers.get("Content-Type", "").lower()

        # Some endpoints still serve PDFs without perfect content-type.
        if "pdf" not in content_type and not pdf_url.lower().endswith(".pdf"):
            print(f"  [warn] non-pdf content type: {content_type} :: {pdf_url}")

        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"  [fail] download {pdf_url} :: {e}")
        return False

def extract_pdf_basic(pdf_path: Path) -> Dict:
    try:
        doc = fitz.open(pdf_path)
        pages = doc.page_count
        first_pages = []
        for i in range(min(3, pages)):
            txt = clean_text(doc.load_page(i).get_text("text"))
            first_pages.append({"page": i + 1, "text": txt[:4000]})
        doc.close()
        return {"page_count": pages, "preview_pages": first_pages}
    except Exception as e:
        return {"page_count": None, "preview_pages": [], "extract_error": str(e)}


# =========================================================
# MAIN
# =========================================================

def main() -> None:
    ensure_dirs()

    driver = make_driver(HEADLESS)
    session = make_session()

    try:
        report_urls, direct_pdf_urls = scrape_search_results(driver, START_URL)
        print(f"\nFound {len(report_urls)} report URLs")
        print(f"Found {len(direct_pdf_urls)} direct PDF URLs from search pages\n")

        records: List[Dict] = []
        seen_pdf_urls: Set[str] = set(direct_pdf_urls)

        for report_url in sorted(report_urls):
            rec = scrape_report_page(driver, report_url)
            if not rec["keep"]:
                print("  [skip] not SEA or contains excluded country")
                continue

            for p in rec["pdf_urls"]:
                seen_pdf_urls.add(p)

            records.append(rec)

        # Add orphan direct PDFs that appeared on search pages
        for p in sorted(seen_pdf_urls):
            records.append({
                "report_url": None,
                "title": None,
                "keep": True,
                "pdf_urls": [p],
                "body_excerpt": "",
            })

        # Deduplicate by PDF URL
        unique_pdf_records: Dict[str, Dict] = {}
        for rec in records:
            for pdf_url in rec["pdf_urls"]:
                if pdf_url not in unique_pdf_records:
                    unique_pdf_records[pdf_url] = {
                        "report_url": rec["report_url"],
                        "title": rec["title"],
                        "pdf_url": pdf_url,
                    }

        print(f"\nUnique PDF URLs to download: {len(unique_pdf_records)}\n")

        # Write metadata
        with open(META_CSV, "w", newline="", encoding="utf-8") as f_csv, \
             open(JSONL_PATH, "w", encoding="utf-8") as f_jsonl:

            writer = csv.DictWriter(
                f_csv,
                fieldnames=[
                    "title", "report_url", "pdf_url", "local_pdf",
                    "downloaded", "page_count"
                ]
            )
            writer.writeheader()

            for i, (pdf_url, meta) in enumerate(unique_pdf_records.items(), start=1):
                title = meta.get("title") or f"icrr_{i}"
                fname = guess_pdf_name(title, pdf_url)
                out_pdf = PDF_DIR / safe_filename(fname)

                row = {
                    "title": title,
                    "report_url": meta.get("report_url"),
                    "pdf_url": pdf_url,
                    "local_pdf": str(out_pdf),
                    "downloaded": False,
                    "page_count": None,
                }

                if not out_pdf.exists():
                    ok = download_pdf(session, pdf_url, out_pdf)
                else:
                    ok = True

                row["downloaded"] = ok

                if ok and out_pdf.exists():
                    pdf_info = extract_pdf_basic(out_pdf)
                    row["page_count"] = pdf_info.get("page_count")

                    rec = {
                        **row,
                        "preview_pages": pdf_info.get("preview_pages", []),
                    }
                else:
                    rec = {**row, "preview_pages": []}

                writer.writerow(row)
                f_jsonl.write(json.dumps(rec, ensure_ascii=False) + "\n")
                print(f"[{i}/{len(unique_pdf_records)}] saved: {out_pdf.name}")

        print("\nDone.")
        print(f"Metadata CSV: {META_CSV}")
        print(f"JSONL:        {JSONL_PATH}")
        print(f"PDF folder:   {PDF_DIR}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()