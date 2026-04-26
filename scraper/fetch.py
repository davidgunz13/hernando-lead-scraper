"""Automated motivated-seller lead scraper for Hernando County, Florida.

Sources:
  Clerk Official Records: https://or.hernandoclerk.com/LandmarkWeb/Home/Index
  Property Appraiser:     https://propsearch.hernandocountypa-florida.us/home

The county portals are public-record web applications with generated markup.
This scraper therefore uses Playwright for the Clerk portal and keeps the
parsing/enrichment logic defensive: individual bad records are logged and
skipped instead of crashing the whole run.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import tempfile
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable
from urllib.parse import parse_qsl, quote_plus, urljoin

import requests
from bs4 import BeautifulSoup
from dbfread import DBF
from playwright.async_api import Browser, Page, TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


CLERK_HOME = "https://or.hernandoclerk.com/LandmarkWeb/Home/Index"
CLERK_ROOT = "https://or.hernandoclerk.com"
PA_HOME = "https://propsearch.hernandocountypa-florida.us/home"
PA_ROOT = "https://propsearch.hernandocountypa-florida.us"

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "30"))
DOCUMENT_TYPES = ("LIS PENDENS", "PROBATE")
OUTPUT_JSON_PATHS = (Path("dashboard/records.json"), Path("data/records.json"))
DATASIFT_CSV_PATHS = (Path("dashboard/datasift_export.csv"), Path("data/datasift_export.csv"))
HTTP_TIMEOUT = 45
MAX_RETRIES = 3
CLERK_DOC_TYPE_IDS = {
    "LIS PENDENS": "987,988,989,1153,1154,1155",
    "PROBATE": "1044,1045,1107,1175",
}

PROBATE_USERNAME = os.getenv("HERNANDO_CLERK_USERNAME", "")
PROBATE_PASSWORD = os.getenv("HERNANDO_CLERK_PASSWORD", "")

OWNER_COLUMNS = ("OWNER", "OWN1", "OWNER1", "NAME", "OWN_NAME")
SITE_ADDRESS_COLUMNS = ("SITE_ADDR", "SITEADDR", "SITUS", "PHY_ADDR", "PROPERTY_ADDRESS")
SITE_CITY_COLUMNS = ("SITE_CITY", "SITECITY", "PHY_CITY")
SITE_ZIP_COLUMNS = ("SITE_ZIP", "SITEZIP", "PHY_ZIP")
MAIL_ADDRESS_COLUMNS = ("ADDR_1", "MAILADR1", "MAIL_ADDR", "MAILING_ADDRESS", "MAILADDR")
MAIL_CITY_COLUMNS = ("CITY", "MAILCITY", "MAIL_CITY")
MAIL_STATE_COLUMNS = ("STATE", "MAILSTATE", "MAIL_STATE")
MAIL_ZIP_COLUMNS = ("ZIP", "MAILZIP", "MAIL_ZIP")
LEGAL_COLUMNS = ("LEGAL", "LEGALDESC", "LEGAL_DESC", "LEGAL_DESCRIPTION")
KEY_COLUMNS = ("KEY", "KEYNUM", "KEY_NO", "PARCEL", "PARCEL_ID", "ALTKEY", "AKPAR")

LOGGER = logging.getLogger("hernando_scraper")


@dataclass
class ParcelRecord:
    owner: str = ""
    legal: str = ""
    key: str = ""
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = "FL"
    prop_zip: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = ""
    mail_zip: str = ""


@dataclass
class LeadRecord:
    doc_num: str = ""
    doc_type: str = ""
    filed: str = ""
    cat: str = ""
    cat_label: str = ""
    owner: str = ""
    grantee: str = ""
    amount: str = ""
    legal: str = ""
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = "FL"
    prop_zip: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = ""
    mail_zip: str = ""
    clerk_url: str = ""
    flags: list[str] = field(default_factory=list)
    score: int = 0
    petitioner_name: str = ""
    petitioner_address: str = ""
    beneficiaries: str = ""
    beneficiaries_addresses: str = ""
    interest_ownership_percentage: str = ""
    parcel_key: str = ""

    def public_dict(self) -> dict[str, Any]:
        return {
            "doc_num": self.doc_num,
            "doc_type": self.doc_type,
            "filed": self.filed,
            "cat": self.cat,
            "cat_label": self.cat_label,
            "owner": self.owner,
            "grantee": self.grantee,
            "amount": self.amount,
            "legal": self.legal,
            "prop_address": self.prop_address,
            "prop_city": self.prop_city,
            "prop_state": self.prop_state,
            "prop_zip": self.prop_zip,
            "mail_address": self.mail_address,
            "mail_city": self.mail_city,
            "mail_state": self.mail_state,
            "mail_zip": self.mail_zip,
            "clerk_url": self.clerk_url,
            "flags": self.flags,
            "score": self.score,
        }


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )


async def retry_async(
    label: str,
    fn: Callable[[], Awaitable[Any]],
    attempts: int = MAX_RETRIES,
    delay: float = 2.0,
) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await fn()
        except Exception as exc:  # noqa: BLE001 - keep scheduled runs alive.
            last_exc = exc
            LOGGER.warning("%s failed on attempt %s/%s: %s", label, attempt, attempts, exc)
            if attempt < attempts:
                await asyncio.sleep(delay * attempt)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"{label} failed")


def retry_sync(label: str, fn: Callable[[], Any], attempts: int = MAX_RETRIES, delay: float = 2.0) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - bad rows/pages should not kill the job.
            last_exc = exc
            LOGGER.warning("%s failed on attempt %s/%s: %s", label, attempt, attempts, exc)
            if attempt < attempts:
                import time

                time.sleep(delay * attempt)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"{label} failed")


def clean(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


def normalize_key(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", clean(value).upper())


def normalize_owner(value: str) -> str:
    value = re.sub(r"\b(ESTATE OF|THE ESTATE OF|DECEASED|DECEDENT|DEFENDANT)\b", "", clean(value), flags=re.I)
    value = re.sub(r"[^A-Z0-9,& ]", " ", value.upper())
    value = re.sub(r"\s+", " ", value)
    return value.strip(" ,")


def owner_variants(name: str) -> set[str]:
    normalized = normalize_owner(name)
    variants = {normalized} if normalized else set()
    if "," in normalized:
        last, first = [part.strip() for part in normalized.split(",", 1)]
        if first and last:
            variants.add(f"{first} {last}")
            variants.add(f"{last} {first}")
            variants.add(f"{last}, {first}")
    else:
        parts = normalized.split()
        if len(parts) >= 2:
            first = parts[0]
            last = parts[-1]
            middle = " ".join(parts[1:-1])
            variants.add(f"{first} {last}")
            variants.add(f"{last} {first}")
            variants.add(f"{last}, {first}")
            if middle:
                variants.add(f"{last}, {first} {middle}")
    return {variant for variant in variants if variant}


def first_value(row: dict[str, Any], candidates: Iterable[str]) -> str:
    upper = {clean(key).upper(): value for key, value in row.items()}
    for candidate in candidates:
        if candidate in upper and clean(upper[candidate]):
            return clean(upper[candidate])
    return ""


def parse_amount(value: str) -> Decimal:
    text = clean(value)
    if not text:
        return Decimal("0")
    match = re.search(r"\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)", text)
    if not match:
        return Decimal("0")
    try:
        return Decimal(match.group(1).replace(",", ""))
    except InvalidOperation:
        return Decimal("0")


def parse_date(value: str) -> date | None:
    text = clean(value)
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    match = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", text)
    if not match:
        return None
    month, day, year = match.groups()
    year = f"20{year}" if len(year) == 2 else year
    try:
        return date(int(year), int(month), int(day))
    except ValueError:
        return None


def is_corporate_owner(owner: str) -> bool:
    return bool(re.search(r"\b(LLC|INC|CORP|COMPANY|CO\.|LP|LLP|TRUST|BANK|ASSOCIATION|HOLDINGS)\b", owner, re.I))


def category_for_doc_type(doc_type: str) -> tuple[str, str]:
    if "LIS" in doc_type.upper():
        return "LP", "Lis Pendens"
    if "PROBATE" in doc_type.upper():
        return "PR", "Probate"
    return "OT", clean(doc_type).title()


async def accept_clerk_disclaimer(page: Page) -> None:
    for selector in (
        "text=/I agree/i",
        "button:has-text('Accept')",
        "input[value*='Accept']",
        "text=/Accept/i",
    ):
        try:
            locator = page.locator(selector).first
            if await locator.count():
                await locator.click(timeout=2500)
                await page.wait_for_timeout(1000)
                return
        except Exception:
            continue


async def maybe_login(page: Page) -> None:
    if not (PROBATE_USERNAME and PROBATE_PASSWORD):
        return
    try:
        if await page.locator("text=/Log\\s*On|Login|Sign\\s*In/i").count():
            await page.locator("text=/Log\\s*On|Login|Sign\\s*In/i").first.click(timeout=4000)
            await page.wait_for_load_state("networkidle", timeout=15000)
        for selector in ("input[name*='User' i]", "input[id*='User' i]", "input[type='text']"):
            if await page.locator(selector).count():
                await page.locator(selector).first.fill(PROBATE_USERNAME)
                break
        for selector in ("input[name*='Password' i]", "input[id*='Password' i]", "input[type='password']"):
            if await page.locator(selector).count():
                await page.locator(selector).first.fill(PROBATE_PASSWORD)
                break
        if await page.locator("button:has-text('Log'), input[type='submit']").count():
            await page.locator("button:has-text('Log'), input[type='submit']").first.click()
            await page.wait_for_load_state("networkidle", timeout=15000)
            LOGGER.info("Logged into Clerk portal for probate detail access.")
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Clerk login was not completed: %s", exc)


async def select_option_containing(page: Page, terms: Iterable[str], exact: bool = False) -> bool:
    wanted = [term.upper() for term in terms if clean(term)]
    candidates = page.locator("select")
    for idx in range(await candidates.count()):
        select = candidates.nth(idx)
        try:
            options = await select.locator("option").all_text_contents()
            for option in options:
                normalized = clean(option).upper()
                if not normalized:
                    continue
                matched = normalized in wanted if exact else any(term in normalized for term in wanted)
                if matched:
                    await select.select_option(label=option.strip(), timeout=3000)
                    await page.wait_for_load_state("networkidle", timeout=10000)
                    await page.wait_for_timeout(750)
                    return True
        except Exception:
            continue
    return False


async def click_text_candidate(page: Page, patterns: Iterable[str]) -> bool:
    for pattern in patterns:
        try:
            locator = page.locator(f"text=/{pattern}/i").first
            if await locator.count():
                await locator.click(timeout=5000)
                await page.wait_for_load_state("networkidle", timeout=15000)
                await page.wait_for_timeout(750)
                return True
        except Exception:
            continue
    return False


async def open_document_type_search(page: Page) -> None:
    candidate_urls = (
        "https://or.hernandoclerk.com/LandmarkWeb/Search/DocumentType",
        "https://or.hernandoclerk.com/LandmarkWeb/Search/DocType",
        "https://or.hernandoclerk.com/LandmarkWeb/Document/SearchByDocumentType",
        CLERK_HOME,
    )
    for url in candidate_urls:
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await accept_clerk_disclaimer(page)
            text = (await page.content()).lower()
            if "document type" in text:
                break
        except Exception:
            continue

    await select_option_containing(page, ("Document Type",), exact=True)
    await click_text_candidate(page, (r"document\s*type",))

    for selector in (
        "a:has-text('Document Type')",
        "button:has-text('Document Type')",
        "input[value*='Document Type']",
    ):
        try:
            if await page.locator(selector).count():
                await page.locator(selector).first.click(timeout=5000)
                await page.wait_for_load_state("networkidle", timeout=15000)
                await page.wait_for_timeout(750)
                break
        except Exception:
            continue


async def fill_first_matching(page: Page, selectors: Iterable[str], value: str) -> bool:
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count():
                await locator.fill(value, timeout=3000)
                return True
        except Exception:
            continue
    return False


async def choose_document_type(page: Page, doc_type: str) -> None:
    if await select_option_containing(page, (doc_type,)):
        return
    for selector in ("input[name*='DocType' i]", "input[id*='DocType' i]", "input[placeholder*='document' i]"):
        if await fill_first_matching(page, (selector,), doc_type):
            return


async def choose_last_30_days(page: Page) -> bool:
    return await select_option_containing(page, ("Last 30 Days", "Last 30", "30 Days"))


async def set_max_results_per_page(page: Page) -> None:
    candidates = page.locator("select")
    best: tuple[int, int, str] | None = None
    for idx in range(await candidates.count()):
        select = candidates.nth(idx)
        try:
            options = await select.locator("option").all_text_contents()
            for option in options:
                text = clean(option)
                match = re.search(r"\b(25|50|100|200|500|1000|2000)\b", text)
                if match:
                    value = int(match.group(1))
                    if value <= 2000 and (best is None or value > best[0]):
                        best = (value, idx, option.strip())
        except Exception:
            continue
    if best:
        _, idx, label = best
        try:
            await candidates.nth(idx).select_option(label=label, timeout=3000)
            await page.wait_for_load_state("networkidle", timeout=10000)
            await page.wait_for_timeout(750)
            LOGGER.info("Set Clerk results per page to %s.", label)
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Could not set results per page to %s: %s", label, exc)


async def log_clerk_page_diagnostics(page: Page, doc_type: str) -> None:
    try:
        title = await page.title()
        selects: list[str] = []
        for idx in range(await page.locator("select").count()):
            options = [clean(option) for option in await page.locator("select").nth(idx).locator("option").all_text_contents()]
            options = [option for option in options if option][:12]
            if options:
                selects.append(f"select {idx}: {options}")
        table_count = await page.locator("table").count()
        LOGGER.info("No %s rows parsed. Current URL: %s", doc_type, page.url)
        LOGGER.info("Clerk page title: %s; tables: %s; selects: %s", title, table_count, " | ".join(selects)[:1800])
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("Could not log Clerk diagnostics: %s", exc)


async def submit_search(page: Page) -> None:
    for selector in (
        "button:has-text('Search')",
        "input[type='submit'][value*='Search']",
        "a:has-text('Search')",
        "button:has-text('Submit')",
    ):
        try:
            if await page.locator(selector).count():
                await page.locator(selector).first.click(timeout=5000)
                await page.wait_for_load_state("networkidle", timeout=30000)
                await page.wait_for_timeout(1000)
                return
        except Exception:
            continue


async def run_clerk_search(page: Page, doc_type: str, start_date: date, end_date: date) -> str:
    await open_document_type_search(page)
    await choose_document_type(page, doc_type)
    start = start_date.strftime("%m/%d/%Y")
    end = end_date.strftime("%m/%d/%Y")
    used_relative_date = await choose_last_30_days(page)
    start_filled = False
    end_filled = False
    if not used_relative_date:
        start_filled = await fill_first_matching(
            page,
            (
                "input[name*='Start' i]",
                "input[id*='Start' i]",
                "input[name*='From' i]",
                "input[id*='From' i]",
                "input[placeholder*='Start' i]",
                "input[placeholder*='From' i]",
                "input[type='date']",
            ),
            start,
        )
        end_filled = await fill_first_matching(
            page,
            (
                "input[name*='End' i]",
                "input[id*='End' i]",
                "input[name*='To' i]",
                "input[id*='To' i]",
                "input[placeholder*='End' i]",
                "input[placeholder*='To' i]",
            ),
            end,
        )
    if not used_relative_date and not (start_filled and end_filled):
        date_inputs = page.locator("input[type='date']")
        if await date_inputs.count() >= 2:
            await date_inputs.nth(0).fill(start_date.isoformat(), timeout=3000)
            await date_inputs.nth(1).fill(end_date.isoformat(), timeout=3000)
    await set_max_results_per_page(page)
    await submit_search(page)
    await set_max_results_per_page(page)
    return await page.content()


def table_rows_from_html(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict[str, str]] = []
    for table in soup.select("table"):
        header_cells = table.select("thead th")
        if not header_cells:
            first_row = table.find("tr")
            header_cells = first_row.find_all(["th", "td"]) if first_row else []
        headers = [clean(cell.get_text(" ")) for cell in header_cells]
        if len(headers) < 2:
            continue
        header_text = " ".join(headers).lower()
        result_header_tokens = (
            "document",
            "instrument",
            "book",
            "page",
            "record",
            "date",
            "filed",
            "type",
            "legal",
            "grantor",
            "grantee",
            "defendant",
            "decedent",
            "party",
        )
        if not any(token in header_text for token in result_header_tokens):
            continue
        for tr in table.select("tbody tr") or table.select("tr")[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            values = [clean(cell.get_text(" ")) for cell in cells]
            row = {headers[i]: values[i] for i in range(min(len(headers), len(values)))}
            link = tr.find("a", href=True)
            if link:
                row["_url"] = urljoin(CLERK_ROOT, link["href"])
            if any(row.values()):
                rows.append(row)
    return rows


def extract_by_labels(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "lxml")
    details: dict[str, str] = {}
    for tr in soup.select("tr"):
        cells = [clean(cell.get_text(" ")) for cell in tr.find_all(["td", "th"])]
        if len(cells) == 2:
            details[cells[0].rstrip(":")] = cells[1]
    for label in soup.select("label"):
        key = clean(label.get_text(" ")).rstrip(":")
        if not key:
            continue
        target = label.get("for")
        value = ""
        if target:
            target_el = soup.find(id=target)
            value = clean(target_el.get_text(" ") if target_el else "")
        if not value:
            parent = label.parent
            value = clean(parent.get_text(" ")) if parent else ""
            value = value.replace(key, "", 1).strip(" :")
        if value:
            details[key] = value
    return details


def value_for(row: dict[str, str], *needles: str) -> str:
    for key, value in row.items():
        normalized = clean(key).lower()
        if any(needle.lower() in normalized for needle in needles) and clean(value):
            return clean(value)
    return ""


def clean_clerk_cell(value: Any) -> str:
    text = clean(value)
    text = re.sub(r"^(hidden_legalfield_|hidden_|nobreak_|unclickable_)", "", text)
    if "<" in text and ">" in text:
        text = BeautifulSoup(text.replace("<div class='nameSeperator'></div>", "; "), "lxml").get_text(" ")
    return clean(text)


def lead_from_row(row: dict[str, str], doc_type: str) -> LeadRecord:
    cat, cat_label = category_for_doc_type(doc_type)
    owner = (
        value_for(row, "defendant")
        if "LIS" in doc_type.upper()
        else value_for(row, "decedent")
    )
    owner = owner or value_for(row, "grantor", "party", "name", "owner")
    return LeadRecord(
        doc_num=value_for(row, "document", "instrument", "doc #", "doc no") or value_for(row, "number"),
        doc_type=doc_type,
        filed=value_for(row, "filed", "recorded", "record date", "date"),
        cat=cat,
        cat_label=cat_label,
        owner=owner,
        grantee=value_for(row, "grantee", "plaintiff", "petitioner"),
        amount=value_for(row, "amount", "consideration", "debt"),
        legal=value_for(row, "legal"),
        clerk_url=row.get("_url", ""),
    )


def lead_from_datatables_row(row: dict[str, Any], doc_type: str) -> LeadRecord:
    cat, cat_label = category_for_doc_type(doc_type)
    doc_id = clean_clerk_cell(row.get("26", ""))
    doc_num = clean_clerk_cell(row.get("12", ""))
    direct_name = clean_clerk_cell(row.get("5", ""))
    reverse_name = clean_clerk_cell(row.get("6", ""))
    owner = reverse_name if cat == "LP" else direct_name or reverse_name
    legal = clean_clerk_cell(row.get("14", ""))
    filed = clean_clerk_cell(row.get("7", ""))
    doc_label = clean_clerk_cell(row.get("8", "")) or doc_type
    return LeadRecord(
        doc_num=doc_num,
        doc_type=doc_label,
        filed=filed,
        cat=cat,
        cat_label=cat_label,
        owner=owner,
        grantee=direct_name if cat == "LP" else reverse_name,
        amount=clean_clerk_cell(row.get("13", "")),
        legal=legal,
        clerk_url=(
            f"{CLERK_ROOT}/LandmarkWeb/Document/Index?id={quote_plus(doc_id)}"
            if doc_id
            else CLERK_HOME
        ),
    )


def has_real_record_signal(lead: LeadRecord) -> bool:
    """Reject profile/contact form rows that can appear in Landmark markup."""
    owner = normalize_owner(lead.owner)
    bogus_owners = {
        "FIRST NAME",
        "EMAIL",
        "ADDRESS",
        "CITY",
        "ZIP",
        "PHONE",
        "FAX",
        "USER NAME",
        "PASSWORD",
    }
    if owner in bogus_owners:
        return False
    if lead.doc_num and re.search(r"\d", lead.doc_num):
        return True
    if lead.filed and parse_date(lead.filed):
        return True
    if lead.clerk_url and re.search(r"(document|instrument|record|details|image|view)", lead.clerk_url, re.I):
        return True
    if lead.legal and len(lead.legal) >= 20:
        return True
    return False


async def enrich_from_detail_page(page: Page, lead: LeadRecord) -> LeadRecord:
    if not lead.clerk_url:
        return lead
    try:
        await page.goto(lead.clerk_url, wait_until="networkidle", timeout=30000)
        await accept_clerk_disclaimer(page)
        details = extract_by_labels(await page.content())
        if not lead.doc_num:
            lead.doc_num = value_for(details, "document", "instrument", "doc")
        if not lead.filed:
            lead.filed = value_for(details, "filed", "recorded", "record date")
        if not lead.legal:
            lead.legal = value_for(details, "legal")
        if not lead.parcel_key:
            lead.parcel_key = value_for(details, "key", "parcel")
        if not lead.amount:
            lead.amount = value_for(details, "amount", "consideration", "debt")
        if not lead.owner:
            owner_key = "defendant" if lead.cat == "LP" else "decedent"
            lead.owner = value_for(details, owner_key, "grantor", "party", "name")
        if lead.cat == "PR":
            lead.petitioner_name = value_for(details, "petitioner")
            lead.petitioner_address = value_for(details, "petitioner address")
            lead.beneficiaries = value_for(details, "beneficiar")
            lead.beneficiaries_addresses = value_for(details, "beneficiar address")
            lead.interest_ownership_percentage = value_for(details, "interest", "ownership", "percentage")
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Could not enrich detail page %s: %s", lead.clerk_url, exc)
    return lead


async def fetch_clerk_records(start_date: date, end_date: date) -> list[LeadRecord]:
    direct_leads = fetch_clerk_records_direct(start_date, end_date)
    if direct_leads:
        LOGGER.info("Fetched %s Clerk records through direct Landmark results endpoint.", len(direct_leads))
        return dedupe_leads(direct_leads)

    LOGGER.warning("Direct Clerk endpoint returned no records; falling back to Playwright grid scraping.")
    async with async_playwright() as pw:
        browser: Browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        )
        page = await context.new_page()
        await retry_async("open clerk home", lambda: page.goto(CLERK_HOME, wait_until="networkidle", timeout=30000))
        await accept_clerk_disclaimer(page)
        await maybe_login(page)
        leads: list[LeadRecord] = []
        for doc_type in DOCUMENT_TYPES:
            try:
                LOGGER.info("Searching Clerk portal for %s.", doc_type)
                html = await retry_async(
                    f"clerk search {doc_type}",
                    lambda doc_type=doc_type: run_clerk_search(page, doc_type, start_date, end_date),
                )
                rows = table_rows_from_html(html)
                LOGGER.info("Found %s candidate %s rows.", len(rows), doc_type)
                if not rows:
                    await log_clerk_page_diagnostics(page, doc_type)
                for row in rows:
                    try:
                        lead = lead_from_row(row, doc_type)
                        if not has_real_record_signal(lead):
                            continue
                        lead = await enrich_from_detail_page(page, lead)
                        if not has_real_record_signal(lead):
                            continue
                        leads.append(lead)
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.warning("Skipping bad Clerk row for %s: %s", doc_type, exc)
            except PlaywrightTimeoutError as exc:
                LOGGER.warning("Timed out searching for %s: %s", doc_type, exc)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Unable to search for %s: %s", doc_type, exc)
        await context.close()
        await browser.close()
    return dedupe_leads(leads)


def fetch_clerk_records_direct(start_date: date, end_date: date) -> list[LeadRecord]:
    session = requests_session()
    leads: list[LeadRecord] = []
    try:
        retry_sync("clerk home", lambda: session.get(CLERK_HOME, timeout=HTTP_TIMEOUT)).raise_for_status()
        retry_sync(
            "clerk disclaimer",
            lambda: session.post(f"{CLERK_ROOT}/LandmarkWeb/Search/SetDisclaimer", timeout=HTTP_TIMEOUT),
        ).raise_for_status()
        search_url = f"{CLERK_ROOT}/LandmarkWeb/search/index?theme=.blue&section=searchCriteriaDocuments&quickSearchSelection="
        retry_sync("clerk document type search page", lambda: session.get(search_url, timeout=HTTP_TIMEOUT)).raise_for_status()
        for doc_type in DOCUMENT_TYPES:
            ids = CLERK_DOC_TYPE_IDS[doc_type]
            criteria = {
                "doctype": ids,
                "beginDate": start_date.strftime("%m/%d/%Y"),
                "endDate": end_date.strftime("%m/%d/%Y"),
                "recordCount": "2000",
                "exclude": "false",
                "ReturnIndexGroups": "false",
                "townName": "",
                "mobileHomesOnly": "false",
                "g-recaptcha-response": "",
            }
            response = retry_sync(
                f"clerk direct criteria {doc_type}",
                lambda criteria=criteria: session.post(
                    f"{CLERK_ROOT}/LandmarkWeb/Search/DocumentTypeSearch",
                    data=criteria,
                    headers={
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": search_url,
                    },
                    timeout=HTTP_TIMEOUT,
                ),
            )
            response.raise_for_status()
            result_json = fetch_clerk_datatables_page(session, search_url)
            data = result_json.get("data") or []
            LOGGER.info("Direct Clerk endpoint returned %s %s rows.", len(data), doc_type)
            for item in data:
                try:
                    lead = lead_from_datatables_row(item, doc_type)
                    filed_date = parse_date(lead.filed)
                    if filed_date and start_date <= filed_date <= end_date and has_real_record_signal(lead):
                        leads.append(lead)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("Skipping bad direct Clerk row for %s: %s", doc_type, exc)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Direct Clerk fetch failed: %s", exc)
    return leads


def fetch_clerk_datatables_page(session: requests.Session, referer: str, start: int = 0, length: int = 2000) -> dict[str, Any]:
    payload: dict[str, str] = {
        "draw": "1",
        "start": str(start),
        "length": str(length),
        "search[value]": "",
        "search[regex]": "false",
        "order[0][column]": "0",
        "order[0][dir]": "asc",
    }
    for idx in range(35):
        payload[f"columns[{idx}][data]"] = str(idx)
        payload[f"columns[{idx}][name]"] = ""
        payload[f"columns[{idx}][searchable]"] = "true"
        payload[f"columns[{idx}][orderable]"] = "true"
        payload[f"columns[{idx}][search][value]"] = ""
        payload[f"columns[{idx}][search][regex]"] = "false"
    response = retry_sync(
        "clerk datatables results",
        lambda: session.post(
            f"{CLERK_ROOT}/LandmarkWeb/Search/GetSearchResults",
            data=payload,
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "Referer": referer,
            },
            timeout=HTTP_TIMEOUT,
        ),
    )
    response.raise_for_status()
    return response.json()


def dedupe_leads(leads: Iterable[LeadRecord]) -> list[LeadRecord]:
    deduped: dict[str, LeadRecord] = {}
    for lead in leads:
        key = normalize_key(lead.doc_num) or f"{normalize_owner(lead.owner)}-{lead.filed}-{lead.cat}"
        if not key:
            continue
        if key not in deduped:
            deduped[key] = lead
    return list(deduped.values())


class ParcelIndex:
    def __init__(self) -> None:
        self.by_owner: dict[str, ParcelRecord] = {}
        self.by_key: dict[str, ParcelRecord] = {}
        self.by_legal: list[ParcelRecord] = []

    def add(self, parcel: ParcelRecord) -> None:
        for variant in owner_variants(parcel.owner):
            self.by_owner.setdefault(variant, parcel)
        if parcel.key:
            self.by_key.setdefault(normalize_key(parcel.key), parcel)
        if parcel.legal:
            self.by_legal.append(parcel)

    def lookup(self, owner: str = "", legal: str = "", key: str = "") -> ParcelRecord | None:
        if key and normalize_key(key) in self.by_key:
            return self.by_key[normalize_key(key)]
        for variant in owner_variants(owner):
            if variant in self.by_owner:
                return self.by_owner[variant]
        legal_norm = normalize_key(legal)
        if legal_norm and len(legal_norm) >= 12:
            for parcel in self.by_legal:
                parcel_legal = normalize_key(parcel.legal)
                if legal_norm in parcel_legal or parcel_legal in legal_norm:
                    return parcel
        return None


def requests_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        }
    )
    return session


def find_bulk_dbf_url(session: requests.Session) -> tuple[str, dict[str, str] | None]:
    response = retry_sync("property appraiser home", lambda: session.get(PA_HOME, timeout=HTTP_TIMEOUT))
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")
    for link in soup.find_all("a", href=True):
        label = clean(link.get_text(" ") + " " + link["href"]).lower()
        if any(token in label for token in ("dbf", "tax roll", "taxroll", "parcel", "download", "bulk")):
            return urljoin(PA_ROOT, link["href"]), None
    for form in soup.find_all("form"):
        form_text = clean(form.get_text(" ")).lower()
        if not any(token in form_text for token in ("dbf", "tax roll", "taxroll", "download", "parcel")):
            continue
        fields = {
            input_el.get("name"): input_el.get("value", "")
            for input_el in form.find_all("input")
            if input_el.get("name")
        }
        target = ""
        for element in form.find_all(["a", "button", "input"]):
            text = clean(element.get_text(" ") or element.get("value", "")).lower()
            href = element.get("href", "")
            if "__doPostBack" in href or any(token in text for token in ("dbf", "download", "tax roll")):
                match = re.search(r"__doPostBack\('([^']+)'(?:,'([^']*)')?\)", href)
                if match:
                    target = match.group(1)
                    fields["__EVENTTARGET"] = target
                    fields["__EVENTARGUMENT"] = match.group(2) or ""
                elif element.get("name"):
                    fields[element["name"]] = element.get("value", "")
                break
        action = form.get("action") or PA_HOME
        if fields:
            return urljoin(PA_ROOT, action), fields
    return "", None


def download_bulk_parcels(session: requests.Session) -> bytes | None:
    try:
        url, post_data = find_bulk_dbf_url(session)
        if not url:
            LOGGER.warning("No bulk parcel DBF download link was discovered.")
            return None
        LOGGER.info("Downloading parcel data from %s.", url)
        if post_data is None:
            response = retry_sync("bulk parcel download", lambda: session.get(url, timeout=HTTP_TIMEOUT))
        else:
            response = retry_sync("bulk parcel postback download", lambda: session.post(url, data=post_data, timeout=HTTP_TIMEOUT))
        response.raise_for_status()
        if len(response.content) < 100:
            LOGGER.warning("Bulk parcel response was too small to be a DBF/ZIP.")
            return None
        return response.content
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Bulk parcel download failed: %s", exc)
        return None


def iter_dbf_rows(blob: bytes) -> Iterable[dict[str, Any]]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        if zipfile.is_zipfile(io.BytesIO(blob)):
            with zipfile.ZipFile(io.BytesIO(blob)) as archive:
                archive.extractall(tmp_path)
            dbf_files = list(tmp_path.rglob("*.dbf")) + list(tmp_path.rglob("*.DBF"))
        else:
            dbf_file = tmp_path / "parcels.dbf"
            dbf_file.write_bytes(blob)
            dbf_files = [dbf_file]
        for dbf_file in dbf_files:
            LOGGER.info("Reading DBF parcel file %s.", dbf_file.name)
            try:
                for row in DBF(str(dbf_file), load=False, ignore_missing_memofile=True, char_decode_errors="ignore"):
                    yield dict(row)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Could not read DBF %s: %s", dbf_file, exc)


def parcel_from_dbf_row(row: dict[str, Any]) -> ParcelRecord:
    return ParcelRecord(
        owner=first_value(row, OWNER_COLUMNS),
        legal=first_value(row, LEGAL_COLUMNS),
        key=first_value(row, KEY_COLUMNS),
        prop_address=first_value(row, SITE_ADDRESS_COLUMNS),
        prop_city=first_value(row, SITE_CITY_COLUMNS),
        prop_state="FL",
        prop_zip=first_value(row, SITE_ZIP_COLUMNS),
        mail_address=first_value(row, MAIL_ADDRESS_COLUMNS),
        mail_city=first_value(row, MAIL_CITY_COLUMNS),
        mail_state=first_value(row, MAIL_STATE_COLUMNS) or "FL",
        mail_zip=first_value(row, MAIL_ZIP_COLUMNS),
    )


def build_parcel_index() -> ParcelIndex:
    index = ParcelIndex()
    session = requests_session()
    blob = download_bulk_parcels(session)
    if not blob:
        return index
    count = 0
    for row in iter_dbf_rows(blob):
        try:
            parcel = parcel_from_dbf_row(row)
            if parcel.owner or parcel.key or parcel.legal:
                index.add(parcel)
                count += 1
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Skipping bad parcel row: %s", exc)
    LOGGER.info("Indexed %s parcel rows.", count)
    return index


def lookup_property_individually(session: requests.Session, owner: str = "", legal: str = "", key: str = "") -> ParcelRecord | None:
    queries = [key, legal, owner]
    for query in [clean(q) for q in queries if clean(q)]:
        try:
            params = {"search": query}
            response = retry_sync(
                f"property lookup {query[:30]}",
                lambda params=params: session.get(PA_HOME, params=params, timeout=HTTP_TIMEOUT),
            )
            response.raise_for_status()
            details = extract_property_details(response.text)
            if details:
                return details
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Individual property lookup failed for %s: %s", query, exc)
    return None


def extract_property_details(html: str) -> ParcelRecord | None:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ")
    if not re.search(r"(owner|mailing|site|property)", text, re.I):
        return None
    details = extract_by_labels(html)
    owner = value_for(details, "owner")
    prop_address = value_for(details, "site address", "property address", "situs")
    mail_address = value_for(details, "mailing address", "mail address")
    legal = value_for(details, "legal")
    key = value_for(details, "key", "parcel")
    if not (owner or prop_address or mail_address or legal):
        return None
    return ParcelRecord(
        owner=owner,
        legal=legal,
        key=key,
        prop_address=prop_address,
        prop_city=value_for(details, "site city", "property city"),
        prop_state="FL",
        prop_zip=value_for(details, "site zip", "property zip"),
        mail_address=mail_address,
        mail_city=value_for(details, "mail city", "mailing city"),
        mail_state=value_for(details, "mail state", "mailing state") or "FL",
        mail_zip=value_for(details, "mail zip", "mailing zip"),
    )


def apply_parcel_to_lead(lead: LeadRecord, parcel: ParcelRecord | None) -> None:
    if not parcel:
        return
    lead.prop_address = lead.prop_address or parcel.prop_address
    lead.prop_city = lead.prop_city or parcel.prop_city
    lead.prop_state = lead.prop_state or parcel.prop_state or "FL"
    lead.prop_zip = lead.prop_zip or parcel.prop_zip
    lead.mail_address = lead.mail_address or parcel.mail_address
    lead.mail_city = lead.mail_city or parcel.mail_city
    lead.mail_state = lead.mail_state or parcel.mail_state
    lead.mail_zip = lead.mail_zip or parcel.mail_zip
    lead.legal = lead.legal or parcel.legal


def score_lead(lead: LeadRecord, today: date) -> None:
    flags: list[str] = []
    if lead.cat == "LP":
        flags.append("Foreclosure")
    if lead.cat == "PR":
        flags.append("Probate / estate")
    if is_corporate_owner(lead.owner):
        flags.append("LLC / corp owner")
    filed = parse_date(lead.filed)
    if filed and filed >= today - timedelta(days=7):
        flags.append("New this week")
    score = 30 + (10 * len(flags))
    amount = parse_amount(lead.amount)
    if amount > Decimal("100000"):
        score += 15
    elif amount > Decimal("50000"):
        score += 10
    if "New this week" in flags:
        score += 5
    if lead.prop_address or lead.mail_address:
        score += 5
    lead.flags = flags
    lead.score = max(0, min(score, 100))


def apply_combo_bonus(leads: list[LeadRecord]) -> None:
    by_owner: dict[str, set[str]] = {}
    for lead in leads:
        normalized = normalize_owner(lead.owner)
        if normalized:
            by_owner.setdefault(normalized, set()).add(lead.cat)
    combo_owners = {owner for owner, cats in by_owner.items() if {"LP", "PR"}.issubset(cats)}
    for lead in leads:
        if normalize_owner(lead.owner) in combo_owners:
            lead.score = min(100, lead.score + 20)
            if "LP+PR combo" not in lead.flags:
                lead.flags.append("LP+PR combo")


def enrich_leads_with_property_data(leads: list[LeadRecord]) -> None:
    index = build_parcel_index()
    session = requests_session()
    for lead in leads:
        try:
            parcel = index.lookup(owner=lead.owner, legal=lead.legal, key=lead.parcel_key)
            if not parcel:
                parcel = lookup_property_individually(
                    session,
                    owner=lead.owner,
                    legal=lead.legal,
                    key=lead.parcel_key,
                )
            apply_parcel_to_lead(lead, parcel)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Property enrichment failed for %s: %s", lead.doc_num or lead.owner, exc)


def primary_party_name(name: str) -> str:
    parties = [clean(part) for part in re.split(r"\s*;\s*|\s+\|\s+", clean(name)) if clean(part)]
    if not parties:
        return clean(name)
    filler = re.compile(r"\b(UNKNOWN|TENANT|SPOUSE|PERSONS|OCCUPANT|WHOM IT MAY CONCERN)\b", re.I)
    for party in parties:
        if not filler.search(party):
            return party
    return parties[0]


def split_name(name: str) -> tuple[str, str]:
    name = normalize_owner(primary_party_name(name))
    if "," in name:
        last, first = [part.strip().title() for part in name.split(",", 1)]
        return first, last
    if is_corporate_owner(name):
        return "", name.title()
    parts = name.title().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return "", parts[0]
    last = parts[0]
    first = " ".join(parts[1:])
    return first, last


def datasift_rows(leads: Iterable[LeadRecord]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for lead in leads:
        first, last = split_name(lead.owner)
        rows.append(
            {
                "First Name": first,
                "Last Name": last,
                "Mailing Address": lead.mail_address,
                "Mailing City": lead.mail_city,
                "Mailing State": lead.mail_state,
                "Mailing Zip": lead.mail_zip,
                "Property Address": lead.prop_address,
                "Property City": lead.prop_city,
                "Property State": lead.prop_state,
                "Property Zip": lead.prop_zip,
                "List Type": lead.cat_label,
                "Document Type": lead.doc_type,
                "Date Filed": lead.filed,
                "Document Number": lead.doc_num,
                "Amount/Debt Owed": lead.amount,
                "Seller Score": str(lead.score),
                "Motivated Seller Flags": "; ".join(lead.flags),
                "Source": "Hernando County Clerk / Property Appraiser",
                "Public Records URL": lead.clerk_url,
            }
        )
    return rows


def write_json_outputs(leads: list[LeadRecord], start_date: date, end_date: date) -> None:
    fetched_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "fetched_at": fetched_at,
        "source": {
            "clerk": CLERK_HOME,
            "property_appraiser": PA_HOME,
        },
        "date_range": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "lookback_days": LOOKBACK_DAYS,
        },
        "total": len(leads),
        "with_address": sum(1 for lead in leads if lead.prop_address or lead.mail_address),
        "records": [lead.public_dict() for lead in leads],
    }
    for path in OUTPUT_JSON_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        LOGGER.info("Wrote %s records to %s.", len(leads), path)


def write_datasift_csv(leads: list[LeadRecord]) -> None:
    rows = datasift_rows(leads)
    fieldnames = [
        "First Name",
        "Last Name",
        "Mailing Address",
        "Mailing City",
        "Mailing State",
        "Mailing Zip",
        "Property Address",
        "Property City",
        "Property State",
        "Property Zip",
        "List Type",
        "Document Type",
        "Date Filed",
        "Document Number",
        "Amount/Debt Owed",
        "Seller Score",
        "Motivated Seller Flags",
        "Source",
        "Public Records URL",
    ]
    for path in DATASIFT_CSV_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        LOGGER.info("Wrote DataSift CSV to %s.", path)


def seed_empty_outputs() -> None:
    today = date.today()
    start = today - timedelta(days=LOOKBACK_DAYS)
    write_json_outputs([], start, today)
    write_datasift_csv([])


async def main() -> None:
    configure_logging()
    today = date.today()
    start_date = today - timedelta(days=LOOKBACK_DAYS)
    LOGGER.info("Fetching Hernando County leads from %s through %s.", start_date, today)
    try:
        leads = await fetch_clerk_records(start_date, today)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Clerk fetch failed; writing empty output instead of crashing: %s", exc)
        leads = []
    enrich_leads_with_property_data(leads)
    for lead in leads:
        score_lead(lead, today)
    apply_combo_bonus(leads)
    write_json_outputs(leads, start_date, today)
    write_datasift_csv(leads)


if __name__ == "__main__":
    asyncio.run(main())
