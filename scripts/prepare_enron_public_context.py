from __future__ import annotations

import hashlib
import json
import re
import requests
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "vei" / "whatif" / "fixtures" / "enron_public_context"
RAW_ROOT = FIXTURE_ROOT / "raw"
PACKAGE_PATH = FIXTURE_ROOT / "package.json"
DATASET_PATH = FIXTURE_ROOT / "enron_public_context_v2.json"
README_PATH = FIXTURE_ROOT / "README.md"
STOCK_FIXTURE_ROOT = ROOT / "vei" / "whatif" / "fixtures" / "enron_stock_history"
STOCK_DATASET_PATH = STOCK_FIXTURE_ROOT / "enron_stock_history_v1.json"
CREDIT_FIXTURE_ROOT = ROOT / "vei" / "whatif" / "fixtures" / "enron_credit_history"
CREDIT_DATASET_PATH = CREDIT_FIXTURE_ROOT / "enron_credit_history_v1.json"
FERC_FIXTURE_ROOT = ROOT / "vei" / "whatif" / "fixtures" / "enron_ferc_history"
FERC_DATASET_PATH = FERC_FIXTURE_ROOT / "enron_ferc_history_v1.json"
USER_AGENT = "Mozilla/5.0 vei-enron-public-context-builder contact@example.com"
_STOCK_ROW_PATTERN = re.compile(
    r"^(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(?P<open>\d+\.\d+)\s+"
    r"(?P<high>\d+\.\d+)\s+"
    r"(?P<low>\d+\.\d+)\s+"
    r"(?P<close>\d+\.\d+)\s+"
    r"(?P<volume>[0-9,]+)"
)
_SCRIPT_BLOCK_PATTERN = re.compile(
    r"<script\b[^>]*>.*?</script>",
    re.IGNORECASE | re.DOTALL,
)
_PICTURE_BLOCK_PATTERN = re.compile(
    r"<picture\b[^>]*>.*?</picture>",
    re.IGNORECASE | re.DOTALL,
)
_IMG_TAG_PATTERN = re.compile(r"<img\b[^>]*>", re.IGNORECASE)
_HTML_ATTR_VALUE_PATTERN = re.compile(
    r"""\s(?:href|src|srcset|poster|content|data-[\w-]+)=["'][^"']*["']""",
    re.IGNORECASE,
)
_STOCK_HISTORY_START = date(1998, 1, 1)
_STOCK_HISTORY_END = date(2001, 12, 31)


@dataclass(frozen=True)
class SourceDocument:
    source_id: str
    filename: str
    url: str
    file_type: str
    description: str
    topics: tuple[str, ...]


SOURCE_DOCUMENTS: tuple[SourceDocument, ...] = (
    SourceDocument(
        source_id="enron_2000_financial_highlights",
        filename="2000_financial_highlights.pdf",
        url="https://enroncorp.com/src/doc/investors/2000_Financial_Highlights2.pdf",
        file_type="pdf",
        description="Archived Enron financial highlights with selected multi-year figures.",
        topics=("financials", "annual"),
    ),
    SourceDocument(
        source_id="enron_2000_form_10k",
        filename="2000_form_10k.pdf",
        url="https://enroncorp.com/src/doc/investors/2001-04-02-10-k.pdf",
        file_type="pdf",
        description="Enron Form 10-K for the fiscal year ended December 31, 2000.",
        topics=("financials", "annual", "filing"),
    ),
    SourceDocument(
        source_id="enron_2000_q4_1999_release",
        filename="2000_q4_1999_earnings_release.html",
        url="https://enroncorp.com/corp/pressroom/releases/2000/ene/4qenet",
        file_type="html",
        description="Archived fourth-quarter 1999 Enron earnings release.",
        topics=("financials", "quarterly"),
    ),
    SourceDocument(
        source_id="enron_2000_q1_release",
        filename="2000_q1_earnings_release.html",
        url=(
            "https://enroncorp.com/corp/pressroom/releases/2000/ene/"
            "1q00release-4-11-final"
        ),
        file_type="html",
        description="Archived first-quarter 2000 Enron earnings release.",
        topics=("financials", "quarterly"),
    ),
    SourceDocument(
        source_id="enron_2000_q2_release",
        filename="2000_q2_earnings_release.html",
        url="https://enroncorp.com/corp/pressroom/releases/2000/ene/2qearnreal-jul20",
        file_type="html",
        description="Archived second-quarter 2000 Enron earnings release.",
        topics=("financials", "quarterly"),
    ),
    SourceDocument(
        source_id="enron_historical_stock_price_pdf",
        filename="2001_enron_historical_stock_price.pdf",
        url=(
            "https://www.famous-trials.com/images/ftrials/Enron/documents/"
            "enronstockchart.pdf"
        ),
        file_type="pdf",
        description=(
            "Archived daily Enron historical stock price and volume table spanning "
            "1998 through 2001."
        ),
        topics=("financials", "stock_price", "market_history", "daily_prices"),
    ),
    SourceDocument(
        source_id="enron_2000_q3_release",
        filename="2000_q3_earnings_release.html",
        url="https://enroncorp.com/corp/pressroom/releases/2000/ene/3q-final",
        file_type="html",
        description="Archived third-quarter 2000 Enron earnings release.",
        topics=("financials", "quarterly"),
    ),
    SourceDocument(
        source_id="enron_2000_emissions_auction",
        filename="2000_enrononline_emissions_auction.html",
        url=(
            "https://enroncorp.com/corp/pressroom/releases/2000/ene/"
            "enrononline_auction_press_release"
        ),
        file_type="html",
        description="Archived EnronOnline emissions auction launch release.",
        topics=("news", "market_launch", "enrononline"),
    ),
    SourceDocument(
        source_id="enron_2000_ibm_agreement",
        filename="2000_ibm_energy_services_agreement.html",
        url="https://enroncorp.com/corp/pressroom/releases/2000/ene/ibmreleaseversion2",
        file_type="html",
        description="Archived Enron Energy Services agreement with IBM.",
        topics=("news", "commercial_agreement", "energy_services"),
    ),
    SourceDocument(
        source_id="enron_2000_mg_offer",
        filename="2000_mg_plc_offer.html",
        url="https://enroncorp.com/corp/pressroom/releases/2000/ene/merlinrelease",
        file_type="html",
        description="Archived Enron offer for MG plc.",
        topics=("news", "acquisition_offer", "metals"),
    ),
    SourceDocument(
        source_id="enron_2000_enrononline_milestone",
        filename="2000_enrononline_50b_milestone.html",
        url=(
            "https://enroncorp.com/corp/pressroom/releases/2000/ene/"
            "49-enrononlinemilestone"
        ),
        file_type="html",
        description="Archived EnronOnline $50B transaction milestone release.",
        topics=("news", "platform_growth", "enrononline"),
    ),
    SourceDocument(
        source_id="enron_2000_blockbuster_launch",
        filename="2000_blockbuster_on_demand_launch.html",
        url="https://enroncorp.com/corp/pressroom/releases/2000/ec/68-blockbuster",
        file_type="html",
        description="Archived Enron and Blockbuster on-demand launch release.",
        topics=("news", "product_launch", "broadband"),
    ),
    SourceDocument(
        source_id="enron_2000_clickpaper_launch",
        filename="2000_clickpaper_launch.html",
        url="https://enroncorp.com/corp/pressroom/releases/2000/ene/clickpaper.com",
        file_type="html",
        description="Archived Clickpaper.com launch release.",
        topics=("news", "product_launch", "industrial_markets"),
    ),
    SourceDocument(
        source_id="enron_2001_press_chronology",
        filename="2001_press_chronology.html",
        url="https://enroncorp.com/corp/pressroom/releases/2001/press_chron2001",
        file_type="html",
        description="Archived 2001 Enron press release chronology.",
        topics=("news", "timeline"),
    ),
    SourceDocument(
        source_id="enron_2001_q1_release",
        filename="2001_q1_earnings_release.html",
        url="https://enroncorp.com/corp/pressroom/releases/2001/ene/ene-q1-01-ltr",
        file_type="html",
        description="Archived first-quarter 2001 Enron earnings release.",
        topics=("financials", "quarterly"),
    ),
    SourceDocument(
        source_id="enron_2001_q2_release",
        filename="2001_q2_earnings_release.html",
        url=(
            "https://enroncorp.com/corp/pressroom/releases/2001/ene/"
            "51-enronsecondquarterrelease-07-12-01-ltr"
        ),
        file_type="html",
        description="Archived second-quarter 2001 Enron earnings release.",
        topics=("financials", "quarterly"),
    ),
    SourceDocument(
        source_id="pge_2001_chapter11_record",
        filename="2001_pge_chapter11_record.html",
        url=(
            "https://www.sec.gov/Archives/edgar/data/1004980/"
            "000095014904000430/f95893ae10vk.htm"
        ),
        file_type="html",
        description="SEC filing that records PG&E's April 6, 2001 Chapter 11 filing.",
        topics=("news", "bankruptcy", "counterparty"),
    ),
    SourceDocument(
        source_id="california_refund_order_record",
        filename="2001_california_refund_hearing.html",
        url="https://seuc.senate.ca.gov/june-19-2001-hearing-information",
        file_type="html",
        description=(
            "California Senate hearing page summarizing the June 19, 2001 FERC "
            "refund and mitigation action."
        ),
        topics=("news", "regulatory", "ferc", "california_crisis"),
    ),
    SourceDocument(
        source_id="skilling_resignation_release",
        filename="2001_skilling_resignation.pdf",
        url=(
            "https://www.justice.gov/archive/enron/exhibit/02-02/BBC-0001/Images/"
            "EXH023-00160.PDF"
        ),
        file_type="pdf",
        description="Archived Enron press release announcing Jeff Skilling's resignation.",
        topics=("news", "governance", "leadership"),
    ),
    SourceDocument(
        source_id="watkins_memo_public_release",
        filename="2002_watkins_memo_release.html",
        url="https://www.latimes.com/archives/la-xpm-2002-jan-16-mn-22968-story.html",
        file_type="html",
        description=(
            "Public press coverage from January 16, 2002 when the full Watkins memo "
            "text became public."
        ),
        topics=("news", "whistleblower", "accounting"),
    ),
    SourceDocument(
        source_id="arthur_andersen_indictment",
        filename="2002_arthur_andersen_indictment.html",
        url=(
            "https://www.justice.gov/archive/dag/speeches/2002/"
            "031402newsconferncearthurandersen.htm"
        ),
        file_type="html",
        description="DOJ announcement and transcript for the Arthur Andersen indictment.",
        topics=("news", "auditor", "indictment"),
    ),
    SourceDocument(
        source_id="enron_credit_rating_report",
        filename="2002_enron_credit_rating_report.pdf",
        url=(
            "https://www.govinfo.gov/content/pkg/CPRT-107SPRT80604/pdf/"
            "CPRT-107SPRT80604.pdf"
        ),
        file_type="pdf",
        description=(
            "Senate committee print on Enron and private-sector watchdogs with "
            "the credit-rating action timeline."
        ),
        topics=("news", "credit", "ratings", "senate_report"),
    ),
    SourceDocument(
        source_id="pge_2001_q3_10q_note",
        filename="pge_2001_q3_10q.html",
        url=(
            "https://www.sec.gov/Archives/edgar/data/784977/"
            "000078497701500017/q10qtr3.htm"
        ),
        file_type="html",
        description=(
            "SEC-filed Portland General Electric 10-Q with dated notes on Enron's "
            "October and November 2001 disclosures."
        ),
        topics=("news", "financials", "regulatory"),
    ),
    SourceDocument(
        source_id="sec_enron_chapter11_record",
        filename="sec_enron_chapter11_record.html",
        url="https://www.sec.gov/file/35-27810",
        file_type="html",
        description="SEC proceeding record for the Enron Chapter 11 case.",
        topics=("news", "bankruptcy"),
    ),
)


def main() -> None:
    FIXTURE_ROOT.mkdir(parents=True, exist_ok=True)
    RAW_ROOT.mkdir(parents=True, exist_ok=True)

    collected_at = _timestamp_now()
    downloaded_files = _download_sources()
    _write_package_manifest(
        collected_at=collected_at, downloaded_files=downloaded_files
    )
    _write_dataset(collected_at=collected_at)
    _write_readme()
    _write_stock_history_fixture(collected_at=collected_at)
    _write_credit_history_fixture(collected_at=collected_at)
    _write_ferc_history_fixture(collected_at=collected_at)

    print(f"wrote fixture: {FIXTURE_ROOT}")
    print(f"wrote manifest: {PACKAGE_PATH}")
    print(f"wrote dataset: {DATASET_PATH}")
    print(f"wrote stock history: {STOCK_DATASET_PATH}")
    print(f"wrote credit history: {CREDIT_DATASET_PATH}")
    print(f"wrote ferc history: {FERC_DATASET_PATH}")


def _download_sources() -> dict[str, dict[str, Any]]:
    downloads: dict[str, dict[str, Any]] = {}
    for source in SOURCE_DOCUMENTS:
        target = RAW_ROOT / source.filename
        payload = _fetch_bytes(source.url)
        payload = _sanitize_payload(source=source, payload=payload)
        target.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        downloads[source.source_id] = {
            "relative_path": str(target.relative_to(FIXTURE_ROOT)),
            "content_digest_prefix": digest[:8],
            "size_bytes": len(payload),
        }
    return downloads


def _fetch_bytes(url: str) -> bytes:
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"unsupported URL scheme for public source fetch: {url}")
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
    response.raise_for_status()
    return response.content


def _sanitize_payload(*, source: SourceDocument, payload: bytes) -> bytes:
    if source.file_type != "html":
        return payload
    text = payload.decode("utf-8", errors="ignore")
    cleaned = _SCRIPT_BLOCK_PATTERN.sub("", text)
    cleaned = _PICTURE_BLOCK_PATTERN.sub("", cleaned)
    cleaned = _IMG_TAG_PATTERN.sub("", cleaned)
    cleaned = _HTML_ATTR_VALUE_PATTERN.sub("", cleaned)
    return cleaned.encode("utf-8")


def _write_package_manifest(
    *,
    collected_at: str,
    downloaded_files: dict[str, dict[str, Any]],
) -> None:
    sources: list[dict[str, Any]] = []
    for source in SOURCE_DOCUMENTS:
        downloaded = downloaded_files[source.source_id]
        sources.append(
            {
                "source_id": source.source_id,
                "source_system": "public_archive",
                "file_type": source.file_type,
                "relative_path": downloaded["relative_path"],
                "collected_at": collected_at,
                "redaction_status": "public_source",
                "source_url": source.url,
                "description": source.description,
                "topics": list(source.topics),
                "content_digest_algorithm": "sha256_prefix8",
                "content_digest_prefix": downloaded["content_digest_prefix"],
                "size_bytes": downloaded["size_bytes"],
            }
        )

    package_payload = {
        "version": "2",
        "name": "enron_public_context",
        "title": "Enron Public Context Pack",
        "description": (
            "Archived public financial checkpoints and public-news timeline data for "
            "Enron. Intended to be filtered against the oldest and latest email dates "
            "in an Enron historical world."
        ),
        "organization_name": "Enron Corporation",
        "organization_domain": "enron.com",
        "timezone": "America/Chicago",
        "sources": sources,
        "artifacts": [
            {
                "artifact_id": "enron_public_context_v2",
                "version": "2",
                "file_type": "json",
                "relative_path": DATASET_PATH.name,
                "description": (
                    "Normalized dated financial checkpoints and public-news events for "
                    "later slicing by email time window."
                ),
            }
        ],
        "metadata": {
            "fixture": True,
            "prepared_at": collected_at,
            "notes": [
                (
                    "This pack keeps raw downloaded source files beside a normalized "
                    "JSON bundle so later integration can stay deterministic."
                )
            ],
        },
    }
    PACKAGE_PATH.write_text(
        json.dumps(package_payload, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def _write_dataset(*, collected_at: str) -> None:
    financial_snapshots = sorted(
        _financial_snapshots(),
        key=lambda item: (item["as_of"], item["snapshot_id"]),
    )
    public_news_events = sorted(
        _public_news_events(),
        key=lambda item: (item["timestamp"], item["event_id"]),
    )
    dataset_payload = {
        "version": "2",
        "pack_name": "enron_public_context",
        "organization_name": "Enron Corporation",
        "organization_domain": "enron.com",
        "prepared_at": collected_at,
        "integration_hint": (
            "At integration time, compute the oldest and latest email timestamps from "
            "the active Enron slice and keep only financial_snapshots and "
            "public_news_events whose dates overlap that email window."
        ),
        "financial_snapshots": financial_snapshots,
        "public_news_events": public_news_events,
    }
    DATASET_PATH.write_text(
        json.dumps(dataset_payload, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def _financial_snapshots() -> list[dict[str, Any]]:
    return [
        {
            "snapshot_id": "fy1998_selected_financial_data",
            "as_of": "1998-12-31T00:00:00Z",
            "kind": "annual",
            "label": "FY1998 selected financial data",
            "source_ids": ["enron_2000_financial_highlights"],
            "summary": (
                "Operating revenue was $31.26B, net income was $703M, total assets "
                "were $29.35B, shareholders' equity was $7.05B, and diluted EPS was "
                "$1.01."
            ),
            "metrics": {
                "operating_revenue_usd_millions": 31260,
                "net_income_usd_millions": 703,
                "total_assets_usd_millions": 29350,
                "shareholders_equity_usd_millions": 7048,
                "diluted_eps": 1.01,
            },
        },
        {
            "snapshot_id": "fy1999_selected_financial_data",
            "as_of": "1999-12-31T00:00:00Z",
            "kind": "annual",
            "label": "FY1999 selected financial data",
            "source_ids": ["enron_2000_financial_highlights"],
            "summary": (
                "Operating revenue was $40.11B, net income was $893M, total assets "
                "were $33.38B, shareholders' equity was $9.57B, and diluted EPS was "
                "$1.10."
            ),
            "metrics": {
                "operating_revenue_usd_millions": 40112,
                "net_income_usd_millions": 893,
                "total_assets_usd_millions": 33381,
                "shareholders_equity_usd_millions": 9570,
                "diluted_eps": 1.10,
            },
        },
        {
            "snapshot_id": "q4_1999_annual_release",
            "as_of": "2000-01-18T00:00:00Z",
            "kind": "quarterly_release",
            "label": "Q4 1999 annual earnings release",
            "source_ids": ["enron_2000_q4_1999_release"],
            "summary": (
                "Enron reported full-year 1999 revenues of $40.0B, net income of "
                "$957M before nonrecurring items, annual diluted EPS of $1.18, and "
                "fourth-quarter diluted EPS of $0.31."
            ),
            "metrics": {
                "annual_revenue_usd_millions": 40000,
                "annual_net_income_usd_millions": 957,
                "annual_diluted_eps": 1.18,
                "fourth_quarter_diluted_eps": 0.31,
            },
        },
        {
            "snapshot_id": "q1_2000_earnings_release",
            "as_of": "2000-04-12T00:00:00Z",
            "kind": "quarterly_release",
            "label": "Q1 2000 earnings release",
            "source_ids": ["enron_2000_q1_release"],
            "summary": (
                "Enron reported first-quarter revenues of $13.1B, net income of "
                "$338M, and diluted EPS of $0.40."
            ),
            "metrics": {
                "revenue_usd_millions": 13100,
                "net_income_usd_millions": 338,
                "diluted_eps": 0.40,
            },
        },
        {
            "snapshot_id": "q2_2000_earnings_release",
            "as_of": "2000-07-24T00:00:00Z",
            "kind": "quarterly_release",
            "label": "Q2 2000 earnings release",
            "source_ids": ["enron_2000_q2_release"],
            "summary": (
                "Enron reported second-quarter revenues of $16.9B, net income of "
                "$289M, and diluted EPS of $0.34."
            ),
            "metrics": {
                "revenue_usd_millions": 16900,
                "net_income_usd_millions": 289,
                "diluted_eps": 0.34,
            },
        },
        {
            "snapshot_id": "q3_2000_earnings_release",
            "as_of": "2000-10-17T00:00:00Z",
            "kind": "quarterly_release",
            "label": "Q3 2000 earnings release",
            "source_ids": ["enron_2000_q3_release"],
            "summary": (
                "Enron reported third-quarter revenues of $30.0B, net income of "
                "$292M, and diluted EPS of $0.34."
            ),
            "metrics": {
                "revenue_usd_millions": 30000,
                "net_income_usd_millions": 292,
                "diluted_eps": 0.34,
            },
        },
        {
            "snapshot_id": "fy2000_10k",
            "as_of": "2000-12-31T00:00:00Z",
            "kind": "annual",
            "label": "FY2000 Form 10-K",
            "source_ids": [
                "enron_2000_financial_highlights",
                "enron_2000_form_10k",
            ],
            "summary": (
                "Operating revenue was $100.79B, net income was $979M, total assets "
                "were $65.50B, shareholders' equity was $11.47B, and diluted EPS was "
                "$1.12."
            ),
            "metrics": {
                "operating_revenue_usd_millions": 100789,
                "net_income_usd_millions": 979,
                "total_assets_usd_millions": 65503,
                "shareholders_equity_usd_millions": 11470,
                "diluted_eps": 1.12,
            },
        },
        {
            "snapshot_id": "q4_2000_annual_release",
            "as_of": "2001-01-22T00:00:00Z",
            "kind": "quarterly_release",
            "label": "Q4 2000 annual earnings release",
            "source_ids": ["enron_2001_press_chronology"],
            "summary": (
                "Enron publicly reported recurring annual diluted EPS of $1.47 for "
                "2000 and fourth-quarter diluted EPS of $0.41."
            ),
            "metrics": {
                "recurring_annual_diluted_eps": 1.47,
                "fourth_quarter_diluted_eps": 0.41,
            },
        },
        {
            "snapshot_id": "q1_2001_earnings_release",
            "as_of": "2001-04-17T00:00:00Z",
            "kind": "quarterly_release",
            "label": "Q1 2001 earnings release",
            "source_ids": ["enron_2001_q1_release"],
            "summary": (
                "Enron publicly reported first-quarter diluted EPS of $0.47, "
                "revenues of $50.1B, and net income of $406M."
            ),
            "metrics": {
                "revenue_usd_millions": 50100,
                "net_income_usd_millions": 406,
                "diluted_eps": 0.47,
            },
        },
        {
            "snapshot_id": "q2_2001_earnings_release",
            "as_of": "2001-07-12T00:00:00Z",
            "kind": "quarterly_release",
            "label": "Q2 2001 earnings release",
            "source_ids": ["enron_2001_q2_release"],
            "summary": (
                "Enron publicly reported second-quarter diluted EPS of $0.45 and net "
                "income of $404M."
            ),
            "metrics": {
                "net_income_usd_millions": 404,
                "diluted_eps": 0.45,
            },
        },
        {
            "snapshot_id": "q3_2001_loss_announcement",
            "as_of": "2001-10-16T00:00:00Z",
            "kind": "event_checkpoint",
            "label": "Q3 2001 loss announcement",
            "source_ids": ["pge_2001_q3_10q_note"],
            "summary": (
                "Enron announced $1.01B of after-tax non-recurring charges, which "
                "resulted in a net loss for the quarter."
            ),
            "metrics": {
                "after_tax_non_recurring_charges_usd_millions": 1010,
            },
        },
    ]


def _public_news_events() -> list[dict[str, Any]]:
    return [
        {
            "event_id": "enrononline_emissions_auction_launch",
            "timestamp": "2000-03-09T00:00:00Z",
            "category": "market_launch",
            "headline": "Enron launched its first online emissions allowance auction",
            "summary": (
                "Enron said EnronOnline would host the first online sulfur dioxide "
                "emissions allowance auction and planned to run the auctions monthly."
            ),
            "source_ids": ["enron_2000_emissions_auction"],
        },
        {
            "event_id": "ibm_energy_services_agreement",
            "timestamp": "2000-04-11T00:00:00Z",
            "category": "commercial_agreement",
            "headline": "Enron signed a $610M long-term electricity agreement with IBM",
            "summary": (
                "Enron Energy Services said it would supply or procure electricity "
                "for IBM under a ten-year agreement valued at $610M."
            ),
            "source_ids": ["enron_2000_ibm_agreement"],
        },
        {
            "event_id": "mg_plc_cash_offer",
            "timestamp": "2000-05-22T00:00:00Z",
            "category": "acquisition_offer",
            "headline": "Enron announced a cash offer for MG plc",
            "summary": (
                "Enron said its recommended cash offer valued MG plc at $446M and "
                "would extend Enron's business model into metals."
            ),
            "source_ids": ["enron_2000_mg_offer"],
        },
        {
            "event_id": "enrononline_fifty_billion_milestone",
            "timestamp": "2000-06-01T00:00:00Z",
            "category": "platform_growth",
            "headline": "EnronOnline passed $50B of transaction value in 2000",
            "summary": (
                "Enron said EnronOnline had exceeded $50B of transaction value in "
                "calendar 2000 and had already beaten its yearly projection."
            ),
            "source_ids": ["enron_2000_enrononline_milestone"],
        },
        {
            "event_id": "blockbuster_on_demand_launch",
            "timestamp": "2000-07-19T00:00:00Z",
            "category": "product_launch",
            "headline": "Enron and Blockbuster announced a 20-year on-demand service deal",
            "summary": (
                "Enron Broadband Services and Blockbuster announced a 20-year "
                "exclusive agreement to launch an entertainment-on-demand service."
            ),
            "source_ids": ["enron_2000_blockbuster_launch"],
        },
        {
            "event_id": "clickpaper_launch",
            "timestamp": "2000-09-05T00:00:00Z",
            "category": "product_launch",
            "headline": "Enron launched Clickpaper.com for the forest products market",
            "summary": (
                "Enron Industrial Markets launched Clickpaper.com as an Internet "
                "transaction system for pulp, paper, and wood commodities."
            ),
            "source_ids": ["enron_2000_clickpaper_launch"],
        },
        {
            "event_id": "cliff_baxter_resignation",
            "timestamp": "2001-05-02T00:00:00Z",
            "category": "governance",
            "headline": "Vice chairman Cliff Baxter resigned",
            "summary": (
                "Enron's archived 2001 press chronology records Cliff Baxter's "
                "resignation on May 2, 2001."
            ),
            "source_ids": ["enron_2001_press_chronology"],
        },
        {
            "event_id": "pge_chapter_11",
            "timestamp": "2001-04-06T00:00:00Z",
            "category": "counterparty_bankruptcy",
            "headline": "PG&E entered Chapter 11",
            "summary": (
                "PG&E's Chapter 11 filing made California power-counterparty risk a "
                "public fact before later Enron branch points tied to power trading "
                "and structured deals."
            ),
            "source_ids": ["pge_2001_chapter11_record"],
        },
        {
            "event_id": "ferc_western_refund_order",
            "timestamp": "2001-06-19T00:00:00Z",
            "category": "regulatory",
            "headline": "FERC imposed mitigation and opened the California refund path",
            "summary": (
                "Public reporting on the June 19, 2001 FERC action put Enron and "
                "other sellers on notice that California crisis sales would face a "
                "refund and mitigation regime."
            ),
            "source_ids": ["california_refund_order_record"],
        },
        {
            "event_id": "skilling_resignation",
            "timestamp": "2001-08-14T00:00:00Z",
            "category": "governance",
            "headline": "Jeff Skilling resigned as chief executive",
            "summary": (
                "Enron announced Skilling's resignation for personal reasons and "
                "said Kenneth Lay would resume the president and chief executive role."
            ),
            "source_ids": ["skilling_resignation_release"],
        },
        {
            "event_id": "third_quarter_loss",
            "timestamp": "2001-10-16T00:00:00Z",
            "category": "financial_disclosure",
            "headline": "Enron announced a third-quarter loss",
            "summary": (
                "The company disclosed $1.01B in after-tax non-recurring charges and "
                "said the quarter ended in a net loss."
            ),
            "source_ids": ["pge_2001_q3_10q_note"],
        },
        {
            "event_id": "sec_information_request",
            "timestamp": "2001-10-22T00:00:00Z",
            "category": "regulatory",
            "headline": "SEC requested information about related-party transactions",
            "summary": (
                "Enron said the SEC had asked it to voluntarily provide information "
                "about certain related-party transactions."
            ),
            "source_ids": ["pge_2001_q3_10q_note"],
        },
        {
            "event_id": "credit_watch_negative",
            "timestamp": "2001-10-29T00:00:00Z",
            "category": "credit",
            "headline": "Moody's kept Enron under review for downgrade",
            "summary": (
                "Public credit-rating reporting showed Enron's investment-grade "
                "status was under acute pressure as liquidity concerns deepened."
            ),
            "source_ids": ["enron_credit_rating_report"],
        },
        {
            "event_id": "sp_rating_lowered_creditwatch_negative",
            "timestamp": "2001-11-01T00:00:00Z",
            "category": "credit",
            "headline": "S&P lowered Enron and kept it on CreditWatch negative",
            "summary": (
                "S&P cut Enron's rating while leaving it just above junk, making "
                "the company's rating threshold a visible public constraint."
            ),
            "source_ids": ["enron_credit_rating_report"],
        },
        {
            "event_id": "special_committee_and_formal_investigation",
            "timestamp": "2001-10-31T00:00:00Z",
            "category": "regulatory",
            "headline": "Board formed a special committee and the SEC opened a formal investigation",
            "summary": (
                "Enron said its board appointed a special committee and that the SEC "
                "had opened a formal investigation into the matters under review."
            ),
            "source_ids": ["pge_2001_q3_10q_note"],
        },
        {
            "event_id": "moodys_and_sp_cut_to_junk",
            "timestamp": "2001-11-28T00:00:00Z",
            "category": "credit",
            "headline": "Moody's and S&P cut Enron to junk",
            "summary": (
                "The final investment-grade break made clear that Enron's trading "
                "model could not survive a junk rating."
            ),
            "source_ids": ["enron_credit_rating_report"],
        },
        {
            "event_id": "restatement_of_prior_financials",
            "timestamp": "2001-11-08T00:00:00Z",
            "category": "restatement",
            "headline": "Enron said it would restate prior financial statements",
            "summary": (
                "Enron said it would restate results from 1997 through 2000 and the "
                "first two quarters of 2001 and reflect a $1.2B reduction in "
                "shareholders' equity."
            ),
            "source_ids": ["pge_2001_q3_10q_note"],
        },
        {
            "event_id": "dynegy_merger_announcement",
            "timestamp": "2001-11-09T00:00:00Z",
            "category": "merger",
            "headline": "Enron announced a proposed sale to Dynegy",
            "summary": (
                "Enron reported that Dynegy would acquire the company, subject to "
                "shareholder approval and other closing conditions."
            ),
            "source_ids": ["pge_2001_q3_10q_note"],
        },
        {
            "event_id": "chapter_11_filing",
            "timestamp": "2001-12-02T00:00:00Z",
            "category": "bankruptcy",
            "headline": "Enron entered Chapter 11 proceedings",
            "summary": (
                "The SEC proceeding record is used as the official source reference "
                "for Enron's Chapter 11 case."
            ),
            "source_ids": ["sec_enron_chapter11_record"],
        },
        {
            "event_id": "watkins_memo_public_release",
            "timestamp": "2002-01-15T00:00:00Z",
            "category": "whistleblower",
            "headline": "Sherron Watkins's memo became public",
            "summary": (
                "Congressional investigators' release put the full warning memo into "
                "the public record months after the internal August 2001 warning."
            ),
            "internally_known_date": "2001-08-22T00:00:00Z",
            "source_ids": ["watkins_memo_public_release"],
        },
        {
            "event_id": "arthur_andersen_indictment",
            "timestamp": "2002-03-14T00:00:00Z",
            "category": "indictment",
            "headline": "Arthur Andersen was indicted for obstruction",
            "summary": (
                "The Justice Department unsealed the indictment of Enron's auditor, "
                "turning the document-destruction scandal into a formal criminal case."
            ),
            "source_ids": ["arthur_andersen_indictment"],
        },
    ]


def _write_readme() -> None:
    financial_count = len(_financial_snapshots())
    news_count = len(_public_news_events())
    README_PATH.write_text(
        "\n".join(
            [
                "# Enron Public Context",
                "",
                "This fixture stores public-source material that can be joined to an Enron mail slice later.",
                "",
                "The current packaged fixture contains:",
                f"- {financial_count} dated financial checkpoints",
                f"- {news_count} dated public news events",
                f"- {len(SOURCE_DOCUMENTS)} archived public source files",
                "",
                "The public dates currently span December 31, 1998 through March 14, 2002.",
                "",
                "Contents:",
                "- `raw/`: downloaded public-source HTML and PDF files.",
                "- `package.json`: manifest describing the raw sources and normalized artifact.",
                "- `enron_public_context_v2.json`: normalized dated financial checkpoints and public-news checkpoints.",
                "",
                "Regenerate with:",
                "- `python scripts/prepare_enron_public_context.py`",
                "",
                "Integration rule:",
                "- Read the oldest and latest email timestamps from the active Enron dataset.",
                "- Keep only the public rows whose dates overlap that email window.",
                "- For a concrete branch point, keep only the rows whose dates are on or before the branch timestamp.",
                "- If the packaged fixture is missing or malformed, Enron mail loading still succeeds with an empty public-context slice.",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _timestamp_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _write_stock_history_fixture(*, collected_at: str) -> None:
    rows = _stock_history_rows()
    _write_derived_fixture(
        root=STOCK_FIXTURE_ROOT,
        dataset_path=STOCK_DATASET_PATH,
        key="stock_history",
        rows=rows,
        collected_at=collected_at,
        description=(
            "Daily Enron stock OHLCV history from January 1998 through "
            "December 2001 extracted from the archived historical-price PDF."
        ),
        source_ids=["enron_historical_stock_price_pdf"],
    )


def _write_credit_history_fixture(*, collected_at: str) -> None:
    rows = _credit_history_rows()
    _write_derived_fixture(
        root=CREDIT_FIXTURE_ROOT,
        dataset_path=CREDIT_DATASET_PATH,
        key="credit_history",
        rows=rows,
        collected_at=collected_at,
        description=(
            "Public Enron credit-rating checkpoints covering the baseline "
            "investment-grade posture and the October-November 2001 downgrade "
            "sequence."
        ),
        source_ids=[
            "enron_2000_financial_highlights",
            "enron_credit_rating_report",
        ],
    )


def _write_ferc_history_fixture(*, collected_at: str) -> None:
    rows = _ferc_history_rows()
    _write_derived_fixture(
        root=FERC_FIXTURE_ROOT,
        dataset_path=FERC_DATASET_PATH,
        key="ferc_history",
        rows=rows,
        collected_at=collected_at,
        description=(
            "Public FERC actions that framed the California power crisis and "
            "Enron's later regulatory exposure."
        ),
        source_ids=["california_refund_order_record"],
    )


def _write_derived_fixture(
    *,
    root: Path,
    dataset_path: Path,
    key: str,
    rows: list[dict[str, Any]],
    collected_at: str,
    description: str,
    source_ids: list[str],
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": "1",
        "pack_name": root.name,
        "organization_name": "Enron Corporation",
        "organization_domain": "enron.com",
        "prepared_at": collected_at,
        "integration_hint": (
            "This derived fixture is loaded automatically for repo-local Enron "
            "worlds and is sliced to the active email window."
        ),
        key: rows,
    }
    dataset_path.write_text(
        json.dumps(payload, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )
    package_payload = {
        "version": "1",
        "name": root.name,
        "title": root.name.replace("_", " ").title(),
        "description": description,
        "organization_name": "Enron Corporation",
        "organization_domain": "enron.com",
        "sources": [
            {
                "source_id": source_id,
                "relative_path": (
                    f"../enron_public_context/raw/{_source_document_map()[source_id].filename}"
                ),
                "description": _source_document_map()[source_id].description,
            }
            for source_id in source_ids
        ],
        "artifacts": [
            {
                "artifact_id": dataset_path.stem,
                "version": "1",
                "file_type": "json",
                "relative_path": dataset_path.name,
                "description": description,
            }
        ],
    }
    (root / "package.json").write_text(
        json.dumps(package_payload, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )
    (root / "README.md").write_text(
        "\n".join(
            [
                f"# {package_payload['title']}",
                "",
                description,
                "",
                f"Rows: {len(rows)}",
                "",
                "Regenerate with:",
                "- `python scripts/prepare_enron_public_context.py`",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _stock_history_rows() -> list[dict[str, Any]]:
    stock_source = (
        RAW_ROOT / _source_document_map()["enron_historical_stock_price_pdf"].filename
    )
    rows_by_date: dict[str, dict[str, Any]] = {}
    with tempfile.TemporaryDirectory() as temp_dir:
        output_pattern = str(Path(temp_dir) / "stock-page-%02d.jpg")
        subprocess.run(
            ["magick", "-density", "180", str(stock_source), output_pattern],
            check=True,
            capture_output=True,
        )
        for image_path in sorted(Path(temp_dir).glob("stock-page-*.jpg")):
            ocr_output = subprocess.run(
                ["/opt/homebrew/bin/tesseract", "stdin", "stdout", "--psm", "6"],
                input=image_path.read_bytes(),
                capture_output=True,
                check=True,
            ).stdout.decode("utf-8", errors="ignore")
            for line in ocr_output.splitlines():
                normalized = (
                    line.strip().replace("~", "").replace("=", " ").replace("  ", " ")
                )
                match = _STOCK_ROW_PATTERN.match(normalized)
                if match is None:
                    continue
                day = datetime.strptime(
                    match.group("date"),
                    "%m/%d/%Y",
                ).date()
                if day < _STOCK_HISTORY_START or day > _STOCK_HISTORY_END:
                    continue
                as_of = f"{day.isoformat()}T00:00:00Z"
                rows_by_date[as_of] = {
                    "as_of": as_of,
                    "open": float(match.group("open")),
                    "high": float(match.group("high")),
                    "low": float(match.group("low")),
                    "close": float(match.group("close")),
                    "volume": int(match.group("volume").replace(",", "")),
                    "label": "Daily trading close",
                    "summary": (
                        "Daily Enron trading data extracted from the archived "
                        "historical stock-price table."
                    ),
                    "source_ids": ["enron_historical_stock_price_pdf"],
                }
    return [rows_by_date[key] for key in sorted(rows_by_date)]


def _credit_history_rows() -> list[dict[str, Any]]:
    return [
        {
            "event_id": "moodys_baseline_investment_grade",
            "as_of": "2000-12-31T00:00:00Z",
            "agency": "Moody's",
            "category": "baseline",
            "headline": "Moody's baseline senior debt rating",
            "summary": "Enron's senior debt was listed at Baa1 in the 2000 financial highlights.",
            "to_rating": "Baa1",
            "source_ids": ["enron_2000_financial_highlights"],
        },
        {
            "event_id": "sp_baseline_investment_grade",
            "as_of": "2000-12-31T00:00:00Z",
            "agency": "S&P",
            "category": "baseline",
            "headline": "S&P baseline senior debt rating",
            "summary": "Enron's senior debt was listed at BBB+ in the 2000 financial highlights.",
            "to_rating": "BBB+",
            "source_ids": ["enron_2000_financial_highlights"],
        },
        {
            "event_id": "fitch_baseline_investment_grade",
            "as_of": "2000-12-31T00:00:00Z",
            "agency": "Fitch",
            "category": "baseline",
            "headline": "Fitch baseline senior debt rating",
            "summary": "Enron's senior debt was listed at BBB+ in the 2000 financial highlights.",
            "to_rating": "BBB+",
            "source_ids": ["enron_2000_financial_highlights"],
        },
        {
            "event_id": "moodys_baa2_review",
            "as_of": "2001-10-29T00:00:00Z",
            "agency": "Moody's",
            "category": "rating_action",
            "headline": "Moody's cut Enron to Baa2 and kept it under review",
            "summary": (
                "Moody's lowered Enron's senior unsecured debt to Baa2 and kept the "
                "rating under review for downgrade."
            ),
            "from_rating": "Baa1",
            "to_rating": "Baa2",
            "watch_status": "review_for_downgrade",
            "source_ids": ["enron_credit_rating_report"],
        },
        {
            "event_id": "sp_bbb_creditwatch_negative",
            "as_of": "2001-11-01T00:00:00Z",
            "agency": "S&P",
            "category": "rating_action",
            "headline": "S&P cut Enron to BBB and kept CreditWatch negative",
            "summary": (
                "S&P lowered Enron's rating from BBB+ to BBB and kept the name on "
                "CreditWatch negative."
            ),
            "from_rating": "BBB+",
            "to_rating": "BBB",
            "watch_status": "creditwatch_negative",
            "source_ids": ["enron_credit_rating_report"],
        },
        {
            "event_id": "fitch_bbb_minus_watch_negative",
            "as_of": "2001-11-05T00:00:00Z",
            "agency": "Fitch",
            "category": "rating_action",
            "headline": "Fitch cut Enron to BBB- and kept watch negative",
            "summary": (
                "Fitch lowered Enron's senior unsecured debt from BBB+ to BBB- and "
                "maintained a negative watch."
            ),
            "from_rating": "BBB+",
            "to_rating": "BBB-",
            "watch_status": "watch_negative",
            "source_ids": ["enron_credit_rating_report"],
        },
        {
            "event_id": "moodys_sp_cut_to_junk",
            "as_of": "2001-11-28T00:00:00Z",
            "agency": "Moody's_and_S&P",
            "category": "rating_action",
            "headline": "Moody's and S&P cut Enron below investment grade",
            "summary": (
                "By November 28, 2001, Moody's and S&P had cut Enron to junk, which "
                "triggered the liquidity spiral that preceded the bankruptcy filing."
            ),
            "to_rating": "below_investment_grade",
            "watch_status": "junk_status",
            "source_ids": ["enron_credit_rating_report"],
        },
    ]


def _ferc_history_rows() -> list[dict[str, Any]]:
    return [
        {
            "event_id": "ferc_june_2001_mitigation_and_refund_path",
            "timestamp": "2001-06-19T00:00:00Z",
            "agency": "FERC",
            "category": "mitigation_order",
            "headline": "FERC imposed Western mitigation and opened the refund path",
            "summary": (
                "The June 19, 2001 FERC action turned California power-market "
                "conduct and refund exposure into a concrete public regulatory fact."
            ),
            "source_ids": ["california_refund_order_record"],
        }
    ]


def _source_document_map() -> dict[str, SourceDocument]:
    return {source.source_id: source for source in SOURCE_DOCUMENTS}


if __name__ == "__main__":
    main()
