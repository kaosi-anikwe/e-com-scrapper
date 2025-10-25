# scripts/amazon_scraper.py
"""
Standalone Amazon scraper script that uses the Apify SDK wrapper.
Provides:
  - run(categories, ...)  -> callable from other modules (returns run object)
  - CLI to run directly:
      python scripts/amazon_scraper.py --categories-file /path/to/categories.txt
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from urllib.parse import quote_plus

from config.settings import settings
from utils.logger import get_logger
from utils.apify_client import run_actor_and_save


# Default actor key as used in utils.apify_client.run_actor_and_save
ACTOR_KEY = "amazon"

logger = get_logger(ACTOR_KEY)


def _build_amazon_search_url(query: str) -> str:
    """Return an Amazon search URL for a given query string."""
    q = quote_plus(query.strip())
    return f"https://www.amazon.com/s?k={q}"


def _normalize_categories(categories: Iterable[str]) -> List[str]:
    return [c.strip() for c in categories if c and c.strip()]


def run(
    categories: Iterable[str],
    *,
    max_items_per_start: int = 100,
    max_search_pages_per_start_url: int = 9999,
    use_captcha_solver: bool = False,
    scrape_product_details: bool = True,
    proxy_country: str = "AUTO_SELECT_PROXY_COUNTRY",
) -> dict:
    """
    Kick off the Amazon actor with a search URL for each category.

    Parameters:
      - categories: iterable of human-readable category strings
      - max_items_per_start: actor input param mapped from sample usage
      - max_search_pages_per_start_url: actor input param
      - use_captcha_solver: actor input param
      - scrape_product_details: actor input param
      - proxy_country: actor input param
      - output_dir: where to save the run metadata and dataset (defaults to settings.DATA_DIR)

    Returns:
      Apify run object (dict) returned by run_actor_and_save (may be blocking).
    """
    cats = _normalize_categories(categories)
    if not cats:
        raise ValueError("No categories provided to run()")

    start_urls = [{"url": _build_amazon_search_url(c)} for c in cats]

    run_input = {
        "categoryOrProductUrls": start_urls,
        "maxItemsPerStartUrl": int(max_items_per_start),
        "language": "en",
        "proxyCountry": proxy_country,
        "maxSearchPagesPerStartUrl": int(max_search_pages_per_start_url),
        "maxOffers": 0,
        "scrapeSellers": False,
        "ensureLoadedProductDescriptionFields": False,
        "useCaptchaSolver": bool(use_captcha_solver),
        "scrapeProductVariantPrices": False,
        "scrapeProductDetails": bool(scrape_product_details),
        "countryCode": "US",
        "zipCode": None,
        "locationDeliverableRoutes": [
            "PRODUCT",
            "SEARCH",
            "OFFERS",
        ],
    }

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(settings.DATA_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_meta_path = out_dir / f"amazon_run_{ts}.json"

    logger.info(
        "Prepared Amazon actor run",
        extra={"start_url_count": len(start_urls), "run_meta_path": str(run_meta_path)},
    )

    # Use helper to start actor and save run + dataset
    run_obj = run_actor_and_save(
        actor_key=ACTOR_KEY,
        input_=run_input,
    )

    logger.info(
        "Amazon actor run finished (or started)",
        extra={"run_meta": run_obj.get("id") or run_obj.get("data", {}).get("id")},
    )

    return run_obj


def _read_categories_from_file(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Categories file not found: {path}")
    with p.open("r", encoding="utf-8") as fh:
        lines = [ln.strip() for ln in fh if ln.strip()]
    return lines


def _parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Amazon Apify actor for a list of categories."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--categories",
        help="Comma-separated categories (e.g. 'ginger,turmeric')",
        type=str,
    )
    group.add_argument(
        "--categories-file", help="Path to file with one category per line", type=str
    )
    parser.add_argument("--max-items-per-start", type=int, default=100)
    parser.add_argument(
        "--wait",
        dest="wait",
        action="store_true",
        help="Wait for actor to finish (default)",
    )
    parser.add_argument(
        "--no-wait",
        dest="wait",
        action="store_false",
        help="Start actor and don't wait for completion",
    )
    parser.set_defaults(wait=True)
    parser.add_argument(
        "--output-dir",
        default=str(settings.DATA_DIR),
        help="Directory to save run metadata and dataset",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_args()
    if args.categories:
        categories_list = [c.strip() for c in args.categories.split(",") if c.strip()]
    else:
        categories_list = _read_categories_from_file(args.categories_file)

    try:
        run(categories_list, max_items_per_start=args.max_items_per_start)
        logger.info("Script completed.")
    except Exception as exc:
        logger.exception("Script failed", extra={"error": str(exc)})
        raise
