# scripts/ebay_scraper.py
"""
Standalone eBay scraper script that uses the project's Apify SDK wrapper (utils.apify_client.run_actor_and_save).

Behavior:
 - Accepts a list of categories (or raw URLs). For plain text categories it builds eBay search URLs:
     https://www.ebay.com/sch/i.html?_nkw=<url-encoded-query>
 - If an entry already starts with http/https it is treated as a startUrl (used as-is).
 - Builds actor input modeled on the sample usage and calls the actor via run_actor_and_save().
 - Saves run metadata and dataset into data/ebay/.
 - Exposes run(categories, ...) for programmatic use and provides a small CLI.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.parse import quote_plus

from utils.logger import get_logger
from config.settings import settings
from utils.apify_client import run_actor_and_save

logger = get_logger("ebay")


def _build_ebay_search_url(query: str) -> str:
    q = quote_plus(query.strip())
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&_sacat=0&_from=R40&_odkw={q}"


def _normalize_categories(categories: Iterable[str]) -> List[str]:
    return [str(c).strip() for c in categories if c and str(c).strip()]


def _categories_to_start_urls(categories: Iterable[str]) -> List[str]:
    start_urls: List[str] = []
    for c in _normalize_categories(categories):
        lower = c.lower()
        if lower.startswith("http://") or lower.startswith("https://"):
            start_urls.append(c)
        else:
            start_urls.append(_build_ebay_search_url(c))
    return start_urls


def run(
    categories: Iterable[str],
    *,
    max_items: int = 10000,
    proxy_use_apify: bool = True,
) -> dict:
    """
    Start the eBay actor with startUrls derived from categories.

    Parameters
    ----------
    categories:
        Iterable of category strings or full URLs.
    max_items:
        Actor input 'maxItems'.
    proxy_use_apify:
        Whether to use Apify proxy.

    Returns
    -------
    dict
        The run object returned by the Apify wrapper.
    """
    cats = _normalize_categories(categories)
    if not cats:
        raise ValueError("No categories provided to run()")

    start_urls = [{"url": u} for u in _categories_to_start_urls(cats)]
    logger.info(
        "Prepared eBay start URLs",
        extra={"start_url_count": len(start_urls), "start_urls_sample": start_urls[:3]},
    )

    run_input = {
        "startUrls": start_urls,
        "maxItems": int(max_items),
        "proxyConfig": {
            "useApifyProxy": bool(proxy_use_apify),
            "apifyProxyGroups": ["RESIDENTIAL"],
        },
    }

    # Timestamped output paths
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(settings.DATA_DIR) / "ebay"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_meta_path = out_dir / f"ebay_run_{ts}.json"

    logger.info("Starting eBay actor run", extra={"run_meta_path": str(run_meta_path)})

    # Use the shared apify wrapper which will handle waiting/downloading when requested.
    run_obj = run_actor_and_save(actor_key="ebay", input_=run_input)

    run_id = run_obj.get("id") or (run_obj.get("data") or {}).get("id")
    logger.info(
        "eBay actor run completed (or started)",
        extra={"run_id": run_id, "run_meta_saved": str(run_meta_path)},
    )

    return run_obj


def _read_categories_from_file(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Categories file not found: {path}")
    with p.open("r", encoding="utf-8") as fh:
        return [ln.strip() for ln in fh if ln.strip()]


def _parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run eBay Apify actor for a list of categories or URLs."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--categories", help="Comma-separated categories or URLs", type=str
    )
    group.add_argument(
        "--categories-file",
        help="Path to file with one category/URL per line",
        type=str,
    )
    parser.add_argument(
        "--max-items", type=int, default=10, help="Max items per start (actor param)"
    )
    parser.add_argument(
        "--no-proxy",
        dest="use_apify_proxy",
        action="store_false",
        help="Do not use Apify proxy",
    )
    parser.set_defaults(use_apify_proxy=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_args()

    if args.categories:
        categories_list = [c.strip() for c in args.categories.split(",") if c.strip()]
    else:
        categories_list = _read_categories_from_file(args.categories_file)

    try:
        run(
            categories_list,
            max_items=args.max_items,
            proxy_use_apify=args.use_apify_proxy,
        )
        logger.info("eBay script completed.")
    except Exception as exc:
        logger.exception("eBay script failed", extra={"error_msg": str(exc)})
        raise
