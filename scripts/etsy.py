# scripts/etsy_scraper.py
"""
Standalone Etsy scraper script that uses the project's Apify SDK wrapper (utils.apify_client.run_actor_and_save).

Behavior:
 - Accepts a list of categories (or raw URLs). For plain text categories it builds Etsy search URLs:
     https://www.etsy.com/search?q=<url-encoded-category>
 - If an item in the categories list starts with "http" it is treated as a startUrl (left unchanged).
 - Builds actor input modeled on the Etsy actor sample usage and calls the actor via run_actor_and_save().
 - Saves run metadata and dataset using the existing apify wrapper (into data/etsy/...).
 - Exposes run(categories, ...) for programmatic use and a small CLI for manual runs.

Example (programmatic):
    from scripts.etsy_scraper import run
    run(["apple watch", "leather wallet"])

Example (CLI):
    python scripts/etsy_scraper.py --categories "apple watch,leather wallet"
    python scripts/etsy_scraper.py --categories-file /path/to/categories.txt
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

logger = get_logger("etsy")


def _build_etsy_search_url(query: str) -> str:
    """Return an Etsy search URL for a given query string."""
    q = quote_plus(query.strip())
    return f"https://www.etsy.com/search?q={q}"


def _normalize_categories(categories: Iterable[str]) -> List[str]:
    items = []
    for c in categories:
        if not c:
            continue
        s = str(c).strip()
        if not s:
            continue
        items.append(s)
    return items


def _categories_to_start_urls(categories: Iterable[str]) -> List[str]:
    """
    Convert categories list into startUrls for the Etsy actor.
    If an entry already looks like a URL (starts with http/https) it is used as-is.
    Otherwise it's converted to an Etsy search URL.
    """
    start_urls: List[str] = []
    for c in _normalize_categories(categories):
        if c.lower().startswith("http://") or c.lower().startswith("https://"):
            start_urls.append(c)
        else:
            start_urls.append(_build_etsy_search_url(c))
    return start_urls


def run(
    categories: Iterable[str],
    *,
    include_description: bool = True,
    include_variation_prices: bool = True,
    max_items: int = 10000,
    proxy_use_apify: bool = True,
    apify_proxy_groups: Optional[List[str]] = None,
    extend_output_function: Optional[str] = None,
    custom_map_function: Optional[str] = None,
) -> dict:
    """
    Start the Etsy actor with startUrls derived from categories and the provided options.

    Parameters
    ----------
    categories:
        Iterable of category strings or full URLs.
    include_description:
        Whether to include description in output (actor input param).
    include_variation_prices:
        Whether to include variation prices.
    max_items:
        Max items per start (actor input param "maxItems").
    end_page:
        Actor input param controlling end page for pagination.
    proxy_use_apify:
        Whether to use Apify proxy (proxy.useApifyProxy).
    apify_proxy_groups:
        List of Apify proxy groups to use (e.g., ["RESIDENTIAL"]).
    extend_output_function:
        Optional JS string provided to the actor to extend output per page (as in example).
    custom_map_function:
        Optional JS string to transform objects (as in example).

    Returns
    -------
    dict:
        The run object returned by the Apify wrapper.
    """
    cats = _normalize_categories(categories)
    if not cats:
        raise ValueError("No categories provided to run()")

    start_urls = _categories_to_start_urls(cats)
    logger.info(
        "Prepared Etsy start URLs",
        extra={"start_url_count": len(start_urls)},
    )

    # Default proxy groups if not provided
    if apify_proxy_groups is None:
        apify_proxy_groups = ["RESIDENTIAL"]

    run_input = {
        "startUrls": start_urls,
        "includeDescription": bool(include_description),
        "includeVariationPrices": bool(include_variation_prices),
        "maxItems": int(max_items),
        "search": None,
        # Provide extend/custom functions if given (actor expects JS strings)
        "extendOutputFunction": extend_output_function,
        "customMapFunction": custom_map_function,
        "proxy": {
            "useApifyProxy": bool(proxy_use_apify),
            "apifyProxyGroups": apify_proxy_groups,
        },
    }

    # Clean None values (actor may accept null but keep payload tidy)
    payload = {k: v for k, v in run_input.items() if v is not None}

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(settings.DATA_DIR) / "etsy"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_meta_path = out_dir / f"etsy_run_{ts}.json"

    logger.info(
        "Starting Etsy actor run",
        extra={
            "run_meta_path": str(run_meta_path),
            "start_urls_sample": start_urls[:3],
        },
    )

    # Delegate to the shared Apify wrapper; it handles saving run metadata & dataset.
    run_obj = run_actor_and_save(actor_key="etsy", input_=payload)

    # The apify wrapper writes metadata to data/etsy/etsy.json and dataset to data/etsy/raw/etsy.dataset.json.
    # For convenience also save a timestamped copy of the run metadata next to it.
    try:
        with open(run_meta_path, "w", encoding="utf-8") as fh:
            import json

            json.dump(run_obj, fh, ensure_ascii=False, indent=2)
        logger.info(
            "Saved timestamped run metadata", extra={"path": str(run_meta_path)}
        )
    except Exception:
        logger.exception(
            "Failed to save timestamped run metadata",
            extra={"path": str(run_meta_path)},
        )

    logger.info(
        "Etsy actor run completed (or started)",
        extra={"run_id": run_obj.get("id") or (run_obj.get("data") or {}).get("id")},
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
        description="Run Etsy Apify actor for a list of categories or URLs."
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
        "--max-items", type=int, default=10000, help="Max items per start (actor param)"
    )
    parser.add_argument(
        "--end-page", type=int, default=None, help="End page for pagination"
    )
    parser.add_argument(
        "--use-apify-proxy",
        dest="use_apify_proxy",
        action="store_true",
        help="Use Apify proxy (default)",
    )
    parser.add_argument(
        "--no-apify-proxy",
        dest="use_apify_proxy",
        action="store_false",
        help="Do not use Apify proxy",
    )
    parser.set_defaults(use_apify_proxy=True)
    parser.add_argument(
        "--proxy-groups",
        type=str,
        default="RESIDENTIAL",
        help="Comma-separated Apify proxy groups (e.g. RESIDENTIAL)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_args()

    if args.categories:
        categories_list = [c.strip() for c in args.categories.split(",") if c.strip()]
    else:
        categories_list = _read_categories_from_file(args.categories_file)

    proxy_groups = [g.strip() for g in args.proxy_groups.split(",") if g.strip()]

    try:
        run(
            categories_list,
            include_description=False,
            include_variation_prices=False,
            max_items=args.max_items,
            proxy_use_apify=args.use_apify_proxy,
            apify_proxy_groups=proxy_groups,
        )
        logger.info("Etsy script completed.")
    except Exception as exc:
        logger.exception("Etsy script failed", extra={"error": str(exc)})
        raise
