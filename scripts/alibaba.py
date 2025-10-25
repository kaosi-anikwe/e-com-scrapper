# scripts/alibaba.py
"""
Standalone Alibaba scraper script that builds `run_input` for the Alibaba Apify Actor
and delegates execution to the project's Apify SDK wrapper (utils.apify_client.run_actor_and_save).

Generated run_input format example:
{
    "maxItems": 5,
    "proxyConfiguration": {
        "useApifyProxy": True,
        "apifyProxyGroups": ["RESIDENTIAL"]
    },
    "startUrls": [{"url": "https://www.alibaba.com/trade/search?fsb=y&IndexArea=product_en&keywords=Groom+Wear"}]
}

Usage (CLI):
  python -m scripts.alibaba --categories "groom wear, lavender oil" --max-items 5
  python -m scripts.alibaba --categories-file ../categories.txt
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Dict
from urllib.parse import urlencode, quote_plus

from config.settings import settings
from utils.logger import get_logger
from utils.apify_client import run_actor_and_save

logger = get_logger("alibaba")


def _build_alibaba_search_url(
    query: str,
) -> str:
    """
    Construct an Alibaba search URL for a given query.
    Uses common query parameters seen on Alibaba search pages.
    """
    base = "https://www.alibaba.com/trade/search"
    # core params
    params: Dict[str, str] = {
        "fsb": "y",
        "IndexArea": "product_en",
        "SearchText": query.strip(),
        "keywords": query.strip(),
    }

    # urlencode will percent-encode keywords correctly; but keep it readable by using quote for keywords
    # ensure keywords are properly encoded
    # build query string manually so keywords are quoted
    qs_items = []
    for k, v in params.items():
        if k == "keywords":
            qs_items.append(f"{k}={quote_plus(v)}")
        else:
            qs_items.append(f"{k}={quote_plus(str(v))}")
    qs = "&".join(qs_items)
    return f"{base}?{qs}"


def _normalize_categories(categories: Iterable[str]) -> List[str]:
    return [str(c).strip() for c in categories if c and str(c).strip()]


def _categories_to_start_urls(
    categories: Iterable[str],
) -> List[Dict[str, str]]:
    """
    Convert category strings or full URLs into startUrls list of dicts for actor input.
    If an item already looks like a URL, use it as-is.
    Otherwise, build an Alibaba search URL for that keyword.
    """
    out = []
    for c in _normalize_categories(categories):
        lc = c.lower()
        if lc.startswith("http://") or lc.startswith("https://"):
            out.append({"url": c})
        else:
            url = _build_alibaba_search_url(c)
            out.append({"url": url})
    return out


def run(
    categories: Iterable[str],
    *,
    max_items: int = 10000,
    proxy_use_apify: bool = True,
    apify_proxy_groups: Optional[List[str]] = None,
) -> dict:
    """
    Start Alibaba actor with generated run_input.

    Parameters
    ----------
    categories: iterable of category keywords or URLs
    max_items: maximum items to fetch (actor param "maxItems")
    proxy_use_apify: whether to enable Apify proxy in proxyConfiguration
    apify_proxy_groups: optional list of Apify proxy groups (e.g. ["RESIDENTIAL"])
    """
    cats = _normalize_categories(categories)
    if not cats:
        raise ValueError("No categories provided to run()")

    # Default proxy groups when using Apify proxy
    if apify_proxy_groups is None:
        apify_proxy_groups = ["RESIDENTIAL"]

    start_urls = _categories_to_start_urls(cats)
    logger.info(
        "Prepared Alibaba start URLs",
        extra={"start_url_count": len(start_urls), "start_urls_sample": start_urls[:3]},
    )

    run_input = {
        "maxItems": int(max_items),
        "proxyConfiguration": {
            "useApifyProxy": bool(proxy_use_apify),
            "apifyProxyGroups": apify_proxy_groups,
        },
        "startUrls": start_urls,
    }

    # Prepare output paths
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(settings.DATA_DIR) / "alibaba"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_meta_path = out_dir / f"alibaba_run_{ts}.json"

    logger.info(
        "Starting Alibaba actor run", extra={"run_meta_path": str(run_meta_path)}
    )

    # Note: the utils.apify_client.run_actor_and_save helper expects an actor_key defined in config.ACTORS.
    # Make sure you set ALIBABA_ACTOR in your .env and config/settings.py maps to 'alibaba' key.
    run_obj = run_actor_and_save(actor_key="alibaba", input_=run_input)

    logger.info(
        "Alibaba actor run finished (or started)",
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
        description="Run Alibaba Apify actor for categories or URLs."
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
        "--max-items", type=int, default=10000, help="Max items (actor param)"
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
        help="Comma-separated Apify proxy groups",
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
            max_items=args.max_items,
            proxy_use_apify=args.use_apify_proxy,
            apify_proxy_groups=proxy_groups,
        )
        logger.info("Alibaba script completed.")
    except Exception as exc:
        logger.exception("Alibaba script failed", extra={"error_msg": str(exc)})
        raise
