# scripts/jumia.py
"""
Standalone Jumia scraper script that uses the project's Apify SDK wrapper (utils.apify_client.run_actor_and_save).

Behavior:
 - Accepts a list of categories (or raw search URLs). For plain text categories it builds Jumia search URLs:
     https://www.jumia.com.ng/catalog/?q=<url-encoded-query>
 - If an entry already starts with http/https it is treated as a searchUrl (used as-is).
 - Builds actor input modeled on the requested format and calls the actor via run_actor_and_save().
 - Saves run metadata into data/jumia/.
 - Exposes run(categories, ...) for programmatic use and provides a small CLI matching the ebay script style.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.parse import quote_plus

from config.settings import settings
from utils.logger import get_logger
from utils.apify_client import run_actor_and_save

logger = get_logger("jumia")


def _build_jumia_search_url(query: str, domain: str = "www.jumia.com.ng") -> str:
    """Return a Jumia search URL for a given query string and domain."""
    q = quote_plus(query.strip())
    return f"https://{domain}/catalog/?q={q}"


def _normalize_categories(categories: Iterable[str]) -> List[str]:
    return [str(c).strip() for c in categories if c and str(c).strip()]


def _categories_to_search_urls(
    categories: Iterable[str], domain: str = "www.jumia.com.ng"
) -> List[str]:
    """
    Convert category strings or full URLs into a list of searchUrls for actor input.
    If an item already looks like a URL (starts with http/https) it is used as-is.
    """
    out: List[str] = []
    for c in _normalize_categories(categories):
        lower = c.lower()
        if lower.startswith("http://") or lower.startswith("https://"):
            out.append(c)
        else:
            out.append(_build_jumia_search_url(c, domain=domain))
    return out


def run(
    categories: Iterable[str],
    *,
    max_items: int = 100,
    proxy_use_apify: bool = True,
    apify_proxy_groups: Optional[List[str]] = None,
    domain: str = "www.jumia.com.ng",
) -> dict:
    """
    Start the Jumia actor with searchUrls derived from categories.

    Parameters
    ----------
    categories:
        Iterable of category strings or full URLs.
    max_items:
        Actor input 'maxItems'.
    proxy_use_apify:
        Whether to use Apify proxy.
    apify_proxy_groups:
        Optional list of Apify proxy groups (defaults to ["RESIDENTIAL"]).
    domain:
        Jumia domain to target (e.g., 'www.jumia.com.ng', 'www.jumia.co.ke').

    Returns
    -------
    dict
        The run object returned by the Apify wrapper.
    """
    cats = _normalize_categories(categories)
    if not cats:
        raise ValueError("No categories provided to run()")

    search_urls = _categories_to_search_urls(cats, domain=domain)
    logger.info(
        "Prepared Jumia search URLs",
        extra={
            "search_url_count": len(search_urls),
            "search_urls_sample": search_urls[:3],
        },
    )

    if apify_proxy_groups is None:
        apify_proxy_groups = ["RESIDENTIAL"]

    run_input = {
        "proxyConfiguration": {
            "useApifyProxy": bool(proxy_use_apify),
            "apifyProxyGroups": apify_proxy_groups,
        },
        "searchUrls": search_urls,
        "maxItems": int(max_items),
    }

    # Timestamped output path
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(settings.DATA_DIR) / "jumia"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_meta_path = out_dir / f"jumia_run_{ts}.json"

    logger.info("Starting Jumia actor run", extra={"run_meta_path": str(run_meta_path)})

    # Use the shared apify wrapper. Make sure 'jumia' is present in config.ACTORS mapping.
    run_obj = run_actor_and_save(actor_key="jumia", input_=run_input)

    run_id = run_obj.get("id") or (run_obj.get("data") or {}).get("id")
    logger.info(
        "Jumia actor run completed (or started)",
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
        description="Run Jumia Apify actor for a list of categories or search URLs."
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
        "--max-items", type=int, default=100, help="Max items to fetch (actor param)"
    )
    parser.add_argument(
        "--domain",
        type=str,
        default="www.jumia.com.ng",
        help="Jumia domain to target (default: www.jumia.com.ng)",
    )
    parser.add_argument(
        "--no-proxy",
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
            domain=args.domain,
        )
        logger.info("Jumia script completed.")
    except Exception as exc:
        logger.exception("Jumia script failed", extra={"error_msg": str(exc)})
        raise
