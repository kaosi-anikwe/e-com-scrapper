# scripts/walmart_scraper.py
"""
Standalone Walmart scraper script that builds `run_input` for the Walmart Apify Actor
and delegates execution to the project's Apify SDK wrapper (utils.apify_client.run_actor_and_save).

Generated run_input example:
{
    "includeReviews": True,
    "maxItems": 10000,
    "onlyReviews": False,
    "proxy": {"useApifyProxy": True},
    "startUrls": [{"url": "https://www.walmart.com/search?query=Mixed+Bouquets"}]
}

Usage (CLI):
  python -m scripts.walmart_scraper --categories "mixed bouquets,lavender oil" --max-items 1000
  python -m scripts.walmart_scraper --categories-file ../categories.txt --no-proxy
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Dict
from urllib.parse import quote_plus

from config.settings import settings
from utils.logger import get_logger
from utils.apify_client import run_actor_and_save

logger = get_logger("walmart")


def _build_walmart_search_url(query: str) -> str:
    """Return a Walmart search URL for a given query string."""
    q = quote_plus(query.strip())
    return f"https://www.walmart.com/search?query={q}"


def _normalize_categories(categories: Iterable[str]) -> List[str]:
    return [str(c).strip() for c in categories if c and str(c).strip()]


def _categories_to_start_urls(categories: Iterable[str]) -> List[Dict[str, str]]:
    """
    Convert category strings or full URLs into startUrls list of dicts for actor input.
    If an entry already looks like a URL (starts with http/https) it is used as-is.
    """
    out: List[Dict[str, str]] = []
    for c in _normalize_categories(categories):
        lc = c.lower()
        if lc.startswith("http://") or lc.startswith("https://"):
            out.append({"url": c})
        else:
            out.append({"url": _build_walmart_search_url(c)})
    return out


def run(
    categories: Iterable[str],
    *,
    include_reviews: bool = True,
    max_items: int = 10000,
    only_reviews: bool = False,
    proxy_use_apify: bool = True,
) -> dict:
    """
    Start the Walmart actor with startUrls derived from categories and provided options.

    Parameters
    ----------
    categories:
        Iterable of category keywords or full URLs.
    include_reviews:
        Whether to include reviews in the output (actor param).
    max_items:
        Actor input 'maxItems'.
    only_reviews:
        Whether to fetch only reviews (actor param).
    proxy_use_apify:
        Whether to use Apify proxy (proxy.useApifyProxy).

    Returns
    -------
    dict
        The run object returned by the Apify wrapper.
    """
    cats = _normalize_categories(categories)
    if not cats:
        raise ValueError("No categories provided to run()")

    start_urls = _categories_to_start_urls(cats)
    logger.info(
        "Prepared Walmart start URLs",
        extra={"start_url_count": len(start_urls), "start_urls_sample": start_urls[:3]},
    )

    run_input = {
        "includeReviews": bool(include_reviews),
        "maxItems": int(max_items),
        "onlyReviews": bool(only_reviews),
        "proxy": {
            "useApifyProxy": bool(proxy_use_apify),
            "apifyProxyGroups": ["RESIDENTIAL"],
        },
        "startUrls": start_urls,
    }

    # Prepare timestamped output paths
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(settings.DATA_DIR) / "walmart"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_meta_path = out_dir / f"walmart_run_{ts}.json"

    logger.info(
        "Starting Walmart actor run", extra={"run_meta_path": str(run_meta_path)}
    )

    # Start actor via the shared apify wrapper. Ensure you have 'walmart' in config.ACTORS mapped to the actor ID.
    run_obj = run_actor_and_save(actor_key="walmart", input_=run_input)

    # Save a timestamped dataset summary (best-effort)
    try:
        dataset_id = run_obj.get("defaultDatasetId") or (run_obj.get("data") or {}).get(
            "defaultDatasetId"
        )
        if dataset_id:
            # small helper: save a manifest with dataset id
            manifest_path = out_dir / f"walmart_dataset_{ts}.meta.json"
            with open(manifest_path, "w", encoding="utf-8") as fh:
                json.dump(
                    {
                        "datasetId": dataset_id,
                        "run": run_obj.get("id") or run_obj.get("data", {}).get("id"),
                    },
                    fh,
                    indent=2,
                )
            logger.info(
                "Saved dataset manifest",
                extra={"path": str(manifest_path), "dataset_id": dataset_id},
            )
    except Exception:
        logger.exception("Failed to save dataset manifest", extra={})

    logger.info(
        "Walmart actor run finished (or started)",
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
        description="Run Walmart Apify actor for a list of categories or URLs."
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
        "--max-items", type=int, default=10000, help="Max items to fetch (actor param)"
    )
    parser.add_argument(
        "--include-reviews",
        dest="include_reviews",
        action="store_true",
        help="Include reviews (default)",
    )
    parser.add_argument(
        "--no-reviews",
        dest="include_reviews",
        action="store_false",
        help="Do not include reviews",
    )
    parser.set_defaults(include_reviews=True)
    parser.add_argument(
        "--only-reviews",
        dest="only_reviews",
        action="store_true",
        help="Fetch only reviews",
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
            include_reviews=args.include_reviews,
            max_items=args.max_items,
            only_reviews=args.only_reviews,
            proxy_use_apify=args.use_apify_proxy,
        )
        logger.info("Walmart script completed.")
    except Exception as exc:
        logger.exception("Walmart script failed", extra={"error_msg": str(exc)})
        raise
