# main.py
"""
CLI entry point for running per-actor scrapers.
Usage examples:
  # Run amazon scraper reading categories from file
  python main.py --actor amazon --categories-file /path/to/categories.txt

  # Run multiple actors (comma separated)
  python main.py --actor amazon,etsy --categories "ginger,turmeric" --output-dir data/myrun
"""
from __future__ import annotations

import argparse
import importlib
from pathlib import Path
from typing import List

from utils.logger import get_logger
from config.settings import settings

logger = get_logger("main")


def _read_categories_from_file(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Categories file not found: {path}")
    with p.open("r", encoding="utf-8") as fh:
        return [ln.strip() for ln in fh if ln.strip()]


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Run ecommerce scrapers via Apify actors."
    )
    parser.add_argument(
        "--actor",
        required=True,
        help="Actor key to run. Options: amazon, etsy, shopify, ebay or comma-separated (e.g. 'amazon,etsy').",
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
    return parser.parse_args()


def _load_categories(args) -> List[str]:
    if args.categories:
        return [c.strip() for c in args.categories.split(",") if c.strip()]
    else:
        return _read_categories_from_file(args.categories_file)


def _call_scraper(actor_key: str, categories: List[str]):
    """
    Import scripts.<actor_key> module and call its `run()` function.
    Each script should expose a callable `run(categories, **kwargs)`.
    """
    module_name = f"scripts.{actor_key}"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        logger.error("Scraper module not found", extra={"module": module_name})
        raise

    if not hasattr(module, "run"):
        logger.error(
            "Scraper module missing run() function", extra={"module": module_name}
        )
        raise RuntimeError(
            f"Module {module_name} does not provide run(categories, ...)"
        )

    logger.info(
        "Dispatching to scraper",
        extra={
            "actor": actor_key,
            "module_name": module_name,
            "categories_count": len(categories),
        },
    )
    # Call the run function with recommended defaults; each script may accept additional kwargs
    result = module.run(categories)
    logger.info(
        "Scraper run returned",
        extra={"actor": actor_key, "result_summary": {"type": type(result).__name__}},
    )
    return result


def main():
    args = _parse_args()
    categories = _load_categories(args)
    actors = [a.strip() for a in args.actor.split(",") if a.strip()]

    for actor_key in actors:
        try:
            logger.info("Starting actor", extra={"actor": actor_key})
            _call_scraper(actor_key, categories)
        except Exception:
            logger.exception("Actor run failed", extra={"actor": actor_key})


if __name__ == "__main__":
    main()
