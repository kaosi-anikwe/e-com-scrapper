"""
Configuration settings for ecommerce_scrapers project.
Loads environment variables from a .env file via python-dotenv.
Expose a single `settings` object for the rest of the codebase to import.
"""

from __future__ import annotations


import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any


from dotenv import load_dotenv


# Load .env from project root (caller should ensure working dir is project root)
load_dotenv()


@dataclass(frozen=True)
class Settings:
    # Core
    APIFY_API_KEY: str

    # Actor IDs (defaults read from env)
    ACTORS: Dict[str, Any]

    # Runtime defaults for running actors
    DEFAULT_MEMORY_MBYTES: int = 1024
    DEFAULT_WAIT_FOR_FINISH: bool = True
    DEFAULT_WAIT_FOR_FINISH_TIMEOUT: int = 600  # seconds

    # Logging / output
    LOGS_DIR: str = "logs"
    DATA_DIR: str = "data"

    # Network / retry
    REQUEST_RETRY_TOTAL: int = 3
    REQUEST_RETRY_BACKOFF_FACTOR: float = 0.5


def _load_settings_from_env() -> Settings:
    apify_key = os.getenv("APIFY_API_KEY") or os.getenv("APIFY_TOKEN")
    if not apify_key:
        raise RuntimeError(
            "APIFY_API_KEY not found in environment. Please add it to your .env or env vars."
        )

    return Settings(
        APIFY_API_KEY=apify_key,
        ACTORS={
            "amazon": os.getenv("AMAZON_ACTOR"),
            "ebay": os.getenv("EBAY_ACTOR"),
            "etsy": os.getenv("ETSY_ACTOR"),
            "alibaba": os.getenv("ALIBABA_ACTOR"),
            "walmart": os.getenv("WALMART_ACTOR"),
            "jumia": os.getenv("JUMIA_ACTOR"),
        },
        DEFAULT_MEMORY_MBYTES=int(os.getenv("DEFAULT_MEMORY_MBYTES", "1024")),
        DEFAULT_WAIT_FOR_FINISH=(
            os.getenv("DEFAULT_WAIT_FOR_FINISH", "true").lower() in ("1", "true", "yes")
        ),
        DEFAULT_WAIT_FOR_FINISH_TIMEOUT=int(
            os.getenv("DEFAULT_WAIT_FOR_FINISH_TIMEOUT", "600")
        ),
        LOGS_DIR=os.getenv("LOGS_DIR", "logs"),
        DATA_DIR=os.getenv("DATA_DIR", "data"),
        REQUEST_RETRY_TOTAL=int(os.getenv("REQUEST_RETRY_TOTAL", "3")),
        REQUEST_RETRY_BACKOFF_FACTOR=float(
            os.getenv("REQUEST_RETRY_BACKOFF_FACTOR", "0.5")
        ),
    )


# Singleton settings object importable across the codebase
settings = _load_settings_from_env()
Path(settings.DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(settings.LOGS_DIR).mkdir(parents=True, exist_ok=True)
