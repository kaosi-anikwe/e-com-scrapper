#!/usr/bin/env python3
"""
normalize_with_azure.py

Reads JSON product files (one file per site or JSONL), performs deterministic extraction,
then calls Azure OpenAI (chat completions) to normalize/infer the remaining fields,
validates the returned JSON, and writes a single CSV using the exact header from
a template CSV file.

Usage (quick):
  python tools/normalize_with_azure.py \
    --input-dir data/raw/ \
    --template-csv ./templates/normalized_template.csv \
    --output-csv data/normalized/all_sites_normalized.csv \
    --azure-endpoint https://<your-resource>.openai.azure.com \
    --azure-key <your-key> \
    --deployment <deployment-name> \
    --concurrency 4 \
    --batch-size 50

Environment variables supported (will be used when CLI args omitted):
  AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, AZURE_OPENAI_DEPLOYMENT, AZURE_API_VERSION

Notes:
 - The script expects raw JSON files to contain either a JSON array of objects
   or JSON Lines (one JSON object per line). It will iterate all files under --input-dir.
 - Provide a template CSV (single-row header) to ensure exact column ordering.
 - Start with --batch-size small (10-50) while tuning prompts and verifying outputs.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from dotenv import load_dotenv
import json
from openai import AzureOpenAI
from openai.types.chat.chat_completion_message_param import ChatCompletionMessageParam
from utils.prompt import PROMPT_INSTRUCTION
from utils.logger import get_logger

logger = get_logger("normalize")

# -------------------------
# Deterministic helpers
# -------------------------
def safe_load_json_file(path: Path) -> List[Dict[str, Any]]:
    """Load JSON objects from a file. Accepts a JSON array or JSONL file."""
    text = path.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # single object - return as list
            return [data]
    except json.JSONDecodeError:
        # Try JSON lines
        objs = []
        for ln in text.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            try:
                objs.append(json.loads(ln))
            except Exception as e:
                logger.warning(
                    "Skipping invalid JSON line",
                    extra={"file": str(path), "line": ln[:200], "error": str(e)},
                )
        return objs
    return []


def extract_deterministic_fields(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract fields that can be computed reliably from the raw record:
    - price (numeric) and currency (ISO-ish if available)
    - images list
    - upc/ean/mpn
    - seller_name, itemLocation
    - domain / source
    """
    out: Dict[str, Any] = {}

    # Source/domain
    url = (
        record.get("url")
        or record.get("link")
        or record.get("product_url")
        or record.get("itemUrl")
    )
    out["url"] = url
    if url:
        try:
            out["source"] = urlparse(str(url)).netloc
        except Exception:
            out["source"] = None
    else:
        out["source"] = record.get("source") or record.get("site") or None

    # Title
    out["title"] = record.get("title") or record.get("name") or None

    # Price extraction: prefer numeric 'price' else parse priceWithCurrency
    price = record.get("price")
    currency = None
    if price is None:
        pwc = record.get("priceWithCurrency") or record.get("price_with_currency") or ""
        # crude parse: extract number and currency symbol if present
        if isinstance(pwc, str) and pwc.strip():
            # look for currency code or symbol and number
            # e.g., "US $24.95/ea" or "€12.00"
            import re

            num_match = re.search(
                r"[-+]?[0-9]{1,3}(?:[0-9,]*)(?:\.[0-9]+)?", pwc.replace(",", "")
            )
            if num_match:
                try:
                    out["price"] = float(num_match.group(0))
                except Exception:
                    out["price"] = None
            # try to detect currency ISO or symbol
            cur_match = re.search(r"\b([A-Z]{3})\b", pwc)
            if cur_match:
                currency = cur_match.group(1)
            else:
                sym_match = re.search(r"([$€£¥])", pwc)
                if sym_match:
                    sym = sym_match.group(1)
                    # map symbol to common code
                    symbol_map = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY"}
                    currency = symbol_map.get(sym)
    else:
        # numeric price present
        try:
            out["price"] = float(price)
        except Exception:
            out["price"] = None

        # maybe currency field separate
        currency = record.get("currency") or record.get("priceCurrency")

    out["currency"] = currency or None

    # Images
    imgs = (
        record.get("images")
        or record.get("image")
        or record.get("image_urls")
        or record.get("photos")
    )
    if isinstance(imgs, (list, tuple)):
        out["image_urls"] = list(imgs)
    elif isinstance(imgs, str):
        out["image_urls"] = [imgs]
    else:
        out["image_urls"] = []

    # Identifiers
    out["upc"] = record.get("upc") or record.get("ean") or None
    out["mpn"] = record.get("mpn") or None

    # Seller info
    out["seller_name"] = record.get("seller") or record.get("sellerName") or None
    out["itemLocation"] = record.get("itemLocation") or record.get("location") or None

    # Category (best-effort)
    cats = record.get("categories") or record.get("category") or None
    if isinstance(cats, list):
        out["category"] = cats[0] if cats else None
    else:
        out["category"] = cats

    # rating/reviews
    out["rating"] = record.get("rating") or record.get("averageRating") or None
    out["review_count"] = record.get("review_count") or record.get("reviews") or None

    # Raw description if present
    out["description"] = (
        record.get("description")
        or record.get("subTitle")
        or record.get("details")
        or None
    )

    return out


def build_prompt(record_det: Dict[str, Any], raw_record: Dict[str, Any]) -> str:
    """Return a user prompt combining instruction, deterministic fields, and the raw record as context."""
    # Put deterministic fields first (helps the model)
    context = {"deterministic": record_det, "raw": raw_record}
    prompt = PROMPT_INSTRUCTION.replace(
        "<<<INPUT_PRODUCT_JSON>>>", json.dumps(context, ensure_ascii=False)
    )

    return prompt


def call_azure_chat_completion(
    endpoint: str,
    deployment: str,
    api_key: str,
    prompt: str,
    api_version: str = "2025-01-01-preview",
    temperature: float = 1,
    timeout: int = 60,
) -> str:
    """
    Call Azure OpenAI using the AzureOpenAI client and return the assistant content string.

    Parameters:
      - endpoint: e.g. "https://<resource>.openai.azure.com"
      - deployment: the deployment name you created in Azure (passed as 'model')
      - api_key: Azure OpenAI key
      - prompt: final user prompt (string)
      - api_version: Azure API version (default matches your snippet)
      - temperature: model temperature (default 1)
      - timeout: request timeout in seconds

    Returns:
      assistant text (string). If structured extraction fails, returns a best-effort stringified response.
    """
    # Initialize client
    try:
        client = AzureOpenAI(
            azure_endpoint=endpoint, api_key=api_key, api_version=api_version
        )
    except Exception as exc:
        logger.exception(
            "Failed to initialize AzureOpenAI client", extra={"error": str(exc)}
        )
        raise

    # Build chat messages (system + user)
    messages: List[ChatCompletionMessageParam] = [
        {"role": "system", "content": "You are a helpful, precise data formatter."},
        {"role": "user", "content": prompt},
    ]

    try:
        resp = client.chat.completions.create(
            model=deployment,
            messages=messages,
            temperature=temperature,
            max_completion_tokens=40000,
            reasoning_effort="high",
        )
    except Exception as exc:
        # Log and re-raise so caller can handle retries
        logger.exception("Azure OpenAI request failed", extra={"error": str(exc)})
        raise

    content = resp.choices[0].message.content

    # Try to extract assistant content robustly from several possible shapes
    if not content:
        # Final fallbacks: to_json(), __str__, or json dump
        try:
            content = resp.to_json() if hasattr(resp, "to_json") else json.dumps(resp)
        except Exception:
            content = str(resp)

    return content



def write_batch_jsonl(records: List[Tuple[Dict[str, Any], Dict[str, Any]]], out_path: Path) -> int:
    """
    Write a JSONL file with one line per record. Each line is a JSON object with:
    {"id": "<index>", "input": {"messages": [{"role":"system","content":"..."}, {"role":"user","content":"<prompt>"}]}}
    Return number of lines written.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with out_path.open("w", encoding="utf-8") as fh:
        for idx, (det, raw) in enumerate(records):
            prompt = build_prompt(det, raw)
            # Build the messages shape; the batch job will use the same messages per input
            line = {
                "custom_id": f"task-{str(idx)}",
                "method": "POST",
                "url": "/chat/completions",
                "body": {
                    "model": "o4-mini",
                    "messages": [
                        {"role": "system", "content": "You are a helpful, precise data formatter."},
                        {"role": "user", "content": prompt}
                    ],
                },
            }
            fh.write(json.dumps(line, ensure_ascii=False) + "\n")
            written += 1
    return written



# -------------------------
# Orchestration
# -------------------------
def normalize_records(
    input_dir: Path,
    template_csv: Path,
    output_csv: Path,
    azure_endpoint: str,
    azure_key: str,
    deployment: str,
    api_version: str = "2024-10-01",
    concurrency: int = 4,
    batch_size: int = 50,
    max_retries: int = 1,
) -> None:
    """
    Main orchestration function.
    - Reads all JSON files under input_dir
    - Builds deterministic contexts
    - Submits requests to Azure OpenAI (concurrently)
    - Parses and validates outputs
    - Writes CSV in order of processing (append mode)
    """
    # Read exact header columns from template CSV
    with template_csv.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
    logger.info(
        "Using template CSV header",
        extra={"columns_count": len(header), "columns": header},
    )

    # Prepare output CSV
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = True
    if output_csv.exists():
        # If exists, don't write header again
        write_header = False

    # Collect all records
    all_records: List[Tuple[Dict[str, Any], Dict[str, Any]]] = (
        []
    )  # (deterministic, raw)
    files = list(Path(input_dir).glob("**/*"))
    json_files = [
        p
        for p in files
        if p.is_file() and p.suffix.lower() in {".json", ".ndjson", ".jsonl", ".txt"}
    ]
    logger.info(f"Found {len(json_files)} JSON files", extra={"count": len(json_files)})

    for jf in json_files:
        try:
            items = safe_load_json_file(jf)
            for it in items:
                if "reviews" in it:
                    del it["reviews"]
                    logger.info(f"Removed review from record")
                det = extract_deterministic_fields(it)
                all_records.append((det, it))
        except Exception:
            logger.exception("Failed to read JSON file", extra={"path": str(jf)})

    total = len(all_records)
    logger.info(f"Prepared {total} records", extra={"total_records": total})

    jsonl_out_path = Path(f"{os.path.splitext(output_csv)[0]}.jsonl")
    write_batch_jsonl(all_records, jsonl_out_path)

    sys.exit(0)

    # Helper to validate/parse model output
    def parse_and_map_output(model_text: str) -> Optional[Dict[str, Any]]:
        if not model_text:
            return None
        # Some LLM outputs include markdown or code fences; strip them
        txt = model_text.strip()
        if txt.startswith("```"):
            # strip code fences
            try:
                txt = "\n".join(txt.splitlines()[1:-1])
            except Exception:
                txt = txt.strip("` \n")
        # find first '{' and last '}' to try to extract JSON substring
        first = txt.find("{")
        last = txt.rfind("}")
        if first != -1 and last != -1 and last > first:
            txt = txt[first : last + 1]
        try:
            parsed = json.loads(txt)
            if isinstance(parsed, dict):
                return parsed
            return None
        except Exception as e:
            logger.warning(
                "Failed to parse JSON from model output",
                extra={"error": str(e), "snippet": txt[:400]},
            )
            return None

    # Worker function: call LLM and get normalized object
    def worker_job(
        idx_and_pair: Tuple[int, Tuple[Dict[str, Any], Dict[str, Any]]],
    ) -> Optional[Dict[str, Any]]:
        idx, (deterministic, raw) = idx_and_pair
        prompt = build_prompt(deterministic, raw)
        for attempt in range(max_retries + 1):
            try:
                content = call_azure_chat_completion(
                    endpoint=azure_endpoint,
                    deployment=deployment,
                    api_key=azure_key,
                    prompt=prompt,
                    api_version=api_version,
                    temperature=1,
                )
                parsed = parse_and_map_output(content)
                if parsed is None:
                    # retry once with slightly different instruction
                    logger.warning(
                        "Model returned unparsable output, retrying once",
                        extra={"index": idx},
                    )
                    time.sleep(1)
                    continue
                # ensure keys for all header columns present (set missing -> None)
                normalized_row = {
                    col: parsed.get(col) if isinstance(parsed, dict) else None
                    for col in header
                }
                # But also include any deterministic fields if null in parsed output
                for k, v in deterministic.items():
                    if k in normalized_row and (normalized_row[k] is None):
                        normalized_row[k] = v
                logger.info(f"Successfully normalized row - {idx}")
                return normalized_row
            except Exception as e:
                logger.exception(
                    "Worker job exception",
                    extra={"index": idx, "attempt": attempt, "error": str(e)},
                )
                time.sleep(1 + attempt * 2)
        logger.error("Worker job failed after retries", extra={"index": idx})
        return None

    # Process in micro-batches but using ThreadPool for concurrency
    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {}
        for idx, pair in enumerate(all_records):
            fut = ex.submit(worker_job, (idx, pair))
            futures[fut] = idx

        processed = 0
        with output_csv.open("a", encoding="utf-8", newline="") as fh_out:
            writer = csv.DictWriter(fh_out, fieldnames=header)
            if write_header:
                writer.writeheader()
            for fut in as_completed(futures):
                idx = futures[fut]
                processed += 1
                try:
                    row = fut.result()
                    if row:
                        # Ensure all fields present and are simple serializable types
                        for k in header:
                            v = row.get(k)
                            # convert lists to JSON strings for CSV cells
                            if isinstance(v, (list, dict)):
                                row[k] = json.dumps(v, ensure_ascii=False)
                        writer.writerow({k: row.get(k) for k in header})
                    else:
                        # write a placeholder row with url and nulls
                        _, (det, raw) = all_records[idx]
                        placeholder = {c: None for c in header}
                        placeholder["url"] = det.get("url") or raw.get("url")
                        writer.writerow(placeholder)
                    logger.info(f"Job processed successfully - {idx}")
                except Exception:
                    logger.exception(
                        "Failed to process future result", extra={"index": idx}
                    )
                if processed % 50 == 0:
                    logger.info(
                        "Progress", extra={"processed": processed, "total": total}
                    )

    logger.info(
        "Normalization complete",
        extra={"total_processed": processed, "output_csv": str(output_csv)},
    )


# -------------------------
# CLI
# -------------------------
def _parse_args():
    parser = argparse.ArgumentParser(
        description="Normalize scraped JSON with Azure OpenAI and write CSV."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing raw JSON files (recursive).",
    )
    parser.add_argument(
        "--template-csv",
        required=True,
        help="CSV file with the exact header (one header row).",
    )
    parser.add_argument(
        "--output-csv", required=True, help="Path to write normalized CSV."
    )
    parser.add_argument(
        "--azure-endpoint",
        default=os.getenv("AZURE_OPENAI_ENDPOINT"),
        help="Azure OpenAI endpoint (e.g. https://<name>.openai.azure.com).",
    )
    parser.add_argument(
        "--azure-key",
        default=os.getenv("AZURE_OPENAI_KEY"),
        help="Azure OpenAI API key.",
    )
    parser.add_argument(
        "--deployment",
        default=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        help="Azure OpenAI deployment name.",
    )
    parser.add_argument(
        "--api-version",
        default=os.getenv("AZURE_API_VERSION", "2025-01-01-preview"),
        help="Azure OpenAI API version.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Number of concurrent worker threads.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="(Not used in this sync POC) logical batch size for the work queue.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=1,
        help="Number of retries for unparsable model outputs.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    load_dotenv()
    args = _parse_args()

    # Basic validations
    if not args.azure_endpoint or not args.azure_key or not args.deployment:
        logger.error(
            "Azure credentials and deployment are required. Set CLI args or env AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, AZURE_OPENAI_DEPLOYMENT"
        )
        raise SystemExit(1)

    normalize_records(
        input_dir=Path(args.input_dir),
        template_csv=Path(args.template_csv),
        output_csv=Path(args.output_csv),
        azure_endpoint=args.azure_endpoint,
        azure_key=args.azure_key,
        deployment=args.deployment,
        api_version=args.api_version,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
        max_retries=args.max_retries,
    )
