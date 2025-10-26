#!/usr/bin/env python3
"""
Process Azure OpenAI Batch JSONL output into a CSV matching a template header.

Usage:
  python tools/process_batch_output.py --batch-jsonl path/to/batch_output.jsonl --template-csv templates/normalized_template.csv

If --output-csv omitted, the script writes the CSV next to the input file replacing .jsonl -> .csv.
"""

from __future__ import annotations
import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from utils.logger import get_logger


logger = get_logger("process_batch")


def _extract_content_from_line(obj: Dict[str, Any]) -> Optional[str]:
    """
    Given one parsed JSONL line (dict), extract the assistant content string if present.
    Handles nested shapes like response.body.choices[0].message.content.
    """
    # Common path in portal JSONL: obj["response"]["body"]["choices"][0]["message"]["content"]
    try:
        resp = obj.get("response") or {}
        body = resp.get("body") or {}
        choices = body.get("choices") or []
        if choices and isinstance(choices, list):
            first = choices[0]
            # message may be dict-like
            msg = first.get("message") or {}
            content = msg.get("content")
            if content:
                return content
            # fallback: some SDKs put the textual output under 'text' or 'message'->'text'
            if first.get("text"):
                return first.get("text")
            if isinstance(msg, str):
                return msg
        # other fallback locations
        if "content" in obj:
            return obj["content"]

        # deep fallback: search nested dicts for a 'content' string
        def search_for_content(d):
            if isinstance(d, dict):
                for k, v in d.items():
                    if (
                        k == "content"
                        and isinstance(v, str)
                        and ("{" in v or "]" in v or '"' in v)
                    ):
                        return v
                    if isinstance(v, dict):
                        found = search_for_content(v)
                        if found:
                            return found
                    if isinstance(v, list):
                        for item in v:
                            found = search_for_content(item)
                            if found:
                                return found
            return None

        return search_for_content(obj)
    except Exception as e:
        logger.exception("Error extracting content", extra={"error": str(e)})
        return None


def _extract_json_from_content(content: str) -> Optional[Dict[str, Any]]:
    """
    Clean content string and parse JSON substring. Returns dict or None.
    """
    if not content:
        return None
    txt = content.strip()
    # strip markdown/code fences if present
    if txt.startswith("```") and txt.endswith("```"):
        # remove outer fences
        parts = txt.splitlines()
        if len(parts) >= 3:
            txt = "\n".join(parts[1:-1]).strip()
    # find JSON object substring (first { ... last })
    first = txt.find("{")
    last = txt.rfind("}")
    if first != -1 and last != -1 and last > first:
        candidate = txt[first : last + 1]
    else:
        candidate = txt
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
        # sometimes model returns top-level array - not expected, but try first element
        if isinstance(parsed, list) and parsed:
            if isinstance(parsed[0], dict):
                return parsed[0]
            return None
    except Exception:
        # last effort: try to replace some common escaped quotes
        try:
            candidate2 = candidate.replace('\\"', '"')
            parsed = json.loads(candidate2)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            logger.debug(
                "Failed to JSON-parse model content", extra={"snippet": candidate[:300]}
            )
            return None
    return None


def _value_for_header(parsed: Dict[str, Any], header_key: str) -> Any:
    """
    Look up header_key in parsed dict with some tolerant matching:
    - exact match
    - lowercase matching
    - underscore/space variants
    """
    if parsed is None:
        return None
    # exact
    if header_key in parsed:
        return parsed[header_key]
    # try lowercase key
    lk = header_key.lower()
    if lk in parsed:
        return parsed[lk]
    # try key variants
    alt1 = header_key.replace(" ", "_")
    if alt1 in parsed:
        return parsed[alt1]
    if alt1.lower() in parsed:
        return parsed[alt1.lower()]
    alt2 = header_key.replace(" ", "")
    if alt2 in parsed:
        return parsed[alt2]
    if alt2.lower() in parsed:
        return parsed[alt2.lower()]
    # try simple fallback: check keys case-insensitively
    for k, v in parsed.items():
        if isinstance(k, str) and k.strip().lower() == lk:
            return v
    return None


def process_batch_jsonl_to_csv(
    batch_jsonl_path: Path,
    template_csv_path: Path,
    output_csv_path: Optional[Path] = None,
) -> Path:
    """
    Process the batch output JSONL file and write the matching CSV.
    Returns path to the written CSV.
    """
    if not batch_jsonl_path.exists():
        raise FileNotFoundError(f"Batch JSONL not found: {batch_jsonl_path}")
    # determine output path
    if output_csv_path is None:
        if batch_jsonl_path.suffix.lower() == ".jsonl":
            output_csv_path = batch_jsonl_path.with_suffix(".csv")
        else:
            output_csv_path = batch_jsonl_path.parent / (batch_jsonl_path.name + ".csv")

    # read header from template csv (first row)
    with template_csv_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)

    logger.info(
        "Processing batch JSONL",
        extra={
            "input": str(batch_jsonl_path),
            "output": str(output_csv_path),
            "columns": len(header),
        },
    )

    total = 0
    written = 0
    with batch_jsonl_path.open("r", encoding="utf-8") as fh_in, output_csv_path.open(
        "w", encoding="utf-8", newline=""
    ) as fh_out:
        writer = csv.DictWriter(fh_out, fieldnames=header)
        writer.writeheader()
        for line in fh_in:
            total += 1
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                logger.warning(
                    "Skipping non-JSON line", extra={"line_preview": line[:200]}
                )
                continue

            # extract assistant content
            content_str = _extract_content_from_line(obj)

            parsed = None
            if content_str:
                parsed = _extract_json_from_content(content_str)

            # fallback: maybe the batch writer already put the parsed JSON under response.body.choices[0].message.content as parsed dict
            if parsed is None:
                # check if content was already a dict in the JSONL (rare)
                # for example: obj["response"]["body"]["choices"][0]["message"]["content"] might be dict
                try:
                    resp = obj.get("response") or {}
                    body = resp.get("body") or {}
                    choices = body.get("choices") or []
                    if choices and isinstance(choices, list):
                        first = choices[0]
                        msg = first.get("message") or {}
                        cont = msg.get("content")
                        if isinstance(cont, dict):
                            parsed = cont
                except Exception:
                    parsed = None

            # Build CSV row ensuring header keys exist
            row: Dict[str, Optional[str]] = {}
            if parsed and isinstance(parsed, dict):
                for col in header:
                    val = _value_for_header(parsed, col)
                    # if it's a list/dict serialize to JSON string (so it fits CSV cell)
                    if isinstance(val, (list, dict)):
                        try:
                            row[col] = json.dumps(val, ensure_ascii=False)
                        except Exception:
                            row[col] = str(val)
                    else:
                        row[col] = val
            else:
                # parsed is None -> create placeholder row filled with nulls; try to fill product ID or custom_id if available
                placeholder = {c: None for c in header}
                # try to find ids
                cid = (
                    obj.get("custom_id")
                    or (obj.get("response") or {}).get("request_id")
                    or None
                )
                # try to parse some fields from top-level (rare)
                top_url = None
                # set product ID field if present in header
                if "product ID" in header and cid:
                    placeholder["product ID"] = cid
                # if there is a nested 'response' with body->choices->message->content raw string that itself is JSON, we already tried parsing it
                # write the placeholder
                row = placeholder  # type: ignore
                logger.warning(
                    "Parsed JSON missing for line; writing placeholder",
                    extra={"line_index": total, "custom_id": cid},
                )

            # ensure all header columns are present
            for k in header:
                if k not in row:
                    row[k] = None

            # Finally, convert any lists/dicts still present, and convert boolean -> lowercase true/false if desirable
            safe_row = {}
            for k in header:
                v = row[k]
                if isinstance(v, (list, dict)):
                    safe_row[k] = json.dumps(v, ensure_ascii=False)
                elif isinstance(v, bool):
                    # CSV will hold True/False; keep as lowercase string to be explicit
                    safe_row[k] = "true" if v else "false"
                elif v is None:
                    safe_row[k] = ""
                else:
                    safe_row[k] = v
            # Write row
            writer.writerow(safe_row)
            written += 1

            if written % 100 == 0:
                logger.info(
                    "Progress",
                    extra={"written": written, "total_lines_processed": total},
                )

    logger.info(
        "Done processing batch JSONL",
        extra={
            "total_lines": total,
            "rows_written": written,
            "output": str(output_csv_path),
        },
    )
    return output_csv_path


def _parse_args():
    p = argparse.ArgumentParser(
        description="Convert Azure OpenAI batch output JSONL to CSV using template header."
    )
    p.add_argument(
        "--batch-jsonl", required=True, help="Path to batch output JSONL file."
    )
    p.add_argument(
        "--template-csv", required=True, help="Path to template CSV (header row used)."
    )
    p.add_argument(
        "--output-csv",
        required=False,
        help="Optional output CSV path. If omitted, replaces .jsonl with .csv.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    batch_path = Path(args.batch_jsonl)
    template_path = Path(args.template_csv)
    out_path = Path(args.output_csv) if args.output_csv else None
    process_batch_jsonl_to_csv(batch_path, template_path, out_path)
