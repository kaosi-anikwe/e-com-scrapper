# e-com-scrapper

> Standalone Python scrapers (Apify actors) for multiple e-commerce sites and a normalization pipeline that turns heterogeneous JSON output into a single canonical CSV schema (using Azure OpenAI for semantic normalization + Azure Batch for large-scale processing).

---

## Table of contents

1. [Quick summary](#quick-summary)
2. [Repository layout](#repository-layout)
3. [Requirements & installation](#requirements--installation)
4. [.env and configuration](#env-and-configuration)
5. [How the scrapers work (usage examples)](#how-the-scrapers-work-usage-examples)
6. [Logging and output locations](#logging-and-output-locations)
7. [Apify Actors — what they are and costs used in this project](#apify-actors---what-they-are-and-costs-used-in-this-project)
8. [Post-scraping: Normalization (Azure + Batch) workflow](#post-scraping-normalization-azure--batch-workflow)
9. [Process Batch output → CSV (what to run locally)](#process-batch-output--csv-what-to-run-locally)
10. [Troubleshooting & common issues](#troubleshooting--common-issues)
11. [Notes, cost tips & disclaimers](#notes-cost-tips--disclaimers)
12. [License](#license)

---

## Quick summary

This project contains **standalone** scraper scripts (one per site) that call Apify actors to perform scraping. Each scraper writes raw dataset JSON into the `data/` folder and creates per-scraper log files in `logs/`. After scraping, you upload JSONL inputs to Azure OpenAI Batch via the Azure portal (we do _not_ automatically create batch jobs from code in this repo). When the Batch job finishes you download the output JSONL and run the included `process_batch` script to convert results into the final CSV that exactly follows the provided template header.

---

## Repository layout (short)

```
.
├─ config/
│  └─ settings.py            # environment-driven settings (DATA_DIR, LOGS_DIR, actor IDs)
├─ scripts/
│  ├─ amazon.py              # Amazon actor wrapper (standalone CLI)
│  ├─ ebay.py                # eBay actor wrapper
│  ├─ etsy.py                # Etsy actor wrapper
│  ├─ alibaba.py             # Alibaba actor wrapper
│  ├─ jumia.py               # Jumia actor wrapper
│  ├─ walmart.py             # Walmart actor wrapper
│  └─ process_batch.py       # Convert Azure Batch JSONL output -> CSV
├─ utils/
│  ├─ apify_client.py        # ApifyClient wrapper (run actor, download dataset to data/<site>/raw/...)
│  ├─ logger.py              # get_logger(...) utility (rotation/formatting, console optional)
│  ├─ normalize.py           # normalization helpers / validation utilities (used offline)
│  └─ prompt.py              # prompt templates for the Azure normalization step (NOT executed here)
├─ requirements.txt
├─ .env.example
└─ README.md                 # (this file)
```

> Note: exact file names may vary slightly (`scripts/amazon.py` etc). The codebase expects you to run scripts from the **project root** so relative imports (`utils`, `config`) resolve correctly.

---

## Requirements & installation

1. Create & activate a Python virtual environment (recommended):

   ```bash
   python -m venv .venv
   source .venv/bin/activate          # macOS / Linux
   .venv\Scripts\activate             # Windows (PowerShell/cmd)
   ```

2. Install dependencies:

   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

3. Ensure you have a `.env` file in project root describing your Apify API key and actor IDs (see next section).

---

## .env and configuration

The project uses `python-dotenv` and `config/settings.py`. Copy `.env.example` → `.env` and populate:

```
APIFY_API_KEY=apify_api_...
AMAZON_ACTOR=BG3WDrGdteHgZgbPK
EBAY_ACTOR=PBSxkfoBWghbE2set
ETSY_ACTOR=MiEVd9O3R4Td5AbV9
WALMART_ACTOR=dKylGAj0fF0pCjGeW
JUMIA_ACTOR=SqE6Cg7U75yiYDAs4
ALIBABA_ACTOR=5bWFgJNW09lFlCPU2
```

`config/settings.py` exposes:

- `settings.DATA_DIR` (default `data/`) — where scrapers save results.
- `settings.LOGS_DIR` (default `logs/`) — where logger writes per-scraper logs.

You may also set runtime flags such as `DEFAULT_WAIT_FOR_FINISH` or memory sizing if you modify the settings.

---

## How the scrapers work (usage examples)

Each scraper is **standalone** and callable as a CLI or as a module. They were designed to be run individually (one-off runs).

### Examples

From the project root:

- Run eBay scraper (categories file, one category per line):

  ```bash
  python scripts/ebay.py --categories-file categories.txt --max-items 100
  ```

- Run Amazon scraper:

  ```bash
  python scripts/amazon.py --categories-file categories.txt --max-items 200
  ```

- Run Jumia scraper:

  ```bash
  python scripts/jumia.py --categories-file categories.txt
  ```

- Run a single-site run programmatically (from code):

  ```py
  from scripts.amazon import run  # run(categories_list, max_items=..., proxy_use_apify=True)
  run(["moringa oil", "coconut oil"], max_items=100)
  ```

**Important:** run from project root so imports like `from utils.logger import get_logger` resolve. If you prefer to run a script directly from `scripts/`, set `PYTHONPATH` appropriately or use `python -m scripts.ebay` to run as a module.

---

## Logging and output locations

- **Logs:** default directory: `logs/` (can be changed via `.env` / settings).
  Each scraper uses the logger util, and log file names are per-scraper (e.g., `logs/amazon.log`, `logs/ebay.log`, `logs/jumia.log`). The logger utility supports rotation (size/time), configurable formatting and optional console output.

- **Raw dataset output (per-scraper):** default directory: `data/<actor_key>/raw/`.
  The Apify helper saves the dataset result as JSON in:

  ```
  data/<actor_key>/raw/<actor_key>.dataset.json
  ```

  Example: `data/ebay/raw/ebay.dataset.json`

- **Where to place Batch input/output:**

  - After scraping and (optionally) merging, convert or save dataset(s) to JSONL for Azure Batch input.
  - You (manually) upload the JSONL(s) in the Azure Portal (Azure AI Foundry / OpenAI Batch UI) or use the Azure file upload APIs and create a Batch job.
  - After the job completes download the **output JSONL** and place it somewhere convenient, e.g. `data/batch_output/amazon.normalized.jsonl`.

- **Normalized CSV output (final):**

  - Use `scripts/process_batch.py` to convert the Batch output JSONL into a CSV whose header exactly matches the canonical CSV template. By default it writes `*.csv` next to the JSONL (replacing `.jsonl` suffix with `.csv`), e.g. `data/batch_output/amazon.normalized.csv`.

---

## Apify Actors — what they are and costs used

**Actors** on Apify are pre-built scraping / automation programs you can rent and run on Apify's platform. In this project we relied on **actor rentals** (each actor has its own monthly rental fee in addition to any Apify subscription or account usage). When you run an actor on Apify you may also consume Apify compute units / proxy credits — the repo assumes you have an Apify account & API token configured in `.env`.

> **Important:** Apify gives an initial free credit ($5) for new accounts. Actor rentals and further usage beyond free credits are billed separately. Actor monthly rental fees listed below are _actor owners' rental prices_ (charged by Apify per actor) and not included in Apify platform subscription fees.

Actors used in this project (with actor rental fee and trial info):

- **Jumia actor** — **$19.99 / month**, free trial: **2 hours**
  [https://console.apify.com/actors/SqE6Cg7U75yiYDAs4](https://console.apify.com/actors/SqE6Cg7U75yiYDAs4)

- **Amazon actor** — **$40 / month**, free trial: **14 days**
  [https://console.apify.com/actors/BG3WDrGdteHgZgbPK](https://console.apify.com/actors/BG3WDrGdteHgZgbPK)

- **eBay actor** — **$50 / month**, free trial: **3 days**
  [https://console.apify.com/actors/PBSxkfoBWghbE2set](https://console.apify.com/actors/PBSxkfoBWghbE2set)

- **Etsy actor** — **$30 / month**, free trial: **3 days**
  [https://console.apify.com/actors/MiEVd9O3R4Td5AbV9](https://console.apify.com/actors/MiEVd9O3R4Td5AbV9)

- **Walmart actor** — **$30 / month**, free trial: **3 days**
  [https://console.apify.com/actors/dKylGAj0fF0pCjGeW](https://console.apify.com/actors/dKylGAj0fF0pCjGeW)

- **Alibaba actor** — **$30 / month**, free trial: **1 day**
  [https://console.apify.com/actors/5bWFgJNW09lFlCPU2](https://console.apify.com/actors/5bWFgJNW09lFlCPU2)

**Note:** It is often possible to complete your scraping workload during the actors’ free trial windows (if your volume & runtime are small enough). If you require longer runs or heavier scraping you will exhaust those trial periods and will then be charged per-month rental fees and any Apify usage beyond free credits.

**Actors vs "rental" vs "call":**

- _Actor_ = a published automation/scraper.
- _Renting_ an actor (monthly) grants access according to the owner's pricing; you still pay for compute (task runs) and proxies separately (depending on account).
- Each `run` triggered from the repo calls `utils.apify_client.ApifySdkClient.call_actor()` which uses the `apify_client` SDK to start/run the actor and (optionally) wait for completion.

---

## Post-scraping: Normalization (Azure OpenAI + Azure Batch)

**Goal:** convert each site's JSON output (different shapes) into a single canonical format that matches this CSV header:

```
platform,date,product ID,name,scientific name,product form,net quantity,unit,price,price per unit,seller name,seller type,seller origin,number of reviews,average rating,sales rank or badge,certifications,claims,transparency origin,cold pressed,steam distilled,refined,list of ingredients,image,product description,return policy,collection method
```

**How we do it (high level):**

1. Create a strict prompt + examples (the project includes prompt templates in `utils/prompt.py`) that instruct an LLM to output JSON with those exact keys.
2. Prepare inputs: one product record per call. For bulk (>1000 records) we use **Azure OpenAI Batch**: upload a JSONL file where each line is a single prompt-based input. (This project assumes you will create/upload batch jobs in the Azure portal — the repo includes a script to process the job output but **does not** create or upload batch jobs programmatically.)
3. Download the **output JSONL** once the batch job completes. Each output line contains a `response` with `choices[0].message.content` — that content is a JSON string matching the template (when model succeeded).
4. Convert the output JSONL → CSV using `scripts/process_batch.py`.

**Azure Batch docs & guidance:** the repo authors used Azure OpenAI Batch via the Azure AI Foundry portal (upload JSONL, create batch job, download results). See Microsoft's docs for how to prepare/upload JSONL and create/poll batch jobs. ([Microsoft Learn][1])

> The repo intentionally keeps Batch upload/creation as a manual portal step because account and storage setup varies widely (Blob + SAS vs direct upload). See the Azure Batch docs for the recommended upload flows and REST API if you want to automate it. ([Microsoft Learn][1])

---

## Process Batch output → CSV (what to run locally)

1. After the Azure Batch job completes, download the `output.jsonl` file from Azure portal (or the output file ID via REST). Put it somewhere like:

   ```
   data/batch_output/amazon.normalized.jsonl
   ```

2. Ensure you have a template CSV header file (CSV with a single header row matching the exact canonical header). For example:

   ```
   templates/normalized_template.csv
   ```

   (If you do not have one, create a CSV file whose first row is exactly the header line shown above.)

3. Run the conversion tool:

   ```bash
   python scripts/process_batch.py --batch-jsonl data/batch_output/amazon.normalized.jsonl --template-csv templates/normalized_template.csv
   ```

   This will write `data/batch_output/amazon.normalized.csv` (a CSV whose columns match the canonical header). The script:

   - extracts assistant `content` field from every JSONL response line,
   - parses the JSON string,
   - validates keys and fills missing fields with `null`/empty,
   - serializes arrays/dicts into JSON strings for CSV cells when necessary.

4. If any line fails parsing, that line is written as a placeholder (the script logs warnings). You can opt to retry such lines synchronously against Azure OpenAI or review them manually.

---

## Troubleshooting & common issues

- **`ModuleNotFoundError: No module named 'utils'`**
  This usually means you ran the scraper from the `scripts/` directory. Run scripts from the **project root** so package-relative imports work:

  ```bash
  # from project root
  python scripts/ebay.py --categories-file categories.txt
  # or, run as a module (safer)
  python -m scripts.ebay --categories-file categories.txt
  ```

  Alternatively, set `PYTHONPATH` to the project root: `export PYTHONPATH=.`, or add the project root to sys.path in your shell / IDE.

- **Actor run fails / KeyError from logger when passing dict via logger.extra**
  The logger in `utils/logger.py` reserves some keys (e.g., `module`, `name`). Avoid passing those in the `extra` dict or remove certain keys before logging. The included `get_logger()` follows a restrictive LogRecord policy — use `logger.info("msg", extra={"details": mydict})` rather than using `module` or `name` keys.

- **Dataset not downloaded or empty**
  Check Apify run metadata log (the run object) and verify `defaultDatasetId` present. The helper `utils.apify_client` downloads the dataset to `data/<actor>/raw/<actor>.dataset.json`. Inspect that file.

- **Model returns non-JSON or empty `content`**

  - Confirm `temperature=0.0` for deterministic outputs.
  - Ensure the model deployment is a **chat-capable** deployment (we recommend using a chat-style or batch-enabled deployment).
  - If the batch output lines contain the assistant output in other fields (e.g., `tool_calls` or `annotations`) check the raw JSONL line and adjust parsing. `scripts/process_batch.py` is tolerant and tries to extract a JSON substring inside `response.body.choices[0].message.content`.

---

## Notes, cost tips & disclaimers

- **Actor fees are monthly rentals** (listed earlier) and are **charged by actor owners on Apify**, separate from Apify account credits. A small project that finishes during free trials may avoid monthly rental charges, but long/large crawls will incur fees.
- **Azure Batch** is cost-efficient for bulk normalization because it processes large JSONL files in a single job (cheaper per item than many individual synchronous calls) — consult Azure docs for details and cost calculation. ([Microsoft Learn][1])
- **Accuracy:** we use Azure OpenAI for semantic normalization. For highest accuracy, provide many in-prompt examples and run a small validation pass (sample 100 records) to verify mapping rules before processing the entire dataset.

---

## Where to go next / automation ideas

- Automate Blob upload + Batch job creation using Azure SDK + small wrapper script (requires setting up Blob container & SAS keys). The repo intentionally keeps this manual because your Azure & security posture may vary.
- Add a re-try worker that re-processes lines that failed to parse (you can attempt synchronous Azure calls for those single items).
- Add unit tests around `utils/normalize.py` using real scraped JSON snippets to validate model prompt + postprocessing.

---

## License

This project is provided under the included `LICENSE` file in the repo. Check that file for the exact license text.

---

## Useful links / docs

- Azure OpenAI / Batch getting started: Microsoft docs — upload JSONL, create batch job, retrieve outputs. ([Microsoft Learn][1])
- Azure OpenAI REST Batch endpoint reference. ([Microsoft Learn][2])

[1]: https://learn.microsoft.com/en-us/azure/ai-services/cognitive-services-azure-openai/how-to/batch-requests
[2]: https://learn.microsoft.com/en-us/azure/ai-services/cognitive-services-azure-openai/reference#batch
