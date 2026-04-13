# azure-python-automation

Python automation scripts for Microsoft Azure platform operations, data engineering workflows, and identity management — built for production use on enterprise Azure environments.

---

## 📋 Scripts

| Script | Description | Output |
|--------|-------------|--------|
| [`adf_report_generator.py`](#1-adf_report_generatorpy) | ADF pipeline parent-child map, trigger schedules, and 45-day run history | Excel (3 sheets) |
| [`adf_linked_service_inventory.py`](#2-adf_linked_service_inventorypy) | Maps every ADF linked service to datasets, pipelines, and Key Vault dependencies | Excel (2 sheets) |
| [`adf_trigger_raw_extractor.py`](#3-adf_trigger_raw_extractorpy) | Extracts raw trigger run history from Legacy and Migrated ADF environments | Excel (raw sheets) |
| [`adf_trigger_comparison.py`](#4-adf_trigger_comparisonpy) | Builds a formatted performance comparison from the raw extractor output | Excel (comparison sheet) |
| [`adls_acl_extractor.py`](#5-adls_acl_extractorpy) | Recursively scans ADLS Gen2 to find all paths where a Managed Identity has ACL entries | CSV |

> **Scripts 3 and 4 work as a pipeline:** run `adf_trigger_raw_extractor.py` first to collect data, then `adf_trigger_comparison.py` to build the comparison report from that data.

---

## 1. adf_report_generator.py

### What it does

Generates a three-sheet Excel report for an Azure Data Factory instance:

| Sheet | Contents |
|-------|----------|
| **Parent-Child Relationships** | Every `ExecutePipeline` activity across all pipelines, including nested control flow (ForEach, IfCondition, Until, Switch) |
| **Pipeline Status** | Full pipeline inventory with Parent / Child / Standalone classification |
| **Pipeline Triggers** | Trigger names, schedules, runtime state, last 45-day run history, timezone-converted execution times |

### Key features

- **Recursive activity walker** — handles arbitrarily deep nesting of ForEach, IfCondition, Until, Switch
- **Parallel run fetching** — thread pool with per-thread rate limiting (5 req/sec) to stay within ARM throttle limits
- **Smart token management** — proactively refreshes bearer tokens 5 minutes before expiry
- **Exponential backoff retry** — handles HTTP 429 / 503 / 401 with Retry-After header support
- **Multi-timezone schedule display** — converts trigger times from source timezone to target with full DST awareness
- **Optimised API usage** — fetches only the latest run per pipeline (ordered DESC, first page only)

### Architecture

```
main()
 └─ build_reports()
     ├─ [1/5] fetch_all_pipeline_definitions_batch()
     │         list_by_factory() + .get() per pipeline (loads full nested activities)
     ├─ [2/5] walk_activities()  [recursive]
     │         ExecutePipeline → parent-child edge
     │         ForEach / IfCondition / Until / Switch → recurse
     ├─ [3/5] triggers.list_by_factory() + extract_schedule_details()
     ├─ [4/5] fetch_runs_parallel_with_limit()  [threaded + rate-limited]
     └─ [5/5] Assemble DataFrames → write Excel
```

### Usage

```bash
python adf_report_generator.py -c config.yaml --parallel
python adf_report_generator.py -c config.yaml --parallel -o report.xlsx -d 30
python adf_report_generator.py --env --parallel -v
```

| Argument | Default | Description |
|----------|---------|-------------|
| `-c, --config` | — | Path to config.yaml |
| `--env` | — | Load config from environment variables |
| `-o, --out` | `ADF_Pipeline_Report.xlsx` | Output file |
| `-d, --days` | `45` | Run history lookback in days |
| `--parallel` | off | Enable parallel run fetching |
| `--max-workers` | `6` | Parallel thread count |
| `-v, --verbose` | off | Per-pipeline status output |

---

## 2. adf_linked_service_inventory.py

### What it does

Enumerates every linked service in an ADF instance and maps its full dependency graph:

| Sheet | Contents |
|-------|----------|
| **Detail** | Linked Service → Dataset → Container/Table → Folder Path → Pipeline |
| **LS_Relationships** | Linked Service → Type → All related datasets and pipelines |

### Key features

- **Full dependency chain** — traces LS → dataset → pipeline in both directions
- **Key Vault dependency detection** — propagates pipeline associations from dependent linked services to the KV linked service
- **Direct activity LS references** — catches Copy / Lookup / GetMetadata activities that reference linked services directly (bypassing datasets)
- **Trigger schedule enrichment** — annotates pipeline associations with active trigger metadata
- **Multi-type support** — AzureBlobStorage, ADLS, AzureSqlDatabase, Oracle, AzureKeyVault, and others

### Architecture

```
main()
 ├─ [1/6] build_linked_service_map()    Enumerate LS, extract URLs, detect KV deps
 ├─ [2/6] attach_datasets()             Map datasets → parent LS + container/folder metadata
 ├─ [3/6] attach_pipeline_references()  Scan activities for direct + dataset-mediated LS refs
 ├─ [4/6] build_trigger_schedule_map()  Pipeline → schedule metadata (started triggers only)
 ├─ [5/6] enrich_pairs_with_schedule() + propagate_keyvault_pipeline_refs()
 └─ [6/6] build_sheet1() + build_sheet2() → write Excel
```

### Usage

```bash
python adf_linked_service_inventory.py -c config.yaml
python adf_linked_service_inventory.py --env -o my_inventory.xlsx
```

| Argument | Default | Description |
|----------|---------|-------------|
| `-c, --config` | `~/config.yaml` | Path to config.yaml |
| `--env` | — | Load config from environment variables |
| `-o, --out` | auto-timestamped | Output Excel file |

---

## 3. adf_trigger_raw_extractor.py

### What it does

Extracts trigger run history from one or both ADF environments (Legacy and Migrated) and writes raw data sheets used by `adf_trigger_comparison.py`.

| Sheet written | Contents |
|--------------|----------|
| **Legacy_Raw** | Trigger run data from the legacy ADF environment |
| **Migrated_Raw** | Trigger run data from the migrated ADF environment |

Output columns: Trigger Name, Pipeline Name, Run Start/End Time, Status, Duration (s), Duration (m), Run Date, Environment.

### Key features

- **Full pagination** — handles `continuationToken` for large trigger run windows
- **Parallel pipeline run fetching** — thread pool with per-thread HTTP session pooling
- **Smart token management** — proactive refresh before expiry with force-refresh on 401
- **Retry with backoff** — handles 429, 5xx, and connection errors with Retry-After support
- **invokedBy validation** — skips pipeline runs where the invoker doesn't match the trigger (prevents cross-trigger data contamination)
- **Drop diagnostics** — reports count of skipped/errored runs by reason at completion

### Architecture

```
main()
 ├─ extract(Legacy)    ─┐
 │   ├─ query_trigger_runs()          paginated POST /queryTriggerRuns
 │   └─ ThreadPoolExecutor            parallel GET /pipelineruns/{id}
 │       └─ process_trigger_run()     validate + parse + build row
 │
 ├─ write_sheet(Legacy_Raw)
 ├─ extract(Migrated)  ─┘  (same flow)
 └─ write_sheet(Migrated_Raw)
```

### Required environment variables

```bash
# Legacy environment
export LEGACY_SUBSCRIPTION_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export LEGACY_RESOURCE_GROUP=your-legacy-resource-group
export LEGACY_FACTORY_NAME=your-legacy-adf-name

# Migrated environment
export MIGRATED_SUBSCRIPTION_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export MIGRATED_RESOURCE_GROUP=your-migrated-resource-group
export MIGRATED_FACTORY_NAME=your-migrated-adf-name
```

### Usage

```bash
# Both environments, last 45 days (default)
python adf_trigger_raw_extractor.py

# Last 30 days
python adf_trigger_raw_extractor.py --days 30

# Specific date range
python adf_trigger_raw_extractor.py --start 2024-01-01 --end 2024-01-31

# Legacy environment only
python adf_trigger_raw_extractor.py --env legacy

# Migrated environment only
python adf_trigger_raw_extractor.py --env migrated

# Custom output and parallelism
python adf_trigger_raw_extractor.py --output my_data.xlsx --workers 4
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--start` | — | Start date (YYYY-MM-DD) |
| `--end` | — | End date (YYYY-MM-DD) |
| `--days` | `45` | Days back from today |
| `--env` | `both` | `legacy` / `migrated` / `both` |
| `--output` | `ADF_Trigger_Raw.xlsx` | Output file path |
| `--workers` | `2` | Parallel worker threads |

---

## 4. adf_trigger_comparison.py

### What it does

Reads the `Legacy_Raw` and `Migrated_Raw` sheets produced by `adf_trigger_raw_extractor.py` and builds a formatted, colour-coded Comparison sheet.

| Column group | Contents |
|-------------|----------|
| **Legacy Metrics** | Succeeded run count + average duration (seconds) per trigger |
| **Migrated Metrics** | Succeeded run count + average duration (seconds) per trigger |
| **Comparison** | Verdict + difference in minutes |

Verdict logic:

| Verdict | Condition |
|---------|-----------|
| `Faster` | Migrated avg ≤ Legacy avg × (1 − threshold) |
| `Slower` | Migrated avg ≥ Legacy avg × (1 + threshold) |
| `Comparable` | Within threshold |
| `Insufficient Runs` | Migrated run count < min_runs |
| `No Migrated Data` | Migrated run count = 0 |
| `No Legacy Baseline` | Legacy run count = 0 |

### Key features

- **Excel formula-based** — COUNTIFS / AVERAGEIFS formulas reference the raw sheets directly, so the comparison auto-updates if raw data is refreshed
- **Colour-coded verdicts** — conditional formatting highlights Faster (green), Slower (red), Comparable (amber), edge cases (grey/orange/blue)
- **Configurable thresholds** — `--min-runs` and `--threshold` via CLI arguments
- **Hidden spacer columns** — E–H and K–P hidden to create clean visual grouping

### Usage

```bash
# Default (reads ADF_Trigger_Raw.xlsx)
python adf_trigger_comparison.py

# Custom input file
python adf_trigger_comparison.py --input my_data.xlsx

# Custom thresholds
python adf_trigger_comparison.py --input my_data.xlsx --min-runs 3 --threshold 0.15
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--input` | `ADF_Trigger_Raw.xlsx` | Input workbook (from raw extractor) |
| `--min-runs` | `5` | Minimum migrated runs for a valid verdict |
| `--threshold` | `0.10` | % difference for Faster/Slower (0.10 = 10%) |

### Two-script workflow

```bash
# Step 1: Extract raw data (writes Legacy_Raw and Migrated_Raw sheets)
python adf_trigger_raw_extractor.py --days 45 --output ADF_Trigger_Raw.xlsx

# Step 2: Build comparison report (writes Comparison sheet to same file)
python adf_trigger_comparison.py --input ADF_Trigger_Raw.xlsx
```

---

## 5. adls_acl_extractor.py

### What it does

Recursively scans an ADLS Gen2 container and extracts every path where a specific Managed Identity (or Service Principal) has an explicit ACL entry.

Output CSV columns: `storage_account`, `container`, `path`, `object_type`, `principal_id`, `principal_type`, `permissions`, `is_default`

### Key features

- **Full recursive scan** — traverses all directories and files via `get_paths(recursive=True)`
- **Four-tier MSI resolution** — accepts Object ID GUID, UAMI name, `name|resourceGroup`, or SP display name
- **Optional mask and special entries** — `--include-mask` and `--include-special` for effective permission context
- **Depth limiting** — `--max-depth N` caps recursion for large containers
- **Graceful error handling** — permission-denied and HTTP errors are counted and skipped, not fatal

### Use cases

- Audit all paths where an MSI has been granted access (pre/post migration validation)
- Identify over-privileged identities across deep folder hierarchies
- Generate evidence for zero-trust access reviews and security compliance

### Usage

```bash
# Interactive
python adls_acl_extractor.py --interactive

# Non-interactive
python adls_acl_extractor.py \
  --storage-account mystorageaccount \
  --container mycontainer \
  --msi my-managed-identity-name

# Object ID directly (fastest)
python adls_acl_extractor.py \
  --storage-account mystorageaccount \
  --container mycontainer \
  --msi xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# With mask + special entries, directories only, max 3 levels
python adls_acl_extractor.py ... --include-mask --include-special \
  --exclude-files --max-depth 3
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--storage-account` | — | Storage account name |
| `--container` | — | Container name |
| `--msi` | — | MSI name or Object ID GUID |
| `--path` | — | Scan specific path only |
| `--root-only` | off | Scan container root only |
| `--exclude-files` | off | Skip files, scan directories only |
| `--max-depth` | `0` (unlimited) | Max recursion depth |
| `--output` | auto-generated | Output CSV path |
| `--include-mask` | off | Include mask entries at matched paths |
| `--include-special` | off | Include owning user/group/other at matched paths |
| `--debug` | off | Verbose debug output |
| `--interactive` | off | Prompt for inputs |

---

## ⚙️ Setup

### Prerequisites

- Python 3.9+
- Azure CLI installed and authenticated: `az login`

### Install dependencies

```bash
pip install -r requirements.txt
```

### Configure (ADF scripts)

```bash
# Option A: config file
cp config.yaml.example config.yaml
# Edit config.yaml with your subscription_id, resource_group, factory_name

# Option B: environment variables
cp .env.example .env
# Edit .env, then use --env flag on scripts that support it
```

---

## 🔐 Security

Credentials are never hardcoded. All sensitive values load from `config.yaml` or environment variables — both excluded from Git via `.gitignore`. Output files (`*.xlsx`, `*.csv`) are also excluded since they may contain real Azure resource names and run data.

---

## 📦 Dependencies

| Package | Scripts | Purpose |
|---------|---------|---------|
| `azure-identity` | all | DefaultAzureCredential / AzureCliCredential |
| `azure-mgmt-datafactory` | scripts 1, 2 | ADF management API |
| `azure-storage-file-datalake` | script 5 | ADLS Gen2 filesystem + ACL access |
| `pandas` | scripts 1, 2 | DataFrame assembly + Excel writing |
| `openpyxl` | all | Excel read/write |
| `requests` | scripts 1, 3 | Direct ARM REST API calls |
| `pyyaml` | scripts 1, 2 | Config file parsing |
| `python-dateutil` | script 1 | Datetime string parsing |

---

## 🗂️ Repo structure

```
azure-python-automation/
├── adf_report_generator.py          # Pipeline report: parent-child, triggers, run history
├── adf_linked_service_inventory.py  # Linked service dependency mapper
├── adf_trigger_raw_extractor.py     # Raw trigger run data extractor (Legacy + Migrated)
├── adf_trigger_comparison.py        # Performance comparison report builder
├── adls_acl_extractor.py            # ADLS Gen2 Managed Identity ACL scanner
├── config.yaml.example              # Config template (copy to config.yaml)
├── .env.example                     # Env var template (copy to .env)
├── requirements.txt                 # Python dependencies
├── .gitignore                       # Excludes credentials and output files
└── README.md
```

---

## 📜 Related certifications

- **AZ-104** — Azure resource management, storage, subscription/RG structure
- **AZ-500** — Managed Identities, RBAC, ADLS Gen2 ACLs, Key Vault, zero-trust
- **DP-900** — Azure Data Factory, linked services, datasets, pipelines

---

## 👤 Author

**Aquib Aziz** — Azure Data Platform & Cloud Engineer  
[LinkedIn](https://linkedin.com/in/aquibaziz) · [GitHub](https://github.com/aquibaziz)
