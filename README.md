# azure-python-automation

Python automation scripts for Microsoft Azure platform operations, data engineering workflows, and identity management — built for production use on enterprise Azure environments.

---

## 📋 Scripts

| Script | Description |
|--------|-------------|
| [`adf_report_generator.py`](#adf_report_generatorpy) | Multi-sheet Excel report: ADF pipeline parent-child relationships, trigger schedules, and 45-day run history |
| [`adf_linked_service_inventory.py`](#adf_linked_service_inventorypy) | Maps every ADF linked service to its datasets, pipelines, Key Vault dependencies, and trigger schedules |

---

## adf_report_generator.py

### What it does

Connects to an Azure Data Factory instance and generates a three-sheet Excel report:

| Sheet | Contents |
|-------|----------|
| **Parent-Child Relationships** | Every `ExecutePipeline` activity found across all pipelines, including nested control flow (ForEach, IfCondition, Until, Switch) |
| **Pipeline Status** | Full pipeline inventory with Parent / Child / Standalone classification |
| **Pipeline Triggers** | Trigger names, schedules, runtime state, last 45-day run history, and timezone-converted execution times |

### Key features

- **Recursive activity walker** — handles arbitrarily deep nesting of ForEach, IfCondition, Until, and Switch activities to find all ExecutePipeline calls
- **Parallel run fetching** — fetches run history for all pipelines concurrently using a configurable thread pool with per-thread rate limiting (5 req/sec)
- **Smart token management** — proactively refreshes Azure bearer tokens 5 minutes before expiry
- **Exponential backoff retry** — handles HTTP 429, 503, 401 with configurable retries and Retry-After header support
- **Multi-timezone schedule display** — converts trigger schedule times from source timezone to target timezone with full DST awareness
- **Optimised API usage** — fetches only the latest run per pipeline rather than paginating full history

### Architecture

```
main()
 └─ build_reports()
     ├─ [1/5] fetch_all_pipeline_definitions_batch()
     │         list_by_factory() → .get() per pipeline (full nested activities)
     ├─ [2/5] walk_activities()  [recursive]
     │         ExecutePipeline → parent-child edge
     │         ForEach / IfCondition / Until / Switch → recurse
     ├─ [3/5] triggers.list_by_factory() + extract_schedule_details()
     ├─ [4/5] fetch_runs_parallel_with_limit()
     │         query_pipeline_runs_optimized()  [threaded, rate-limited]
     └─ [5/5] Assemble DataFrames → write Excel
```

### Usage

```bash
# Basic run with config file
python adf_report_generator.py -c config.yaml

# Parallel mode — recommended for factories with 50+ pipelines
python adf_report_generator.py -c config.yaml --parallel

# Custom output and lookback window
python adf_report_generator.py -c config.yaml --parallel -o report.xlsx -d 30

# Load config from environment variables
python adf_report_generator.py --env --parallel

# Verbose per-pipeline output
python adf_report_generator.py -c config.yaml --parallel -v
```

### Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `-c, --config` | — | Path to config.yaml |
| `--env` | — | Load config from environment variables |
| `-o, --out` | `ADF_Pipeline_Report.xlsx` | Output file path |
| `-d, --days` | `45` | Run history lookback in days |
| `--parallel` | off | Enable parallel run fetching |
| `--max-workers` | `6` | Parallel thread count |
| `-v, --verbose` | off | Per-pipeline status output |

---

## adf_linked_service_inventory.py

### What it does

Enumerates every linked service in an ADF instance and maps it to its full dependency graph — datasets, pipelines, Key Vault credentials, and trigger schedules.

| Sheet | Contents |
|-------|----------|
| **Detail** | Linked Service → Dataset → Container/Table → Folder Path → Pipeline |
| **LS_Relationships** | Linked Service → Type → All related datasets and pipelines |

### Key features

- **Full dependency chain** — traces LS → dataset → pipeline in both directions
- **Key Vault dependency detection** — identifies which linked services retrieve credentials from Key Vault and propagates pipeline associations to the KV LS
- **Direct activity LS references** — catches pipelines that reference linked services directly (bypassing datasets) via `linkedServiceName` on Copy, Lookup, GetMetadata activities
- **Trigger schedule enrichment** — annotates pipeline associations with active trigger schedule metadata (frequency, interval, timezone, recurrence days/hours)
- **Multi-type support** — handles AzureBlobStorage, AzureDataLakeStore, AzureSqlDatabase, Oracle, AzureKeyVault, and others gracefully

### Architecture

```
main()
 ├─ [1/6] build_linked_service_map()
 │         Enumerates LS, extracts connection URLs, detects KV dependencies
 ├─ [2/6] attach_datasets()
 │         Maps datasets → parent LS, extracts container + folder metadata
 ├─ [3/6] attach_pipeline_references()
 │         Scans all activities for direct LS refs + dataset-mediated refs
 ├─ [4/6] build_trigger_schedule_map()
 │         Builds pipeline → schedule metadata map (started triggers only)
 ├─ [5/6] enrich_pairs_with_schedule()
 │         + propagate_keyvault_pipeline_refs()
 └─ [6/6] build_sheet1() + build_sheet2() → write Excel
```

### Usage

```bash
# Default (reads ~/config.yaml)
python adf_linked_service_inventory.py

# Explicit config path
python adf_linked_service_inventory.py --config path/to/config.yaml

# Load from environment variables
python adf_linked_service_inventory.py --env

# Custom output path
python adf_linked_service_inventory.py -o my_inventory.xlsx
```

### Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `-c, --config` | `~/config.yaml` | Path to config.yaml |
| `--env` | — | Load config from environment variables |
| `-o, --out` | auto-timestamped | Output Excel file path |

---

## ⚙️ Setup

### Prerequisites

- Python 3.9+
- Azure CLI (`az`) installed and logged in — `az login`

### Install dependencies

```bash
pip install -r requirements.txt
```

### Configure

**Option A — Config file**

```bash
cp config.yaml.example config.yaml
# Edit config.yaml — fill in subscription_id, resource_group, factory_name
```

**Option B — Environment variables**

```bash
cp .env.example .env
# Edit .env — then run scripts with --env flag
```

### Authenticate

```bash
# Azure CLI (simplest for local development)
az login

# Or set Service Principal credentials
export AZURE_CLIENT_ID=<your-client-id>
export AZURE_CLIENT_SECRET=<your-client-secret>
export AZURE_TENANT_ID=<your-tenant-id>
# Then change AzureCliCredential() to DefaultAzureCredential() in the script
```

---

## 🔐 Security

Credentials are never hardcoded. All sensitive values load from `config.yaml` or environment variables — both excluded from Git via `.gitignore`. The `.gitignore` in this repo also excludes `*.xlsx`, `*.csv`, and output files that could contain real Azure resource names or pipeline data.

---

## 📦 Dependencies

| Package | Purpose |
|---------|---------|
| `azure-identity` | AzureCliCredential / DefaultAzureCredential |
| `azure-mgmt-datafactory` | ADF management API client |
| `pandas` | DataFrame assembly and Excel writing |
| `openpyxl` | Excel file engine |
| `requests` | Direct ARM REST API calls (adf_report_generator) |
| `pyyaml` | Config file parsing |
| `python-dateutil` | Datetime string parsing |

---

## 🗂️ Repo structure

```
azure-python-automation/
├── adf_report_generator.py          # Pipeline report: parent-child, triggers, run history
├── adf_linked_service_inventory.py  # Linked service dependency mapper
├── config.yaml.example              # Config template (copy to config.yaml)
├── .env.example                     # Env var template (copy to .env)
├── requirements.txt                 # Python dependencies
├── .gitignore                       # Excludes credentials and output files
└── README.md
```

---

## 📜 Related certifications

These scripts apply concepts covered in:
- **AZ-104** — Azure resource management, subscription/resource group structure
- **AZ-500** — Managed Identities, RBAC, Key Vault, DefaultAzureCredential patterns
- **DP-900** — Azure Data Factory architecture, linked services, datasets, pipelines

---

## 👤 Author

**Aquib Aziz** — Azure Data Platform & Cloud Engineer  
[LinkedIn](https://linkedin.com/in/aquibaziz) · [GitHub](https://github.com/aquibaziz)
