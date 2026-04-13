# azure-python-automation

Python automation scripts for Microsoft Azure platform operations, data engineering workflows, and identity management — built for production use on enterprise Azure environments.

---

## 📋 Contents

| Script | Description |
|--------|-------------|
| [`adf_report_generator.py`](./adf_report_generator.py) | Multi-sheet Excel report covering ADF pipeline relationships, trigger schedules, and run history |

---

## 🔍 adf_report_generator.py

### What it does

Connects to an Azure Data Factory instance and generates a three-sheet Excel report:

| Sheet | Contents |
|-------|----------|
| **Parent-Child Relationships** | Every `ExecutePipeline` activity found across all pipelines, including nested control flow (ForEach, IfCondition, Until, Switch) |
| **Pipeline Status** | Full pipeline inventory with Parent / Child / Standalone classification |
| **Pipeline Triggers** | Trigger names, schedules, runtime state, last 45-day run history, and timezone-converted execution times |

### Key features

- **Recursive activity walker** — handles arbitrarily deep nesting of ForEach, IfCondition, Until, and Switch activities to find all ExecutePipeline calls
- **Parallel run fetching** — fetches run history for all pipelines concurrently using a configurable thread pool with per-thread rate limiting (5 req/sec) to stay within Azure ARM throttle limits
- **Smart token management** — proactively refreshes Azure bearer tokens 5 minutes before expiry, preventing mid-run authentication failures on large factories
- **Exponential backoff retry** — handles HTTP 429 (throttled), 503 (unavailable), and 401 (auth expired) with configurable retries and Retry-After header support
- **Multi-timezone schedule display** — converts trigger schedule times from their configured timezone (Windows or IANA) to a target output timezone, with full DST awareness using today's date for offset resolution
- **Optimised API usage** — fetches only the latest run per pipeline (first page, ordered DESC) rather than paginating all history

### Architecture

```
main()
 └─ build_reports()
     ├─ [1/5] fetch_all_pipeline_definitions_batch()
     │         list_by_factory() → .get() per pipeline (full nested activities)
     │
     ├─ [2/5] walk_activities()  [recursive]
     │         ExecutePipeline → records parent-child edge
     │         ForEach / IfCondition / Until / Switch → recurse into branches
     │
     ├─ [3/5] triggers.list_by_factory()
     │         extract_schedule_details() → timezone conversion
     │
     ├─ [4/5] fetch_runs_parallel_with_limit()
     │         query_pipeline_runs_optimized()  [per pipeline, threaded]
     │         @retry_with_backoff → exponential backoff on errors
     │
     └─ [5/5] Assemble DataFrames → write Excel (openpyxl)
```

---

## ⚙️ Setup

### Prerequisites

- Python 3.9+
- Azure CLI (`az`) installed and logged in, **or** a Service Principal with `Reader` + `Data Factory Contributor` access

### Install dependencies

```bash
pip install -r requirements.txt
```

### Configure

**Option A — Config file (recommended for local use)**

```bash
cp config.yaml.example config.yaml
# Edit config.yaml with your subscription_id, resource_group, factory_name
```

**Option B — Environment variables (recommended for CI/CD and automation)**

```bash
cp .env.example .env
# Edit .env with your values
# Then run with --env flag
```

### Authenticate

```bash
# Azure CLI login (easiest for local development)
az login

# Or set Service Principal env vars
export AZURE_CLIENT_ID=<your-client-id>
export AZURE_CLIENT_SECRET=<your-client-secret>
export AZURE_TENANT_ID=<your-tenant-id>
```

---

## 🚀 Usage

```bash
# Basic run
python adf_report_generator.py -c config.yaml

# Parallel mode — recommended for factories with 50+ pipelines
python adf_report_generator.py -c config.yaml --parallel

# Custom output path and lookback window
python adf_report_generator.py -c config.yaml --parallel -o my_report.xlsx -d 30

# Load config from environment variables
python adf_report_generator.py --env --parallel

# Verbose — shows each pipeline as it's processed
python adf_report_generator.py -c config.yaml --parallel -v
```

### Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `-c, --config` | — | Path to config.yaml |
| `--env` | — | Load config from environment variables |
| `-o, --out` | `ADF_Pipeline_Report.xlsx` | Output file path |
| `-d, --days` | `45` | Run history lookback window in days |
| `--parallel` | off | Enable parallel run fetching (recommended) |
| `--max-workers` | `6` | Number of parallel threads |
| `-v, --verbose` | off | Print per-pipeline status |

---

## 🔐 Security

**Credentials are never hardcoded.** All sensitive values are loaded from:
- `config.yaml` (excluded from Git via `.gitignore`)
- Environment variables (`.env` file, also excluded)
- `DefaultAzureCredential` (Azure CLI / Managed Identity / Service Principal)

The `.gitignore` in this repo excludes `config.yaml`, `.env`, `*.xlsx`, `*.csv`, and all other files that could contain real Azure resource names or run data.

---

## 📦 Dependencies

| Package | Purpose |
|---------|---------|
| `azure-identity` | DefaultAzureCredential authentication |
| `azure-mgmt-datafactory` | ADF management API client |
| `pandas` | DataFrame assembly and Excel writing |
| `openpyxl` | Excel file engine |
| `requests` | Direct ARM REST API calls for run history |
| `pyyaml` | Config file parsing |
| `python-dateutil` | Datetime string parsing |

Optional: `tzdata` — improves Windows timezone resolution coverage beyond the built-in map.

---

## 📜 Related Certifications

This script applies concepts from:
- **AZ-104** — Azure resource management, subscription/resource group structure
- **AZ-500** — Managed Identities, RBAC, DefaultAzureCredential patterns
- **DP-900** — Azure Data Factory architecture and pipeline concepts

---

## 🗂️ Repo Structure

```
azure-python-automation/
├── adf_report_generator.py   # Main script
├── config.yaml.example       # Config template (copy to config.yaml)
├── .env.example              # Env var template (copy to .env)
├── requirements.txt          # Python dependencies
├── .gitignore                # Excludes credentials and output files
└── README.md
```

---

## 👤 Author

**Aquib Aziz** — Azure Data Platform & Cloud Engineer  
[LinkedIn](https://linkedin.com/in/aquibaziz) · [GitHub](https://github.com/aquibaziz)
