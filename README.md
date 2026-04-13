# azure-python-automation

Python automation scripts for Microsoft Azure platform operations, data engineering workflows, and identity management — built for production use on enterprise Azure environments.

---

## 📋 Scripts

| Script | Description | Auth |
|--------|-------------|------|
| [`adf_report_generator.py`](#adf_report_generatorpy) | ADF pipeline parent-child relationships, trigger schedules, 45-day run history | DefaultAzureCredential |
| [`adf_linked_service_inventory.py`](#adf_linked_service_inventorypy) | Maps every ADF linked service to datasets, pipelines, and Key Vault dependencies | AzureCliCredential |
| [`adls_acl_extractor.py`](#adls_acl_extractorpy) | Recursively scans ADLS Gen2 to find all paths where a Managed Identity has ACL entries | AzureCliCredential |

---

## adf_report_generator.py

### What it does

Generates a three-sheet Excel report for an Azure Data Factory instance:

| Sheet | Contents |
|-------|----------|
| **Parent-Child Relationships** | Every `ExecutePipeline` activity found across all pipelines, including nested control flow (ForEach, IfCondition, Until, Switch) |
| **Pipeline Status** | Full pipeline inventory with Parent / Child / Standalone classification |
| **Pipeline Triggers** | Trigger names, schedules, runtime state, last 45-day run history, and timezone-converted execution times |

### Key features

- **Recursive activity walker** — handles arbitrarily deep nesting of ForEach, IfCondition, Until, and Switch activities
- **Parallel run fetching** — concurrent thread pool with per-thread rate limiting (5 req/sec) to stay within ARM throttle limits
- **Smart token management** — proactively refreshes bearer tokens 5 minutes before expiry
- **Exponential backoff retry** — handles HTTP 429 / 503 / 401 with Retry-After header support
- **Multi-timezone schedule display** — converts trigger times from source timezone to target timezone with DST awareness
- **Optimised API usage** — fetches only the latest run per pipeline (first page, ordered DESC)

### Architecture

```
main()
 └─ build_reports()
     ├─ [1/5] fetch_all_pipeline_definitions_batch()
     │         list_by_factory() → .get() per pipeline (loads full nested activities)
     ├─ [2/5] walk_activities()  [recursive]
     │         ExecutePipeline → parent-child edge
     │         ForEach / IfCondition / Until / Switch → recurse into branches
     ├─ [3/5] triggers.list_by_factory() + extract_schedule_details()
     ├─ [4/5] fetch_runs_parallel_with_limit()
     │         query_pipeline_runs_optimized()  [threaded + rate-limited]
     └─ [5/5] Assemble DataFrames → write Excel (3 sheets)
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
| `-o, --out` | `ADF_Pipeline_Report.xlsx` | Output file path |
| `-d, --days` | `45` | Run history lookback in days |
| `--parallel` | off | Enable parallel run fetching (recommended) |
| `--max-workers` | `6` | Parallel thread count |
| `-v, --verbose` | off | Per-pipeline status output |

---

## adf_linked_service_inventory.py

### What it does

Enumerates every linked service in an ADF instance and maps its full dependency graph:

| Sheet | Contents |
|-------|----------|
| **Detail** | Linked Service → Dataset → Container/Table → Folder Path → Pipeline |
| **LS_Relationships** | Linked Service → Type → All related datasets and pipelines |

### Key features

- **Full dependency chain** — traces LS → dataset → pipeline in both directions
- **Key Vault dependency detection** — identifies which linked services retrieve credentials from Key Vault and propagates pipeline associations to the KV linked service
- **Direct activity LS references** — catches pipelines that reference linked services directly (bypassing datasets) via `linkedServiceName` on Copy, Lookup, GetMetadata activities
- **Trigger schedule enrichment** — annotates pipeline associations with active trigger metadata (frequency, interval, timezone, recurrence pattern)
- **Multi-type support** — handles AzureBlobStorage, ADLS, AzureSqlDatabase, Oracle, AzureKeyVault, and others

### Architecture

```
main()
 ├─ [1/6] build_linked_service_map()     Enumerate LS, extract URLs, detect KV deps
 ├─ [2/6] attach_datasets()              Map datasets → parent LS + container/folder metadata
 ├─ [3/6] attach_pipeline_references()   Scan activities for direct + dataset-mediated LS refs
 ├─ [4/6] build_trigger_schedule_map()   Pipeline → schedule metadata (started triggers only)
 ├─ [5/6] enrich_pairs_with_schedule()
 │         + propagate_keyvault_pipeline_refs()
 └─ [6/6] build_sheet1() + build_sheet2() → write Excel (2 sheets)
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
| `-o, --out` | auto-timestamped | Output Excel file path |

---

## adls_acl_extractor.py

### What it does

Recursively scans an ADLS Gen2 container and extracts every path where a specific
Managed Identity (or Service Principal) has an explicit ACL entry.

Output: CSV file with columns:

| Column | Description |
|--------|-------------|
| `storage_account` | Storage account name |
| `container` | Container (filesystem) name |
| `path` | Full path of the scanned object |
| `object_type` | `file`, `directory`, or `container-root` |
| `principal_id` | Object ID GUID of the matched identity |
| `principal_type` | `user` or `group` |
| `permissions` | Normalised: `R`, `R,W`, `R,W,X`, `-` etc. |
| `is_default` | `True` if this is a default (inherited) ACE |

### Key features

- **Full recursive scan** — traverses all directories and files via `get_paths(recursive=True)`
- **Four-tier MSI resolution** — accepts Object ID GUID, UAMI name, `name|resourceGroup`, or SP display name
- **Optional mask and special entries** — `--include-mask` and `--include-special` add effective permission context at matched paths
- **Depth limiting** — `--max-depth N` caps recursion for large containers
- **Graceful error handling** — HTTP errors and permission-denied paths are skipped and counted, not fatal
- **Azure SDK log suppression** — verbose SDK INFO/WARNING logs silenced during scan for clean progress output

### Use cases

- Audit all paths where an MSI has been granted access (pre/post migration validation)
- Identify over-privileged identities across deep folder hierarchies
- Support access reviews and zero-trust governance for ADLS Gen2
- Generate evidence for security compliance reviews

### Architecture

```
main()
 ├─ resolve_msi_to_principal_id()   4-tier: GUID → identity show → identity list → sp list
 ├─ DataLakeServiceClient connect
 └─ scan loop
     ├─ do_scan("/")                Container root
     └─ for item in get_paths(recursive=True):
             scan_path_for_principal()
               └─ get_access_control() → parse_acl_entries() → filter by principal_id
 └─ csv.DictWriter → output CSV
```

### Usage

```bash
# Interactive — prompts for all inputs
python adls_acl_extractor.py --interactive

# Non-interactive
python adls_acl_extractor.py \
  --storage-account mystorageaccount \
  --container mycontainer \
  --msi my-managed-identity-name

# Provide Object ID directly (fastest resolution)
python adls_acl_extractor.py \
  --storage-account mystorageaccount \
  --container mycontainer \
  --msi xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# Include mask and special entries at matched paths
python adls_acl_extractor.py ... --include-mask --include-special

# Directories only, max 3 levels deep
python adls_acl_extractor.py ... --exclude-files --max-depth 3

# Scan a specific path only
python adls_acl_extractor.py ... --path /raw/folder1
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--storage-account` | — | Storage account name |
| `--container` | — | Container (filesystem) name |
| `--msi` | — | MSI name or Object ID GUID |
| `--path` | — | Scan specific path only |
| `--root-only` | off | Scan container root only |
| `--exclude-files` | off | Skip files, scan directories only |
| `--max-depth` | 0 (unlimited) | Maximum recursion depth |
| `--output` | auto-generated | Output CSV path |
| `--include-mask` | off | Include mask entries at matched paths |
| `--include-special` | off | Include owning user/group/other at matched paths |
| `--debug` | off | Verbose debug output |
| `--interactive` | off | Prompt for inputs interactively |

---

## ⚙️ Setup

### Prerequisites

- Python 3.9+
- Azure CLI (`az`) installed and authenticated — run `az login`

### Install dependencies

```bash
pip install -r requirements.txt
```

### Configure (for ADF scripts)

```bash
cp config.yaml.example config.yaml
# Edit config.yaml — fill in subscription_id, resource_group, factory_name
```

Or use environment variables:

```bash
cp .env.example .env
# Edit .env — then pass --env flag to scripts
```

---

## 🔐 Security

Credentials are never hardcoded. All sensitive values (subscription IDs, resource group names, factory names) are loaded from `config.yaml` or environment variables — both excluded from Git via `.gitignore`. Output files (`*.xlsx`, `*.csv`) are also excluded since they may contain real Azure resource names and pipeline data.

---

## 📦 Dependencies

| Package | Scripts | Purpose |
|---------|---------|---------|
| `azure-identity` | all | AzureCliCredential / DefaultAzureCredential |
| `azure-mgmt-datafactory` | adf_report_generator, adf_linked_service_inventory | ADF management API |
| `azure-storage-file-datalake` | adls_acl_extractor | ADLS Gen2 filesystem + ACL access |
| `pandas` | adf_report_generator, adf_linked_service_inventory | DataFrame assembly + Excel writing |
| `openpyxl` | adf_report_generator, adf_linked_service_inventory | Excel file engine |
| `requests` | adf_report_generator | Direct ARM REST API calls |
| `pyyaml` | adf scripts | Config file parsing |
| `python-dateutil` | adf_report_generator | Datetime string parsing |

---

## 🗂️ Repo structure

```
azure-python-automation/
├── adf_report_generator.py          # Pipeline report: parent-child, triggers, run history
├── adf_linked_service_inventory.py  # Linked service dependency mapper
├── adls_acl_extractor.py            # ADLS Gen2 Managed Identity ACL scanner
├── config.yaml.example              # Config template (copy to config.yaml)
├── .env.example                     # Env var template (copy to .env)
├── requirements.txt                 # Python dependencies
├── .gitignore                       # Excludes credentials and output files
└── README.md
```

---

## 📜 Related certifications

These scripts apply concepts covered in:
- **AZ-104** — Azure resource management, storage accounts, subscription/RG structure
- **AZ-500** — Managed Identities, RBAC, ADLS Gen2 ACLs, Key Vault, zero-trust architecture
- **DP-900** — Azure Data Factory, linked services, datasets, pipelines

---

## 👤 Author

**Aquib Aziz** — Azure Data Platform & Cloud Engineer  
[LinkedIn](https://linkedin.com/in/aquibaziz) · [GitHub](https://github.com/aquibaziz)
