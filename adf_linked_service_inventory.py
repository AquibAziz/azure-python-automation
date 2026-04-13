#!/usr/bin/env python3
"""
Azure Data Factory: Linked Service Inventory & Dependency Mapper
================================================================
Generates a two-sheet Excel report mapping every linked service in an
ADF instance to its associated datasets, pipelines, Key Vault dependencies,
and trigger schedules.

Sheet 1 — Detail view:
  Linked Service → Dataset → Container/Table → Folder Path → Pipeline

Sheet 2 — Relationship summary:
  Linked Service → Type → Related Datasets → Related Pipelines

Key capabilities:
  - Resolves dataset → linked service → pipeline chains
  - Detects Key Vault credential dependencies across linked services
  - Identifies scheduled, tumbling window, and blob event triggers
  - Handles AzureBlobStorage, ADLS, AzureSqlDatabase, Oracle, KeyVault types
  - Gracefully handles missing attributes across SDK versions

Usage:
  python adf_linked_service_inventory.py
  python adf_linked_service_inventory.py --config path/to/config.yaml
  python adf_linked_service_inventory.py --env

Config (config.yaml):
  subscription_id: <your-subscription-id>
  resource_group:  <your-resource-group>
  factory_name:    <your-adf-name>

Authentication:
  Uses AzureCliCredential — run 'az login' before executing.
  For Service Principal auth, set:
    AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID
  and switch credential to DefaultAzureCredential.
"""

import argparse
import os
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import yaml
from azure.identity import AzureCliCredential
from azure.mgmt.datafactory import DataFactoryManagementClient


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

def load_config_from_file(path: str) -> dict:
    """Load ADF connection config from a YAML file.

    Expected keys: subscription_id, resource_group, factory_name.
    Never hardcode these values — store them in config.yaml
    which is excluded from version control via .gitignore.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config_from_env() -> dict:
    """Load ADF connection config from environment variables.

    Required env vars:
      AZURE_SUBSCRIPTION_ID
      AZURE_RESOURCE_GROUP
      ADF_FACTORY_NAME
    """
    missing = []
    cfg = {
        "subscription_id": os.environ.get("AZURE_SUBSCRIPTION_ID"),
        "resource_group":  os.environ.get("AZURE_RESOURCE_GROUP"),
        "factory_name":    os.environ.get("ADF_FACTORY_NAME"),
    }
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise SystemExit(f"Missing environment variables: {missing}")
    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# Linked service loader
# ─────────────────────────────────────────────────────────────────────────────

def build_linked_service_map(
    adf_client: DataFactoryManagementClient,
    resource_group: str,
    factory_name: str,
) -> Tuple[Dict[str, dict], Dict[str, Set[str]]]:
    """Enumerate all linked services and initialise their metadata containers.

    Also detects Key Vault credential dependencies — i.e. which linked services
    use an AzureKeyVault linked service to retrieve their credentials.

    Returns:
        linked_service_info : dict keyed by LS name, containing type, url,
                              datasets, containers, folder paths, pipeline refs
        keyvault_usage      : dict mapping KV linked service name →
                              set of LS names that depend on it
    """
    linked_service_info: Dict[str, dict] = {}
    keyvault_usage: Dict[str, Set[str]] = {}

    for ls in adf_client.linked_services.list_by_factory(resource_group, factory_name):
        name    = ls.name
        ls_type = ls.properties.type

        # Extract connection URL / identifier depending on service type
        url = _extract_connection_url(ls, ls_type)

        linked_service_info[name] = {
            "type":                 ls_type,
            "url":                  url,
            "datasets":             [],
            "containers":           [],
            "folderPaths":          [],
            "dataset_pipeline_pairs": set(),
            "direct_pipelines":     set(),
        }

        # Map Key Vault credential dependencies for non-KV linked services
        if ls_type != "AzureKeyVault":
            _collect_keyvault_refs(ls, name, keyvault_usage)

    return linked_service_info, keyvault_usage


def _extract_connection_url(ls, ls_type: str) -> str:
    """Extract a human-readable connection identifier from a linked service.

    Returns the most useful available field for each service type.
    Returns a placeholder string for unrecognised types.
    """
    props = ls.properties
    if ls_type == "AzureBlobStorage":
        return getattr(props, "connection_string", "") or getattr(props, "account_name", "")
    if ls_type == "AzureDataLakeStore":
        return getattr(props, "url", "")
    if ls_type in ("AzureSqlDatabase", "Oracle"):
        return (
            getattr(props, "connection_string", "")
            or getattr(props, "connection_string_secret", "")
        )
    return f"Type: {ls_type} — no URL handler defined"


def _collect_keyvault_refs(ls, ls_name: str, keyvault_usage: Dict[str, Set[str]]) -> None:
    """Detect all Key Vault linked service references on a given linked service.

    ADF stores KV references in multiple possible attribute locations depending
    on the SDK version and linked service type. All three paths are checked.
    """
    props = ls.properties
    for attr in ("key_vault_properties", "linked_service_name", "key_vault_linked_service"):
        kv_holder = getattr(props, attr, None)
        if kv_holder is None:
            continue
        # key_vault_properties nests the LS reference one level deeper
        if attr == "key_vault_properties":
            kv_holder = getattr(kv_holder, "linked_service_name", None)
        if kv_holder and hasattr(kv_holder, "reference_name"):
            keyvault_usage.setdefault(kv_holder.reference_name, set()).add(ls_name)


# ─────────────────────────────────────────────────────────────────────────────
# Dataset mapper
# ─────────────────────────────────────────────────────────────────────────────

def attach_datasets(
    adf_client: DataFactoryManagementClient,
    resource_group: str,
    factory_name: str,
    linked_service_info: Dict[str, dict],
) -> Dict[str, str]:
    """Enumerate all datasets and attach them to their parent linked service.

    Also extracts container/file-system and folder-path metadata where available.
    Handles both file-based datasets (ADLS, Blob) and table-based datasets (SQL, Oracle).

    Returns:
        dataset_to_ls : dict mapping dataset name → linked service name
    """
    dataset_to_ls: Dict[str, str] = {}

    for ds in adf_client.datasets.list_by_factory(resource_group, factory_name):
        ref_name = getattr(ds.properties.linked_service_name, "reference_name", None)
        if ref_name not in linked_service_info:
            continue

        dataset_to_ls[ds.name] = ref_name

        # Extract location metadata (file-based datasets)
        container, folder_path = "", ""
        location = getattr(ds.properties, "location", None)
        if location:
            container   = getattr(location, "file_system", "") or getattr(location, "container", "")
            folder_path = getattr(location, "folder_path", "") or getattr(location, "folderPath", "")

        # Fallback: table/schema metadata (SQL/Oracle datasets)
        if not container:
            for attr in ("table_name", "table"):
                val = getattr(ds.properties, attr, None)
                if val:
                    container = str(val)
                    break
        if not folder_path:
            val = getattr(ds.properties, "schema", None)
            if val:
                folder_path = str(val)

        linked_service_info[ref_name]["datasets"].append(ds.name)
        linked_service_info[ref_name]["containers"].append(str(container).strip())
        linked_service_info[ref_name]["folderPaths"].append(str(folder_path).strip())

    return dataset_to_ls


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline activity scanner
# ─────────────────────────────────────────────────────────────────────────────

def _extract_direct_ls_refs(activity) -> Set[str]:
    """Extract linked service references directly on an activity (non-dataset path).

    Some activity types reference linked services directly via linkedServiceName
    rather than through a dataset. Both direct attribute and input/output paths
    are checked.
    """
    found: Set[str] = set()
    for attr in ("linked_service_name", "linkedServiceName"):
        lsn = getattr(activity, attr, None)
        if lsn:
            ref = getattr(lsn, "reference_name", None) or (lsn if isinstance(lsn, str) else None)
            if ref:
                found.add(ref)

    for field in ("inputs", "outputs"):
        for obj in getattr(activity, field, None) or []:
            for attr in ("linked_service_name", "linkedServiceName"):
                lsn = getattr(obj, attr, None)
                if lsn:
                    ref = getattr(lsn, "reference_name", None) or (lsn if isinstance(lsn, str) else None)
                    if ref:
                        found.add(ref)
    return found


def _collect_dataset_refs(activity) -> list:
    """Collect all dataset references from an activity.

    Handles generic inputs/outputs as well as activity-type-specific paths
    for Copy, Lookup, and GetMetadata activities.
    """
    refs = []
    refs.extend(getattr(activity, "inputs",  None) or [])
    refs.extend(getattr(activity, "outputs", None) or [])

    activity_type = getattr(activity, "type", "")
    if activity_type == "Copy":
        source = getattr(activity, "source", None)
        sink   = getattr(activity, "sink",   None)
        if source and hasattr(source, "dataset"):
            refs.append(source.dataset)
        if sink and hasattr(sink, "dataset"):
            refs.append(sink.dataset)
    elif activity_type in ("Lookup", "GetMetadata"):
        ds = getattr(activity, "dataset", None)
        if ds:
            refs.append(ds)

    return refs


def attach_pipeline_references(
    adf_client: DataFactoryManagementClient,
    resource_group: str,
    factory_name: str,
    linked_service_info: Dict[str, dict],
    dataset_to_ls: Dict[str, str],
) -> None:
    """Scan all pipeline activities and attach pipeline references to linked services.

    Two paths are traced:
      1. Direct LS reference on activity → recorded in direct_pipelines
      2. Dataset reference on activity → resolved to LS via dataset_to_ls
         → recorded in dataset_pipeline_pairs as (dataset_name, pipeline_name)
    """
    for pl in adf_client.pipelines.list_by_factory(resource_group, factory_name):
        pipeline_name = pl.name
        activities    = getattr(pl, "activities", []) or []

        for activity in activities:
            # Path 1: direct linked service references
            for ls_ref in _extract_direct_ls_refs(activity):
                if ls_ref in linked_service_info:
                    linked_service_info[ls_ref]["dataset_pipeline_pairs"].add(("", pipeline_name))
                    linked_service_info[ls_ref]["direct_pipelines"].add(pipeline_name)

            # Path 2: dataset-mediated references
            for ds_ref in _collect_dataset_refs(activity):
                ds_name = (
                    getattr(ds_ref, "reference_name", None)
                    or (ds_ref if isinstance(ds_ref, str) else None)
                )
                if not ds_name:
                    continue
                ls_name = dataset_to_ls.get(ds_name)
                if ls_name and ls_name in linked_service_info:
                    linked_service_info[ls_name]["dataset_pipeline_pairs"].add((ds_name, pipeline_name))


# ─────────────────────────────────────────────────────────────────────────────
# Trigger schedule enrichment
# ─────────────────────────────────────────────────────────────────────────────

def _safe_isoformat(dt) -> str:
    """Safely convert a datetime-like object to ISO 8601 string."""
    if dt is None:
        return ""
    return dt.isoformat() if hasattr(dt, "isoformat") else str(dt)


def build_trigger_schedule_map(
    adf_client: DataFactoryManagementClient,
    resource_group: str,
    factory_name: str,
) -> Dict[str, list]:
    """Build a map of pipeline name → trigger schedule details.

    Only processes triggers in 'started' (active) runtime state.
    Handles three trigger types:
      - ScheduleTrigger       : recurrence-based schedule
      - TumblingWindowTrigger : fixed-interval tumbling window
      - BlobEventsTrigger     : storage event-driven trigger

    Returns:
        dict mapping pipeline_name → list of schedule detail tuples
    """
    schedule_map: Dict[str, list] = {}

    for trigger in adf_client.triggers.list_by_factory(resource_group, factory_name):
        trigger_type   = trigger.properties.type
        runtime_state  = getattr(trigger.properties, "runtime_state", "").lower()

        if runtime_state != "started":
            continue
        if trigger_type not in ("ScheduleTrigger", "TumblingWindowTrigger", "BlobEventsTrigger"):
            continue

        if trigger_type == "ScheduleTrigger":
            recurrence  = getattr(trigger.properties, "recurrence", None)
            schedule    = getattr(recurrence, "schedule", None) if recurrence else None
            week_days   = getattr(schedule, "week_days",  []) or [] if schedule else []
            month_days  = getattr(schedule, "month_days", []) or [] if schedule else []
            hours       = getattr(schedule, "hours",      []) or [] if schedule else []
            minutes     = getattr(schedule, "minutes",    []) or [] if schedule else []

            details = (
                trigger.name,
                getattr(recurrence, "frequency",   "") if recurrence else "",
                getattr(recurrence, "interval",    "") if recurrence else "",
                _safe_isoformat(getattr(recurrence, "start_time", None)) if recurrence else "",
                _safe_isoformat(getattr(recurrence, "end_time",   None)) if recurrence else "",
                getattr(recurrence, "time_zone",   "") if recurrence else "",
                ", ".join(week_days),
                ", ".join(str(d) for d in month_days),
                ", ".join(str(h) for h in hours),
                ", ".join(str(m) for m in minutes),
            )
            for pipe_ref in getattr(trigger.properties, "pipelines", []):
                pipeline_name = pipe_ref.pipeline_reference.reference_name
                schedule_map.setdefault(pipeline_name, []).append(details)

        elif trigger_type == "TumblingWindowTrigger":
            pipe_ref      = trigger.properties.pipeline
            pipeline_name = pipe_ref.pipeline_reference.reference_name
            schedule_map.setdefault(pipeline_name, []).append((
                trigger.name, "TumblingWindow",
                getattr(trigger.properties, "interval", ""),
                "", "", "", "", "", "", "",
            ))

        elif trigger_type == "BlobEventsTrigger":
            for pipe_ref in getattr(trigger.properties, "pipelines", []):
                pipeline_name = pipe_ref.pipeline_reference.reference_name
                schedule_map.setdefault(pipeline_name, []).append((
                    trigger.name, "Event", "", "", "", "", "", "", "", "",
                ))

    return schedule_map


def enrich_pairs_with_schedule(
    linked_service_info: Dict[str, dict],
    schedule_map: Dict[str, list],
) -> None:
    """Enrich dataset_pipeline_pairs with trigger schedule metadata in-place.

    Replaces each (dataset, pipeline) pair with an extended tuple that includes
    schedule frequency, interval, time zone, and recurrence details.
    """
    for info in linked_service_info.values():
        enriched: Set[tuple] = set()
        for ds_name, pipeline in info.get("dataset_pipeline_pairs", set()):
            if pipeline in schedule_map:
                sched = schedule_map[pipeline][0]
                enriched.add((ds_name, pipeline, True, *sched[1:]))
            else:
                enriched.add((ds_name, pipeline, False, "", "", "", "", "", "", "", "", ""))
        info["dataset_pipeline_pairs"] = enriched


def propagate_keyvault_pipeline_refs(
    linked_service_info: Dict[str, dict],
    keyvault_usage: Dict[str, Set[str]],
) -> None:
    """Propagate pipeline references from dependent linked services to their Key Vault LS.

    When a linked service uses a Key Vault LS for credential retrieval, any
    pipeline that uses the dependent LS should also be associated with the KV LS
    for complete dependency tracing.
    """
    for kv_ls, dependent_ls_set in keyvault_usage.items():
        if kv_ls not in linked_service_info:
            continue
        kv_info = linked_service_info[kv_ls]
        for dep_ls in dependent_ls_set:
            if dep_ls not in linked_service_info:
                continue
            src = linked_service_info[dep_ls]
            kv_info["dataset_pipeline_pairs"] |= src.get("dataset_pipeline_pairs", set())
            kv_info["direct_pipelines"]       |= src.get("direct_pipelines", set())


# ─────────────────────────────────────────────────────────────────────────────
# Report assembly
# ─────────────────────────────────────────────────────────────────────────────

def build_sheet1(linked_service_info: Dict[str, dict]) -> pd.DataFrame:
    """Build Sheet 1: detailed view of LS → dataset → container → pipeline."""
    rows = []
    for ls_name, info in linked_service_info.items():
        pairs         = info.get("dataset_pipeline_pairs", set())
        already_added = set()

        for tup in pairs:
            ds_name  = tup[0] if len(tup) > 0 else ""
            pipeline = tup[1] if len(tup) > 1 else ""
            idx = next(
                (i for i, d in enumerate(info["datasets"]) if d == ds_name),
                0,
            )
            rows.append({
                "Linked Service Name":  ls_name,
                "Type":                 info["type"],
                "Dataset Name":         ds_name,
                "Container / Table":    info["containers"][idx]   if idx < len(info["containers"])   else "",
                "Folder Path / Schema": info["folderPaths"][idx]  if idx < len(info["folderPaths"])  else "",
                "Pipeline":             pipeline,
            })
            if ds_name:
                already_added.add(ds_name)

        # Include datasets not yet associated with any pipeline
        for idx, ds_name in enumerate(info["datasets"]):
            if ds_name not in already_added:
                rows.append({
                    "Linked Service Name":  ls_name,
                    "Type":                 info["type"],
                    "Dataset Name":         ds_name,
                    "Container / Table":    info["containers"][idx]  if idx < len(info["containers"])  else "",
                    "Folder Path / Schema": info["folderPaths"][idx] if idx < len(info["folderPaths"]) else "",
                    "Pipeline":             "",
                })

        # Include linked services with no datasets or pipeline associations
        if not info["datasets"] and not pairs:
            rows.append({
                "Linked Service Name":  ls_name,
                "Type":                 info["type"],
                "Dataset Name":         "",
                "Container / Table":    "",
                "Folder Path / Schema": "",
                "Pipeline":             "",
            })

    return pd.DataFrame(rows, columns=[
        "Linked Service Name", "Type", "Dataset Name",
        "Container / Table", "Folder Path / Schema", "Pipeline",
    ])


def build_sheet2(linked_service_info: Dict[str, dict]) -> pd.DataFrame:
    """Build Sheet 2: relationship summary of LS → datasets + pipelines."""
    rows = []
    for ls_name, info in linked_service_info.items():
        datasets  = sorted(set(info.get("datasets", [])))
        pipelines = sorted(set(info.get("direct_pipelines", set())))

        for ds in datasets:
            rows.append({
                "Linked Service Name": ls_name,
                "Type":                info["type"],
                "Related Dataset":     ds,
                "Related Pipeline":    "",
            })
        for pl in pipelines:
            rows.append({
                "Linked Service Name": ls_name,
                "Type":                info["type"],
                "Related Dataset":     "",
                "Related Pipeline":    pl,
            })
        if not datasets and not pipelines:
            rows.append({
                "Linked Service Name": ls_name,
                "Type":                info["type"],
                "Related Dataset":     "",
                "Related Pipeline":    "",
            })

    return pd.DataFrame(rows, columns=[
        "Linked Service Name", "Type", "Related Dataset", "Related Pipeline",
    ])


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="ADF Linked Service Inventory — maps LS → datasets, pipelines, KV dependencies.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python adf_linked_service_inventory.py
  python adf_linked_service_inventory.py --config path/to/config.yaml
  python adf_linked_service_inventory.py --env
  python adf_linked_service_inventory.py -o my_report.xlsx
        """,
    )
    parser.add_argument("--config", "-c", default=os.path.expanduser("~/config.yaml"),
                        help="Path to config.yaml (default: ~/config.yaml)")
    parser.add_argument("--env",    action="store_true",
                        help="Load config from environment variables instead of file")
    parser.add_argument("-o", "--out", default=None,
                        help="Output Excel file path (default: auto-generated with timestamp)")
    args = parser.parse_args(argv)

    # Load config
    if args.env:
        cfg = load_config_from_env()
    else:
        cfg = load_config_from_file(args.config)

    subscription_id = str(cfg["subscription_id"]).strip()
    resource_group  = str(cfg["resource_group"]).strip()
    factory_name    = str(cfg["factory_name"]).strip()

    # Authenticate — uses Azure CLI credentials
    # Switch to DefaultAzureCredential() for Managed Identity / Service Principal support
    credential  = AzureCliCredential()
    adf_client  = DataFactoryManagementClient(credential, subscription_id)

    print(f"[1/6] Loading linked services from '{factory_name}'...")
    linked_service_info, keyvault_usage = build_linked_service_map(
        adf_client, resource_group, factory_name
    )
    print(f"      Found {len(linked_service_info)} linked services "
          f"({len(keyvault_usage)} Key Vault dependencies detected)")

    print("[2/6] Attaching datasets...")
    dataset_to_ls = attach_datasets(adf_client, resource_group, factory_name, linked_service_info)
    total_datasets = sum(len(v["datasets"]) for v in linked_service_info.values())
    print(f"      Found {total_datasets} datasets across {len(dataset_to_ls)} unique linked service refs")

    print("[3/6] Scanning pipeline activities for LS/dataset references...")
    attach_pipeline_references(
        adf_client, resource_group, factory_name, linked_service_info, dataset_to_ls
    )

    print("[4/6] Building trigger schedule map...")
    schedule_map = build_trigger_schedule_map(adf_client, resource_group, factory_name)
    print(f"      Found active trigger schedules for {len(schedule_map)} pipelines")

    print("[5/6] Enriching pairs with schedule metadata and propagating KV refs...")
    enrich_pairs_with_schedule(linked_service_info, schedule_map)
    propagate_keyvault_pipeline_refs(linked_service_info, keyvault_usage)

    print("[6/6] Building report and writing Excel...")
    df_sheet1 = build_sheet1(linked_service_info)
    df_sheet2 = build_sheet2(linked_service_info)

    out_path = args.out or f"ADF_LinkedService_Inventory_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_sheet1.to_excel(writer, sheet_name="Detail",            index=False)
        df_sheet2.to_excel(writer, sheet_name="LS_Relationships",  index=False)

    print(f"\nReport written: {out_path}")
    print(f"  Sheet 1 rows : {len(df_sheet1)}")
    print(f"  Sheet 2 rows : {len(df_sheet2)}")


if __name__ == "__main__":
    main()
