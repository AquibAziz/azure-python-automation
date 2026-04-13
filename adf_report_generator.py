#!/usr/bin/env python3
"""
Azure Data Factory: Pipeline Report Generator
=============================================
Generates a multi-sheet Excel report covering:
  - Sheet 1: Parent-Child pipeline relationships
  - Sheet 2: Pipeline status summary (Parent / Child / Standalone)
  - Sheet 3: Pipeline triggers, schedules, and last 45-day run history

Features:
  - Recursive activity walker (ForEach, IfCondition, Until, Switch, ExecutePipeline)
  - Parallel run-history fetching with rate limiting (5 req/sec)
  - Automatic token refresh with 5-minute buffer
  - Exponential backoff retry on 429 / 503 / 401 / connection errors
  - Multi-timezone schedule display (source TZ → AEST/AEDT conversion)
  - Optimised batch pipeline definition loading

Usage:
  python adf_report_generator.py -c config.yaml -o report.xlsx --parallel

Config (config.yaml):
  subscription_id: <your-subscription-id>
  resource_group:  <your-resource-group>
  factory_name:    <your-adf-name>

Authentication:
  Uses DefaultAzureCredential. Ensure one of the following is available:
    - Azure CLI login  (az login)
    - Managed Identity (when running in Azure)
    - Service Principal env vars:
        AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID
"""

import argparse
import concurrent.futures
import importlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import sys
import time
from typing import Dict, List, Optional, Set, Tuple
import json
import os

import pandas as pd
import requests
import yaml
from dateutil import parser as dateparser
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


# ─────────────────────────────────────────────────────────────────────────────
# Timezone constants
# ─────────────────────────────────────────────────────────────────────────────

# Default output timezone — change via TARGET_TZ_ENV env var or update here
TARGET_TZ = ZoneInfo(os.environ.get("TARGET_TIMEZONE", "Australia/Sydney"))

WINDOWS_TO_IANA = {
    "AUS Eastern Standard Time":      "Australia/Sydney",
    "India Standard Time":            "Asia/Kolkata",
    "UTC":                            "UTC",
    "Singapore Standard Time":        "Asia/Singapore",
    "Eastern Standard Time":          "America/New_York",
    "Central Standard Time":          "America/Chicago",
    "Pacific Standard Time":          "America/Los_Angeles",
    "GMT Standard Time":              "Europe/London",
    "Central European Standard Time": "Europe/Berlin",
    "Tokyo Standard Time":            "Asia/Tokyo",
    "China Standard Time":            "Asia/Shanghai",
    "Arabian Standard Time":          "Asia/Dubai",
    "New Zealand Standard Time":      "Pacific/Auckland",
    "W. Australia Standard Time":     "Australia/Perth",
    "Cen. Australia Standard Time":   "Australia/Adelaide",
}

TZ_DISPLAY_NAMES = {
    "AUS Eastern Standard Time":      "AEST/AEDT",
    "India Standard Time":            "IST",
    "UTC":                            "UTC",
    "Singapore Standard Time":        "SGT",
    "Eastern Standard Time":          "EST/EDT",
    "Central Standard Time":          "CST/CDT",
    "Pacific Standard Time":          "PST/PDT",
    "GMT Standard Time":              "GMT/BST",
    "Central European Standard Time": "CET/CEST",
    "Tokyo Standard Time":            "JST",
    "China Standard Time":            "CST",
    "Arabian Standard Time":          "GST",
    "New Zealand Standard Time":      "NZST/NZDT",
    "W. Australia Standard Time":     "AWST",
    "Cen. Australia Standard Time":   "ACST/ACDT",
}


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Config:
    """ADF connection configuration loaded from a YAML file.

    Never hardcode subscription_id, resource_group, or factory_name.
    Store them in config.yaml (excluded from version control via .gitignore).
    """
    subscription_id: str
    resource_group: str
    factory_name: str

    @staticmethod
    def from_yaml(path: str) -> "Config":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        try:
            return Config(
                subscription_id=str(data["subscription_id"]).strip(),
                resource_group=str(data["resource_group"]).strip(),
                factory_name=str(data["factory_name"]).strip(),
            )
        except KeyError as e:
            raise SystemExit(f"Missing required config key in {path}: {e}")

    @staticmethod
    def from_env() -> "Config":
        """Alternative: load from environment variables instead of a file.

        Set these before running:
            export AZURE_SUBSCRIPTION_ID=<your-subscription-id>
            export AZURE_RESOURCE_GROUP=<your-resource-group>
            export ADF_FACTORY_NAME=<your-adf-name>
        """
        missing = []
        sub  = os.environ.get("AZURE_SUBSCRIPTION_ID")
        rg   = os.environ.get("AZURE_RESOURCE_GROUP")
        name = os.environ.get("ADF_FACTORY_NAME")
        if not sub:  missing.append("AZURE_SUBSCRIPTION_ID")
        if not rg:   missing.append("AZURE_RESOURCE_GROUP")
        if not name: missing.append("ADF_FACTORY_NAME")
        if missing:
            raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
        return Config(subscription_id=sub, resource_group=rg, factory_name=name)


# ─────────────────────────────────────────────────────────────────────────────
# Token Manager
# ─────────────────────────────────────────────────────────────────────────────

class TokenManager:
    """Manages Azure ARM bearer tokens with automatic refresh.

    Refreshes proactively when fewer than 5 minutes remain on the current token,
    preventing mid-request expiry failures on long-running reports.
    """

    _REFRESH_BUFFER_SECONDS = 300  # refresh 5 minutes before expiry

    def __init__(self, credential: DefaultAzureCredential):
        self.credential = credential
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

    def get_token(self) -> str:
        now = datetime.now(timezone.utc)
        needs_refresh = (
            self._token is None
            or self._token_expiry is None
            or (self._token_expiry - now).total_seconds() < self._REFRESH_BUFFER_SECONDS
        )
        if needs_refresh:
            token_obj = self.credential.get_token("https://management.azure.com/.default")
            self._token = token_obj.token
            self._token_expiry = datetime.fromtimestamp(token_obj.expires_on, tz=timezone.utc)
            print(
                f"[Token] Refreshed. Expires at "
                f"{self._token_expiry.strftime('%Y-%m-%d %H:%M:%S UTC')}",
                file=sys.stderr,
            )
        return self._token


# ─────────────────────────────────────────────────────────────────────────────
# Rate Limiter
# ─────────────────────────────────────────────────────────────────────────────

class RateLimiter:
    """Token-bucket rate limiter to stay within Azure ARM API throttle limits.

    Azure Resource Manager enforces per-subscription request quotas.
    Default: 5 requests/second (conservative — adjust via calls_per_second).
    """

    def __init__(self, calls_per_second: float = 5.0):
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0.0

    def wait(self):
        now = time.time()
        elapsed = now - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call = time.time()


# ─────────────────────────────────────────────────────────────────────────────
# Retry decorator
# ─────────────────────────────────────────────────────────────────────────────

def retry_with_backoff(max_retries: int = 5, base_delay: float = 2, max_delay: float = 60):
    """Decorator: exponential backoff retry for transient Azure API errors.

    Handles:
      429 (throttled)   — respects Retry-After header when present
      503 (unavailable) — infrastructure-level transient failure
      401 (auth)        — clears cached token to force refresh on next attempt
      ConnectionError / Timeout — network-level transient failures
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as exc:
                    status = exc.response.status_code if exc.response else 0
                    if status in (429, 503, 401):
                        retries += 1
                        if retries >= max_retries:
                            print(f"[Retry] Max retries reached for {func.__name__}: {exc}", file=sys.stderr)
                            raise
                        delay = min(base_delay * (2 ** (retries - 1)), max_delay)
                        if status == 429 and exc.response and "Retry-After" in exc.response.headers:
                            try:
                                delay = int(exc.response.headers["Retry-After"])
                            except ValueError:
                                pass
                        print(
                            f"[Retry] HTTP {status} in {func.__name__}, "
                            f"attempt {retries}/{max_retries}, waiting {delay}s...",
                            file=sys.stderr,
                        )
                        time.sleep(delay)
                        if status == 401 and "token_manager" in kwargs:
                            kwargs["token_manager"]._token = None
                        continue
                    raise
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                    retries += 1
                    if retries >= max_retries:
                        print(f"[Retry] Max retries reached for {func.__name__}: {exc}", file=sys.stderr)
                        raise
                    delay = min(base_delay * (2 ** (retries - 1)), max_delay)
                    print(
                        f"[Retry] Connection error in {func.__name__}, "
                        f"attempt {retries}/{max_retries}, waiting {delay}s...",
                        file=sys.stderr,
                    )
                    time.sleep(delay)
            raise RuntimeError(f"Max retries exceeded for {func.__name__}")
        return wrapper
    return decorator


# ─────────────────────────────────────────────────────────────────────────────
# Azure client factory
# ─────────────────────────────────────────────────────────────────────────────

def get_adf_client(cfg: Config) -> Tuple[DefaultAzureCredential, DataFactoryManagementClient, TokenManager]:
    """Create authenticated ADF management client using DefaultAzureCredential.

    Credential resolution order (automatic):
      1. Environment variables (AZURE_CLIENT_ID / SECRET / TENANT_ID)
      2. Azure CLI (az login)
      3. Managed Identity (when deployed to Azure)
      4. Visual Studio / VS Code login
    """
    credential = DefaultAzureCredential()
    client = DataFactoryManagementClient(credential, cfg.subscription_id)
    token_manager = TokenManager(credential)
    return credential, client, token_manager


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline definition loader
# ─────────────────────────────────────────────────────────────────────────────

def fetch_all_pipeline_definitions_batch(
    adf_client: DataFactoryManagementClient,
    cfg: Config,
) -> Dict[str, any]:
    """Fetch complete pipeline definitions including all nested activities.

    Why not use list_by_factory() alone?
      list_by_factory() returns shallow pipeline objects — nested activities
      inside ForEach / IfCondition / Until / Switch are NOT populated.
      We must call .get() per pipeline to retrieve the full activity graph.

    This is the most API-call-intensive step. Progress is printed every 10 pipelines.
    """
    print("[OPTIMIZATION] Fetching all pipeline names...", file=sys.stderr)
    pipeline_names = [
        p.name
        for p in adf_client.pipelines.list_by_factory(cfg.resource_group, cfg.factory_name)
    ]
    print(f"       Found {len(pipeline_names)} pipelines. Fetching full definitions...", file=sys.stderr)

    pipeline_defs: Dict[str, any] = {}
    for idx, name in enumerate(pipeline_names, 1):
        try:
            pipeline_defs[name] = adf_client.pipelines.get(
                cfg.resource_group, cfg.factory_name, name
            )
        except Exception as exc:
            print(f"       Warning: Could not fetch '{name}': {exc}", file=sys.stderr)
        if idx % 10 == 0 or idx == len(pipeline_names):
            print(f"       Progress: {idx}/{len(pipeline_names)}", file=sys.stderr)

    print(f"       Loaded {len(pipeline_defs)} complete pipeline definitions.", file=sys.stderr)
    return pipeline_defs


# ─────────────────────────────────────────────────────────────────────────────
# Timezone utilities
# ─────────────────────────────────────────────────────────────────────────────

def resolve_iana_timezone(windows_tz: str) -> ZoneInfo:
    """Resolve a Windows or IANA timezone name to a ZoneInfo object.

    Three-tier fallback strategy:
      1. Try as direct IANA name (some ADF triggers store IANA directly)
      2. Try via tzdata package Windows→IANA mapping (optional dependency)
      3. Fall back to hardcoded WINDOWS_TO_IANA map
      4. Default to UTC and emit a warning
    """
    if not windows_tz:
        return ZoneInfo("UTC")

    # Tier 1: direct IANA
    try:
        return ZoneInfo(windows_tz)
    except (ZoneInfoNotFoundError, KeyError):
        pass

    # Tier 2: tzdata package
    try:
        tzdata_windows = importlib.import_module("tzdata.windows")
        windows_to_olson = getattr(tzdata_windows, "windows_to_olson", {})
        iana = windows_to_olson.get(windows_tz)
        if iana:
            return ZoneInfo(iana)
    except (ImportError, AttributeError):
        pass

    # Tier 3: manual map
    iana = WINDOWS_TO_IANA.get(windows_tz)
    if iana:
        return ZoneInfo(iana)

    print(f"[WARN] Unknown timezone '{windows_tz}', defaulting to UTC", file=sys.stderr)
    return ZoneInfo("UTC")


def convert_time_to_target_tz(hour: int, minute: int, source_tz: ZoneInfo) -> str:
    """Convert HH:MM from source_tz to the configured TARGET_TZ.

    Uses today's date so DST is always resolved correctly for the current
    period (avoids static UTC offset errors around DST transitions).
    Returns a string like '02:01 AEDT'.
    """
    try:
        today = datetime.now(tz=source_tz).date()
        dt = datetime(today.year, today.month, today.day, hour, minute, tzinfo=source_tz)
        dt_target = dt.astimezone(TARGET_TZ)
        offset_hours = dt_target.utcoffset().total_seconds() / 3600
        abbrev = "AEDT" if offset_hours == 11 else "AEST"
        return f"{dt_target.strftime('%H:%M')} {abbrev}"
    except Exception as exc:
        print(f"[WARN] Time conversion failed ({hour}:{minute}): {exc}", file=sys.stderr)
        return ""


def convert_timestring_to_target_tz(time_str: str, source_tz: ZoneInfo) -> str:
    """Convert a 'HH:MM' string from source_tz to TARGET_TZ string."""
    if not time_str:
        return ""
    try:
        parts = time_str.strip().split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        return convert_time_to_target_tz(h, m, source_tz)
    except Exception as exc:
        print(f"[WARN] Could not parse time '{time_str}': {exc}", file=sys.stderr)
        return ""


def format_run_dt_local(dt_raw: str) -> str:
    """Format an ADF run datetime string to the configured TARGET_TZ."""
    if not dt_raw:
        return "N/A"
    dt = dateparser.parse(str(dt_raw))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TARGET_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def normalize_trigger_runtime_state(status: str) -> str:
    """Normalise ADF trigger runtime state to a display-friendly label."""
    s = str(status or "").strip().lower()
    if s == "started":
        return "Started"
    if s == "stopped":
        return "Stopped"
    return status or "Unknown"


def build_combined_last_run_status(pipeline_status: str, trigger_status: str) -> str:
    """Combine pipeline run status and trigger runtime state into one readable label."""
    p = (pipeline_status or "N/A").strip() or "N/A"
    t = (trigger_status  or "N/A").strip() or "N/A"
    return f"Pipeline: {p} | Trigger: {t}"


# ─────────────────────────────────────────────────────────────────────────────
# Trigger schedule helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_schedule_string(trigger) -> str:
    """Build a human-readable schedule summary string from a trigger object."""
    props = getattr(trigger, "properties", None)
    if not props:
        return ""
    recurrence = getattr(props, "recurrence", None)
    if not recurrence:
        return ""

    freq      = getattr(recurrence, "frequency", "") or ""
    interval  = getattr(recurrence, "interval",  "") or ""
    schedule  = getattr(recurrence, "schedule",  None)

    week_days  = ",".join(getattr(schedule, "week_days",  []) or []) if schedule else ""
    month_days = ",".join(str(x) for x in (getattr(schedule, "month_days", []) or [])) if schedule else ""
    hours      = ",".join(str(x) for x in (getattr(schedule, "hours",   []) or [])) if schedule else ""
    minutes    = ",".join(str(x) for x in (getattr(schedule, "minutes", []) or [])) if schedule else ""

    parts = []
    if freq and interval:
        parts.append(f"{freq} - {interval}")
    elif freq:
        parts.append(str(freq))
    if month_days: parts.append(f"Month Day - {month_days}")
    if week_days:  parts.append(f"Week Day - {week_days}")
    if hours:      parts.append(f"Hour {hours}")
    if minutes:    parts.append(f"Minute {minutes}")
    return ", ".join(parts)


def extract_schedule_details(trigger) -> Tuple[str, str, str, str, str, str]:
    """Extract structured schedule details from an ADF trigger.

    Returns:
        adv_opt          : "MonthDays" | "WeekDays" | ""
        recur_detail     : Comma-separated day/date values
        sched_time       : Configured time(s), e.g. "16:01"            (Column O)
        tz_display       : Short TZ label,     e.g. "IST"              (Column P)
        sched_time_target: Converted time(s),  e.g. "02:01 AEDT"       (Column Q)
        source           : Extraction path: "ScheduleTimes" | "Hours/Minutes" | "StartTime"
    """
    props      = getattr(trigger, "properties", None)
    recurrence = getattr(props, "recurrence", None)
    if not recurrence:
        return "", "", "", "UTC", "", ""

    windows_tz = (
        getattr(recurrence, "time_zone",  None)
        or getattr(recurrence, "timeZone", None)
        or "UTC"
    ).strip()

    tz_display = TZ_DISPLAY_NAMES.get(windows_tz, windows_tz or "UTC")
    source_tz  = resolve_iana_timezone(windows_tz)

    schedule = getattr(recurrence, "schedule", None)
    adv_opt, recur_detail, sched_time, sched_time_target, source = "", "", "", "", ""

    if schedule:
        week_days  = getattr(schedule, "week_days",  []) or getattr(schedule, "WeekDays",  []) or []
        month_days = getattr(schedule, "month_days", []) or getattr(schedule, "MonthDays", []) or []
        if month_days:
            adv_opt      = "MonthDays"
            recur_detail = ", ".join(str(x) for x in month_days)
        elif week_days:
            adv_opt      = "WeekDays"
            recur_detail = ", ".join(week_days)

        schedule_times = (
            getattr(schedule, "schedule_times", [])
            or getattr(schedule, "ScheduleTimes", [])
        )
        hours   = getattr(schedule, "hours",   []) or getattr(schedule, "Hours",   [])
        minutes = getattr(schedule, "minutes", []) or getattr(schedule, "Minutes", [])

        if schedule_times:
            raw_times       = sorted(str(t) for t in schedule_times)
            sched_time      = ", ".join(raw_times)
            sched_time_target = ", ".join(
                filter(None, (convert_timestring_to_target_tz(t, source_tz) for t in raw_times))
            )
            source = "ScheduleTimes"

        elif hours or minutes:
            if not minutes:
                minutes = [0]
            raw_times = sorted(
                f"{int(h):02d}:{int(m):02d}" for h in hours for m in minutes
            )
            sched_time        = ", ".join(raw_times)
            sched_time_target = ", ".join(filter(None, (
                convert_time_to_target_tz(int(t.split(":")[0]), int(t.split(":")[1]), source_tz)
                for t in raw_times
            )))
            source = "Hours/Minutes"

        else:
            sched_time, sched_time_target, source = _extract_from_start_time(recurrence, source_tz)
    else:
        sched_time, sched_time_target, source = _extract_from_start_time(recurrence, source_tz)

    return adv_opt, recur_detail, sched_time, tz_display, sched_time_target, source


def _extract_from_start_time(recurrence, source_tz: ZoneInfo) -> Tuple[str, str, str]:
    """Helper: extract schedule time from recurrence startTime field."""
    start_time = (
        getattr(recurrence, "start_time", None)
        or getattr(recurrence, "startTime", None)
    )
    if not start_time:
        return "", "", ""
    try:
        dt = dateparser.parse(str(start_time))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=source_tz)
        sched_time = dt.strftime("%H:%M")
        dt_target  = dt.astimezone(TARGET_TZ)
        offset_h   = dt_target.utcoffset().total_seconds() / 3600
        abbrev     = "AEDT" if offset_h == 11 else "AEST"
        return sched_time, f"{dt_target.strftime('%H:%M')} {abbrev}", "StartTime"
    except Exception as exc:
        print(f"[WARN] StartTime parse failed: {exc}", file=sys.stderr)
        return "", "", ""


# ─────────────────────────────────────────────────────────────────────────────
# Run history fetcher
# ─────────────────────────────────────────────────────────────────────────────

@retry_with_backoff(max_retries=5, base_delay=2, max_delay=60)
def query_pipeline_runs_optimized(
    cfg: Config,
    pipeline_name: str,
    token_manager: TokenManager,
    days: int = 45,
) -> Tuple[Optional[dict], bool, List[dict]]:
    """Fetch the first page of pipeline runs ordered by latest start time.

    Optimisation: fetches only one page (latest run) rather than paginating
    through all history. Sufficient for the "last run" report use case.

    Returns:
        latest_run : Most recent run dict, or None if no runs in window
        has_recent : True if any run exists in the lookback window
        runs_page  : All runs on the first page
    """
    url = (
        "https://management.azure.com"
        f"/subscriptions/{cfg.subscription_id}"
        f"/resourceGroups/{cfg.resource_group}"
        f"/providers/Microsoft.DataFactory/factories/{cfg.factory_name}"
        f"/queryPipelineRuns?api-version=2018-06-01"
    )
    now   = datetime.utcnow()
    start = now - timedelta(days=days)

    headers = {
        "Authorization": f"Bearer {token_manager.get_token()}",
        "Content-Type": "application/json",
    }
    payload = {
        "lastUpdatedAfter":  start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lastUpdatedBefore": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "filters": [{"operand": "PipelineName", "operator": "Equals", "values": [pipeline_name]}],
        "orderBy": [{"orderBy": "RunStart", "order": "DESC"}],
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=90)
    resp.raise_for_status()

    runs = resp.json().get("value", [])
    if not runs:
        return None, False, []
    return runs[0], True, runs


def extract_trigger_name_from_run(run: dict) -> str:
    """Best-effort extraction of trigger name from an ADF run payload."""
    invoked = run.get("invokedBy") or {}
    for key in ("name", "triggerName", "pipelineTriggerName"):
        val = invoked.get(key)
        if val:
            return str(val)
    ref = str(invoked.get("id") or invoked.get("referenceName") or "")
    if "/triggers/" in ref:
        return ref.split("/triggers/", 1)[1].split("/", 1)[0]
    return ""


def select_latest_run_for_trigger(
    runs: List[dict],
    trigger_name: str,
    require_completed: bool = False,
) -> Optional[dict]:
    """Return the most recent run whose invoking trigger matches trigger_name."""
    target = (trigger_name or "").strip().lower()
    if not target:
        return None
    for run in runs or []:
        if extract_trigger_name_from_run(run).strip().lower() != target:
            continue
        if require_completed:
            status = str(run.get("status", "")).strip().lower()
            if status in {"inprogress", "queued", "canceling"} or not run.get("runEnd"):
                continue
        return run
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Activity walker (recursive)
# ─────────────────────────────────────────────────────────────────────────────

def _get_nested_activities(activity, *attr_names) -> Optional[list]:
    """Try to get nested activities from direct attribute, then type_properties."""
    for attr in attr_names:
        val = getattr(activity, attr, None)
        if val:
            return val
    type_props = getattr(activity, "type_properties", None)
    if type_props is not None:
        for attr in attr_names:
            val = getattr(type_props, attr, None)
            if val:
                return val
    return None


def walk_activities(
    activities: list,
    parent_name: str,
    parent_folder: str,
    records: list,
    parent_set: Set[str],
    child_set: Set[str],
    child_to_parent: Dict[str, Set[str]],
) -> None:
    """Recursively walk all activities in a pipeline, extracting parent-child relationships.

    Handles control flow activity types:
      - ExecutePipeline : records a parent→child edge
      - ForEach         : recurses into inner activities
      - IfCondition     : recurses into ifTrue and ifFalse branches
      - Until           : recurses into inner activities
      - Switch          : recurses into each case + default branch

    Both direct-attribute and type_properties SDK layouts are supported.
    """
    for activity in activities or []:
        activity_type = getattr(activity, "type", "")

        if activity_type == "ExecutePipeline":
            child_name    = getattr(getattr(activity, "pipeline", None), "reference_name", "") or ""
            activity_name = getattr(activity, "name", "") or ""
            raw_params    = getattr(activity, "parameters", None)
            if raw_params is None:
                type_props = getattr(activity, "type_properties", None)
                if type_props is not None:
                    raw_params = getattr(type_props, "parameters", None)
            try:
                parameters = json.dumps(raw_params, default=str) if raw_params else ""
            except Exception:
                parameters = str(raw_params or "")

            if child_name:
                parent_set.add(parent_name)
                child_set.add(child_name)
                child_to_parent.setdefault(child_name, set()).add(parent_name)
                records.append({
                    "Parent Pipeline":   parent_name,
                    "Parent Folder":     parent_folder,
                    "Child Pipeline":    child_name,
                    "Activity Name":     activity_name,
                    "Parameters Passed": parameters,
                })

        elif activity_type == "ForEach":
            nested = _get_nested_activities(activity, "activities")
            if nested:
                walk_activities(nested, parent_name, parent_folder,
                                records, parent_set, child_set, child_to_parent)

        elif activity_type == "IfCondition":
            for branch_attr in ("if_true_activities", "if_false_activities"):
                branch = _get_nested_activities(activity, branch_attr)
                if branch:
                    walk_activities(branch, parent_name, parent_folder,
                                    records, parent_set, child_set, child_to_parent)

        elif activity_type == "Until":
            nested = _get_nested_activities(activity, "activities")
            if nested:
                walk_activities(nested, parent_name, parent_folder,
                                records, parent_set, child_set, child_to_parent)

        elif activity_type == "Switch":
            cases = _get_nested_activities(activity, "cases")
            if cases:
                for case in cases:
                    case_acts = getattr(case, "activities", None)
                    if case_acts:
                        walk_activities(case_acts, parent_name, parent_folder,
                                        records, parent_set, child_set, child_to_parent)
            default_acts = _get_nested_activities(activity, "default_activities")
            if default_acts:
                walk_activities(default_acts, parent_name, parent_folder,
                                records, parent_set, child_set, child_to_parent)


# ─────────────────────────────────────────────────────────────────────────────
# Parallel run fetcher
# ─────────────────────────────────────────────────────────────────────────────

def fetch_runs_parallel_with_limit(
    names: List[str],
    cfg: Config,
    token_manager: TokenManager,
    days: int,
    max_workers: int,
    verbose: bool,
) -> Dict[str, Tuple]:
    """Fetch pipeline run history in parallel with rate limiting.

    Uses a shared RateLimiter across all threads to stay within ARM throttle.
    Progress is printed every 25 completions.
    """
    rate_limiter = RateLimiter(calls_per_second=5.0)

    def fetch_one(name: str) -> Tuple[str, Tuple]:
        rate_limiter.wait()
        try:
            result = query_pipeline_runs_optimized(cfg, name, token_manager, days)
            if verbose:
                print(f"       {'✓' if result[0] else '○'} {name}", file=sys.stderr)
            return name, result
        except Exception as exc:
            print(f"       ✗ {name}: {exc}", file=sys.stderr)
            return name, (None, False, [])

    print(f"       Using {max_workers} workers (rate limit: 5 req/sec)", file=sys.stderr)
    results: Dict[str, Tuple] = {}
    completed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(fetch_one, name): name for name in names}
        for future in concurrent.futures.as_completed(future_map):
            pname, result = future.result()
            results[pname] = result
            completed += 1
            if completed % 25 == 0 or completed == len(names):
                print(f"       Progress: {completed}/{len(names)} pipelines", file=sys.stderr)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Report builder
# ─────────────────────────────────────────────────────────────────────────────

def build_reports(
    cfg: Config,
    days: int,
    parallel: bool,
    max_workers: int,
    verbose: bool,
    out_path: str,
) -> None:
    """Orchestrate all report stages and write the final Excel file.

    Stages:
      1. Fetch all pipeline definitions (full, with nested activities)
      2. Walk activities → build parent-child relationship graph
      3. Fetch all triggers and extract schedule metadata
      4. Fetch run history for all pipelines
      5. Assemble final DataFrame and write Excel
    """
    _, adf_client, token_manager = get_adf_client(cfg)

    # ── Stage 1: Pipeline definitions ─────────────────────────────────────
    print("[1/5] Fetching pipeline definitions...", file=sys.stderr)
    pipeline_defs = fetch_all_pipeline_definitions_batch(adf_client, cfg)
    all_pipelines = list(pipeline_defs.keys())

    # ── Stage 2: Parent-child relationships ───────────────────────────────
    print("[2/5] Building parent-child relationships...", file=sys.stderr)
    parent_set: Set[str]              = set()
    child_set: Set[str]               = set()
    child_to_parent: Dict[str, Set]   = {}
    sheet1_records: List[dict]        = []

    for name, pdef in pipeline_defs.items():
        folder_obj    = getattr(pdef, "folder", None)
        parent_folder = getattr(folder_obj, "name", "") if folder_obj else ""
        activities    = getattr(pdef, "activities", []) or []
        walk_activities(
            activities, name, parent_folder,
            sheet1_records, parent_set, child_set, child_to_parent,
        )

    df_sheet1 = pd.DataFrame(sheet1_records).drop_duplicates() if sheet1_records else pd.DataFrame()

    all_pipelines_set = set(all_pipelines) | parent_set | child_set
    df_sheet2 = pd.DataFrame([
        {
            "Pipeline Name":       pname,
            "Parent or Child":     "Parent" if pname in parent_set else ("Child" if pname in child_set else "N/A"),
            "Parent Pipeline Name": ", ".join(sorted(child_to_parent.get(pname, []))) if pname in child_set else "N/A",
        }
        for pname in sorted(all_pipelines_set)
    ])

    # ── Stage 3: Triggers ─────────────────────────────────────────────────
    print("[3/5] Fetching triggers...", file=sys.stderr)
    triggers = list(adf_client.triggers.list_by_factory(cfg.resource_group, cfg.factory_name))
    print(f"       Found {len(triggers)} triggers.", file=sys.stderr)

    pipeline_trigger_schedule: List[dict] = []
    for trigger in triggers:
        schedule_str = build_schedule_string(trigger)
        adv_opt, recur_detail, sched_time, tz_display, sched_time_target, source = \
            extract_schedule_details(trigger)

        props          = getattr(trigger, "properties", None)
        pipelines_list = getattr(props, "pipelines", None) or []
        linked_pipelines: List[str] = []

        for pipe in pipelines_list:
            try:
                ref  = getattr(pipe, "pipeline_reference", None)
                name = getattr(ref, "reference_name", None) if ref else getattr(pipe, "reference_name", None)
                if name:
                    linked_pipelines.append(name)
            except Exception as exc:
                print(f"[WARN] Could not read pipeline ref from trigger: {exc}", file=sys.stderr)

        for pname in linked_pipelines:
            pipeline_trigger_schedule.append({
                "Pipeline Name":                pname,
                "Trigger Name":                 trigger.name,
                "Schedule":                     schedule_str,
                "Advanced Recurrence Option":   adv_opt,
                "Recurrence Detail":            recur_detail,
                "Schedule Execution Time":      sched_time,
                "Trigger Timezone":             tz_display,
                "Schedule Execution Time AEST": sched_time_target,
                "Schedule Source":              source,
            })

    # ── Stage 4: Run history ──────────────────────────────────────────────
    print(f"[4/5] Fetching run history ({'parallel' if parallel else 'sequential'})...", file=sys.stderr)
    names = sorted(all_pipelines_set)

    if parallel:
        pipeline_runs_map = fetch_runs_parallel_with_limit(
            names, cfg, token_manager, days, max_workers, verbose
        )
    else:
        pipeline_runs_map: Dict[str, Tuple] = {}
        for idx, pname in enumerate(names, 1):
            try:
                pipeline_runs_map[pname] = query_pipeline_runs_optimized(
                    cfg, pname, token_manager, days
                )
            except Exception as exc:
                print(f"       ✗ {pname}: {exc}", file=sys.stderr)
                pipeline_runs_map[pname] = (None, False, [])
            if idx % 25 == 0 or idx == len(names):
                print(f"       Progress: {idx}/{len(names)}", file=sys.stderr)

    # ── Stage 5: Assemble final report ────────────────────────────────────
    print("[5/5] Building final report...", file=sys.stderr)
    trigger_status_map = {
        t.name: getattr(getattr(t, "properties", None), "runtime_state", "") or "Unknown"
        for t in triggers
    }

    sheet3_rows: List[dict] = []
    for pname in names:
        status_text  = "Parent" if pname in parent_set else ("Child" if pname in child_set else "N/A")
        parent_names = ", ".join(sorted(child_to_parent.get(pname, []))) if pname in child_set else "N/A"

        latest_run, has_recent, runs_page = pipeline_runs_map.get(pname, (None, False, []))
        last_start, last_end, last_45, last_status, last_error = "N/A", "N/A", "N", "", ""

        if latest_run:
            last_start  = format_run_dt_local(latest_run.get("runStart"))
            last_end    = format_run_dt_local(latest_run.get("runEnd"))
            last_status = (latest_run.get("status", "") or "").strip()
            last_error  = (
                (latest_run.get("message") or latest_run.get("failureType") or "")
                if last_status.lower() == "failed" else ""
            )
            last_45 = "Y" if has_recent else "N"

        triggers_for_pipeline = [t for t in pipeline_trigger_schedule if t["Pipeline Name"] == pname]

        if triggers_for_pipeline:
            for t in triggers_for_pipeline:
                trig_state = normalize_trigger_runtime_state(
                    trigger_status_map.get(t["Trigger Name"], "Unknown")
                )
                row_start, row_end, row_45, row_status, row_error = \
                    last_start, last_end, last_45, last_status, last_error

                if trig_state == "Stopped":
                    row_start, row_end, row_45, row_status, row_error = "N/A", "N/A", "N", "N/A", ""
                else:
                    matched_run = select_latest_run_for_trigger(runs_page, t["Trigger Name"])
                    if matched_run:
                        row_start  = format_run_dt_local(matched_run.get("runStart"))
                        row_end    = format_run_dt_local(matched_run.get("runEnd"))
                        row_status = (matched_run.get("status", "") or "").strip()
                        row_error  = (
                            (matched_run.get("message") or matched_run.get("failureType") or "")
                            if row_status.lower() == "failed" else ""
                        )
                        row_45 = "Y"

                sheet3_rows.append({
                    "Pipeline Name":                pname,
                    "Parent or Child":              status_text,
                    "Parent Pipeline Name":         parent_names,
                    "Associated Trigger":           t["Trigger Name"],
                    "Trigger Status":               trig_state,
                    "Last Run 45 Days":             row_45,
                    "Last Run Start Date":          row_start,
                    "Last Run End Date":            row_end,
                    "Schedule":                     t["Schedule"],
                    "Last Run Status":              build_combined_last_run_status(row_status, trig_state),
                    "Last Run Error":               row_error,
                    "Advanced Recurrence Option":   t["Advanced Recurrence Option"],
                    "Recurrence Detail":            t["Recurrence Detail"],
                    "Schedule Execution Time":      t["Schedule Execution Time"],
                    "Trigger Timezone":             t["Trigger Timezone"],
                    "Schedule Execution Time AEST": t["Schedule Execution Time AEST"],
                    "Schedule Source":              t["Schedule Source"],
                })
        else:
            sheet3_rows.append({
                "Pipeline Name":                pname,
                "Parent or Child":              status_text,
                "Parent Pipeline Name":         parent_names,
                "Associated Trigger":           "N/A",
                "Trigger Status":               "",
                "Last Run 45 Days":             last_45,
                "Last Run Start Date":          last_start,
                "Last Run End Date":            last_end,
                "Schedule":                     "",
                "Last Run Status":              build_combined_last_run_status(last_status, "N/A"),
                "Last Run Error":               last_error,
                "Advanced Recurrence Option":   "",
                "Recurrence Detail":            "",
                "Schedule Execution Time":      "",
                "Trigger Timezone":             "",
                "Schedule Execution Time AEST": "",
                "Schedule Source":              "",
            })

    column_order = [
        "Pipeline Name", "Parent or Child", "Parent Pipeline Name",
        "Associated Trigger", "Trigger Status",
        "Last Run 45 Days", "Last Run Start Date", "Last Run End Date",
        "Schedule", "Last Run Status", "Last Run Error",
        "Advanced Recurrence Option", "Recurrence Detail",
        "Schedule Execution Time", "Trigger Timezone",
        "Schedule Execution Time AEST", "Schedule Source",
    ]
    df_sheet3 = pd.DataFrame(sheet3_rows)[column_order]

    print(f"Writing report: {out_path}", file=sys.stderr)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_sheet1.to_excel(writer, sheet_name="Parent-Child Relationships", index=False)
        df_sheet2.to_excel(writer, sheet_name="Pipeline Status",           index=False)
        df_sheet3.to_excel(writer, sheet_name="Pipeline Triggers",         index=False)

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"Report complete: {out_path}", file=sys.stderr)
    print(f"Pipelines processed: {len(names)}", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ADF Pipeline Report Generator — outputs Excel with parent-child, status, and trigger sheets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic run with config file
  python adf_report_generator.py -c config.yaml

  # Parallel mode with custom output path
  python adf_report_generator.py -c config.yaml --parallel -o my_report.xlsx

  # Load config from environment variables instead of file
  python adf_report_generator.py --env -o report.xlsx --parallel

  # Verbose output showing each pipeline
  python adf_report_generator.py -c config.yaml --parallel -v
        """,
    )
    p.add_argument("-c", "--config",      help="Path to config.yaml")
    p.add_argument("--env",               action="store_true",
                   help="Load config from environment variables instead of a file")
    p.add_argument("-o", "--out",         default="ADF_Pipeline_Report.xlsx",
                   help="Output Excel file path (default: ADF_Pipeline_Report.xlsx)")
    p.add_argument("-d", "--days",        type=int, default=45,
                   help="Lookback window for run history in days (default: 45)")
    p.add_argument("--parallel",          action="store_true",
                   help="Fetch run history in parallel (recommended for large factories)")
    p.add_argument("--max-workers",       type=int, default=6,
                   help="Number of parallel worker threads (default: 6)")
    p.add_argument("-v", "--verbose",     action="store_true",
                   help="Print status for each pipeline as it's processed")
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    if args.env:
        cfg = Config.from_env()
    elif args.config:
        cfg = Config.from_yaml(args.config)
    else:
        raise SystemExit(
            "Error: provide either --config <path> or --env to load configuration.\n"
            "Run with --help for usage examples."
        )

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"ADF Report Generator", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)
    print(f"Factory:        {cfg.factory_name}", file=sys.stderr)
    print(f"Resource Group: {cfg.resource_group}", file=sys.stderr)
    print(f"Lookback:       {args.days} days", file=sys.stderr)
    print(f"Mode:           {'parallel' if args.parallel else 'sequential'}", file=sys.stderr)
    if args.parallel:
        print(f"Workers:        {args.max_workers}", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    start_time = time.time()
    try:
        build_reports(cfg, args.days, args.parallel, args.max_workers, args.verbose, args.out)
        elapsed = time.time() - start_time
        print(f"Total time: {elapsed:.1f}s ({elapsed / 60:.1f} min)", file=sys.stderr)
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Cancelled by user.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        import traceback
        print(f"\n[FATAL] {exc}", file=sys.stderr)
        traceback.print_exc()
        raise SystemExit(1)


if __name__ == "__main__":
    main()
