#!/usr/bin/env python3
"""
ADLS Gen2: Managed Identity ACL Extractor
==========================================
Recursively scans an ADLS Gen2 container and extracts every path where a
specific Managed Identity (or Service Principal) has explicit ACL entries.

Output: CSV report with columns:
  storage_account, container, path, object_type,
  principal_id, principal_type, permissions, is_default

Use cases:
  - Audit where an MSI has been granted access across a storage container
  - Validate zero-trust identity configurations before or after migration
  - Support access reviews and governance reporting for ADLS Gen2
  - Identify over-privileged identities across deep folder hierarchies

Usage:
  # Interactive mode (prompts for inputs)
  python adls_acl_extractor.py --interactive

  # Non-interactive with arguments
  python adls_acl_extractor.py \\
    --storage-account <account-name> \\
    --container <container-name> \\
    --msi <msi-name-or-object-id>

  # Scan specific path only
  python adls_acl_extractor.py --storage-account <name> --container <c> \\
    --msi <msi> --path /raw/folder1

  # Include mask and special entries at matched paths
  python adls_acl_extractor.py --storage-account <name> --container <c> \\
    --msi <msi> --include-mask --include-special

  # Directories only, limited depth
  python adls_acl_extractor.py --storage-account <name> --container <c> \\
    --msi <msi> --exclude-files --max-depth 3

Authentication:
  Uses AzureCliCredential (az login) with fallback to DefaultAzureCredential.
  The identity running this script needs:
    - Storage Blob Data Reader (or higher) on the container
    - OR "Execute" permission on each directory to read ACLs

MSI resolution — accepts any of:
  - Object ID GUID directly     (fastest, most reliable)
  - UAMI name                   (resolved via az identity list)
  - UAMI name|resourceGroup     (resolved via az identity show)
  - Service principal name      (resolved via az ad sp list)
"""

import argparse
import csv
import json
import logging
import re
import subprocess
import sys
from typing import Dict, List, Optional, Tuple

from azure.core.exceptions import HttpResponseError
from azure.identity import AzureCliCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

GUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$"
)

# Suppress verbose Azure SDK logs unless debug mode is active
logging.getLogger("azure.storage").setLevel(logging.ERROR)
logging.getLogger("azure.core").setLevel(logging.ERROR)


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def is_guid(s: str) -> bool:
    """Return True if the string is a valid Azure AD Object ID (GUID/UUID)."""
    return bool(GUID_RE.match((s or "").strip()))


def run_az_cmd(cmd: List[str], timeout: int = 10) -> str:
    """Run an Azure CLI command and return stdout text.

    Returns an empty string on any failure — callers handle the fallback logic.
    Requires 'az' CLI to be installed and authenticated (az login).
    """
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, timeout=timeout)
        return out.decode("utf-8", errors="ignore").strip()
    except Exception:
        return ""


def get_credential():
    """Return an Azure credential, preferring CLI auth with SDK fallback."""
    try:
        return AzureCliCredential()
    except Exception:
        return DefaultAzureCredential()


# ─────────────────────────────────────────────────────────────────────────────
# MSI / Principal resolution
# ─────────────────────────────────────────────────────────────────────────────

def resolve_msi_to_principal_id(
    msi_input: str,
    debug: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    """Resolve an MSI identifier to (principal_id, display_name).

    Resolution strategy (four-tier fallback):
      1. Input is already a GUID  → use directly, look up display name
      2. Input is "name|rg"       → az identity show --name --resource-group
      3. Input is a UAMI name     → az identity list (search by name)
      4. Input is an SP name      → az ad sp list (search by displayName)
         If no exact match, performs a broad contains() search with confirmation.

    Returns (principal_id_guid, display_name) or (None, None) if unresolvable.

    Tip: Providing the Object ID GUID directly is fastest and most reliable.
    Find it in Azure Portal → Managed Identity → Overview → Object (principal) ID.
    """
    msi_input = (msi_input or "").strip()
    if not msi_input:
        return None, None

    # ── Tier 1: already a GUID ────────────────────────────────────────────
    if is_guid(msi_input):
        if debug:
            print(f"[DEBUG] Input is GUID: {msi_input}", file=sys.stderr)
        display = run_az_cmd(["az", "ad", "sp", "show", "--id", msi_input,
                               "--query", "displayName", "-o", "tsv"])
        if not display:
            display = run_az_cmd([
                "az", "identity", "list",
                "--query", f"[?principalId=='{msi_input}'].name | [0]",
                "-o", "tsv",
            ])
        return msi_input, (display or msi_input)

    # ── Tier 2: "name|resourceGroup" format ───────────────────────────────
    if "|" in msi_input:
        name, rg = [x.strip() for x in msi_input.split("|", 1)]
        if debug:
            print(f"[DEBUG] identity show: name={name}, rg={rg}", file=sys.stderr)
        raw = run_az_cmd([
            "az", "identity", "show",
            "--name", name, "--resource-group", rg,
            "--query", "{principalId:principalId, name:name}",
            "-o", "json",
        ])
        if raw:
            try:
                data = json.loads(raw)
                pid  = data.get("principalId")
                if pid and is_guid(pid):
                    return pid, data.get("name", name)
            except json.JSONDecodeError:
                pass

    # ── Tier 3: UAMI name search ──────────────────────────────────────────
    if debug:
        print(f"[DEBUG] identity list search: {msi_input}", file=sys.stderr)
    raw = run_az_cmd([
        "az", "identity", "list",
        "--query", f"[?name=='{msi_input}'] | [0].{{principalId:principalId, name:name}}",
        "-o", "json",
    ])
    if raw:
        try:
            data = json.loads(raw)
            pid  = data.get("principalId")
            if pid and is_guid(pid):
                return pid, data.get("name", msi_input)
        except json.JSONDecodeError:
            pass

    # ── Tier 4a: Service principal exact display name ─────────────────────
    if debug:
        print(f"[DEBUG] sp list exact: {msi_input}", file=sys.stderr)
    raw = run_az_cmd([
        "az", "ad", "sp", "list",
        "--display-name", msi_input,
        "--query", "[0].{id:id, displayName:displayName}",
        "-o", "json",
    ])
    if raw:
        try:
            data = json.loads(raw)
            pid  = data.get("id")
            if pid and is_guid(pid):
                return pid, data.get("displayName", msi_input)
        except json.JSONDecodeError:
            pass

    # ── Tier 4b: Broad SP search (partial name match, requires confirmation) ─
    if debug:
        print(f"[DEBUG] sp list broad contains search: {msi_input}", file=sys.stderr)
    raw = run_az_cmd([
        "az", "ad", "sp", "list", "--all",
        "--query",
        f"[?contains(displayName, '{msi_input}')] | [0].{{id:id, displayName:displayName}}",
        "-o", "json",
    ])
    if raw:
        try:
            data = json.loads(raw)
            pid  = data.get("id")
            disp = data.get("displayName")
            if pid and is_guid(pid):
                print(f"[INFO] Partial match found: '{disp}' (ID: {pid})")
                confirm = input("Is this the correct identity? (y/n): ").strip().lower()
                if confirm == "y":
                    return pid, disp
        except json.JSONDecodeError:
            pass

    return None, None


# ─────────────────────────────────────────────────────────────────────────────
# ACL parsing
# ─────────────────────────────────────────────────────────────────────────────

def parse_acl_entries(acl: str, debug: bool = False) -> List[Dict[str, str]]:
    """Parse an ADLS Gen2 ACL string into a list of structured entry dicts.

    ACL string format (POSIX-style, comma-separated):
      [default:]type:entity:permissions

    Examples:
      user:<guid>:rwx           named user with rwx
      group:<guid>:r-x          named group with r-x
      mask::r-x                 effective permission mask
      other::---                world permission
      user::rwx                 owning user
      default:user:<guid>:r-x   default (inherited) ACE for named user

    Returns list of dicts with keys:
      principal_type  : user / group / mask / other
      principal_name  : GUID, $superuser, $owninggroup, Mask, or Other
      principal_id    : GUID string (only for named user/group entries)
      permissions     : normalised "R,W,X" string or "-"
      is_default      : "True" / "False"
    """
    if not acl:
        return []

    entries: List[Dict[str, str]] = []
    for part in (p.strip() for p in acl.split(",") if p.strip()):
        segments   = part.split(":")
        is_default = False

        if segments and segments[0] == "default":
            is_default = True
            segments   = segments[1:]

        if len(segments) != 3:
            if debug:
                print(f"[DEBUG] Skipping malformed ACL entry: {part}", file=sys.stderr)
            continue

        typ, entity, perm = segments

        # Human-readable label for well-known special entries
        if not entity:
            label_map = {
                "user":  "$superuser",
                "group": "$owninggroup",
                "mask":  "Mask",
                "other": "Other",
            }
            principal_name = label_map.get(typ, f"${typ}")
        else:
            principal_name = entity

        # Normalise permissions: rwx → "R,W,X", r-- → "R", --- → "-"
        perm_lower = (perm or "").lower()
        perms      = []
        if "r" in perm_lower: perms.append("R")
        if "w" in perm_lower: perms.append("W")
        if "x" in perm_lower: perms.append("X")

        entries.append({
            "principal_type": typ,
            "principal_name": principal_name,
            "principal_id":   entity if is_guid(entity) else "",
            "permissions":    ",".join(perms) if perms else "-",
            "is_default":     "True" if is_default else "False",
        })

    return entries


# ─────────────────────────────────────────────────────────────────────────────
# Path scanner
# ─────────────────────────────────────────────────────────────────────────────

def is_valid_path(path: str) -> bool:
    """Return True if the path is non-empty and free of control characters."""
    return bool(path) and not any(c in path for c in ("\x00", "\r", "\n"))


def scan_path_for_principal(
    fs_client,
    path: str,
    target_principal_id: str,
    storage: str,
    container: str,
    is_directory: bool,
    include_mask: bool,
    include_special: bool,
    debug: bool = False,
) -> Tuple[List[Dict], bool]:
    """Read ACLs for a single path and return rows where target principal is present.

    Three row categories are collected:
      1. MSI rows      : entries whose principal_id == target_principal_id (always)
      2. Mask rows     : if --include-mask AND the MSI is present on this path
      3. Special rows  : if --include-special AND the MSI is present on this path
         (owning user $superuser, owning group $owninggroup, other)

    Azure SDK INFO/WARNING logs are suppressed during ACL reads to keep
    output clean during large recursive scans.

    Returns: (rows, success_bool)
    """
    if not is_valid_path(path):
        if debug:
            print(f"[DEBUG] Skipping invalid path: {repr(path)}", file=sys.stderr)
        return [], False

    # Normalise path — ensure leading slash
    clean_path = path.strip()
    if clean_path != "/" and not clean_path.startswith("/"):
        clean_path = "/" + clean_path

    # Temporarily suppress Azure SDK logs during ACL read
    azure_logger   = logging.getLogger("azure.storage.filedatalake")
    original_level = azure_logger.level
    azure_logger.setLevel(logging.CRITICAL)

    try:
        if is_directory:
            client   = fs_client.get_directory_client(clean_path)
            obj_type = "container-root" if clean_path == "/" else "directory"
        else:
            client   = fs_client.get_file_client(clean_path)
            obj_type = "file"

        acl_data = client.get_access_control()
        acl_text = acl_data.get("acl", "")

        if debug:
            print(f"[DEBUG] {clean_path}: {acl_text}", file=sys.stderr)

        entries     = parse_acl_entries(acl_text, debug)
        msi_entries = [e for e in entries if e.get("principal_id") == target_principal_id]
        has_msi     = bool(msi_entries)
        full_path   = f"/{container}{clean_path}" if clean_path != "/" else f"/{container}"

        rows: List[Dict] = []

        # Always emit rows for the target MSI
        for e in msi_entries:
            rows.append({
                "storage_account": storage,
                "container":       container,
                "path":            full_path,
                "object_type":     obj_type,
                "principal_id":    target_principal_id,
                "principal_type":  e["principal_type"],
                "permissions":     e["permissions"],
                "is_default":      e["is_default"],
            })

        # Optional: mask entries (only where MSI is present — provides effective permission context)
        if include_mask and has_msi:
            for e in entries:
                if e.get("principal_type") == "mask":
                    rows.append({
                        "storage_account": storage,
                        "container":       container,
                        "path":            full_path,
                        "object_type":     obj_type,
                        "principal_id":    "",
                        "principal_type":  "mask",
                        "permissions":     e["permissions"],
                        "is_default":      e["is_default"],
                    })

        # Optional: special entries (owning user, owning group, other)
        if include_special and has_msi:
            for e in entries:
                is_special = (
                    e.get("principal_type") == "other"
                    or e.get("principal_name") in ("$superuser", "$owninggroup")
                )
                if is_special:
                    rows.append({
                        "storage_account": storage,
                        "container":       container,
                        "path":            full_path,
                        "object_type":     obj_type,
                        "principal_id":    "",
                        "principal_type":  e["principal_type"],
                        "permissions":     e["permissions"],
                        "is_default":      e["is_default"],
                    })

        return rows, True

    except HttpResponseError as exc:
        if debug:
            sc = getattr(exc, "status_code", None)
            print(f"[DEBUG] HTTP {sc} on {clean_path}: {exc}", file=sys.stderr)
        return [], False
    except Exception as exc:
        if debug:
            print(f"[DEBUG] Error on {clean_path}: {type(exc).__name__}: {exc}", file=sys.stderr)
        return [], False
    finally:
        azure_logger.setLevel(original_level)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Extract ADLS Gen2 ACL entries for a specific Managed Identity (recursive by default).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive — prompts for all inputs
  python adls_acl_extractor.py --interactive

  # Non-interactive with all required args
  python adls_acl_extractor.py \\
    --storage-account mystorageaccount \\
    --container mycontainer \\
    --msi my-managed-identity-name

  # Use Object ID directly (fastest)
  python adls_acl_extractor.py \\
    --storage-account mystorageaccount \\
    --container mycontainer \\
    --msi xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

  # Include mask and special entries at matched paths
  python adls_acl_extractor.py ... --include-mask --include-special

  # Directories only, max 3 levels deep
  python adls_acl_extractor.py ... --exclude-files --max-depth 3

  # Scan a specific path only
  python adls_acl_extractor.py ... --path /raw/folder1

  # Container root only
  python adls_acl_extractor.py ... --root-only
        """,
    )

    p.add_argument("--storage-account",  help="Storage account name (without .dfs.core.windows.net)")
    p.add_argument("--container",        help="Container (filesystem) name")
    p.add_argument("--msi",              help="MSI name, Object ID GUID, or 'name|resource-group'")
    p.add_argument("--path",             help="Scan a specific path only (overrides recursive scan)")
    p.add_argument("--root-only",        action="store_true",
                   help="Scan container root only — disables recursive scan")
    p.add_argument("--exclude-files",    action="store_true",
                   help="Scan directories only — skip individual files")
    p.add_argument("--max-depth",        type=int, default=0,
                   help="Maximum recursion depth (0 = unlimited)")
    p.add_argument("--output",           help="Output CSV path (auto-generated if not specified)")
    p.add_argument("--include-mask",     action="store_true",
                   help="Include mask entries at paths where the target MSI is present")
    p.add_argument("--include-special",  action="store_true",
                   help="Include owning user/group/other entries at paths where MSI is present")
    p.add_argument("--debug",            action="store_true",
                   help="Enable verbose debug output")
    p.add_argument("--interactive",      action="store_true",
                   help="Prompt for inputs interactively")

    return p.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    # ── Collect required inputs ───────────────────────────────────────────
    if args.interactive or not all([args.storage_account, args.container, args.msi]):
        print("\n" + "=" * 70)
        print("ADLS Gen2 ACL Extractor — Managed Identity Scanner")
        print("=" * 70 + "\n")
        storage   = args.storage_account or input("Storage Account Name : ").strip()
        container = args.container       or input("Container Name       : ").strip()
        msi_input = args.msi             or input("MSI Name or Object ID: ").strip()
        if not all([storage, container, msi_input]):
            print("[ERROR] All three inputs are required.")
            sys.exit(1)
    else:
        storage   = args.storage_account
        container = args.container
        msi_input = args.msi

    debug = args.debug

    # ── Resolve MSI to principal ID ───────────────────────────────────────
    print(f"\n[INFO] Resolving identity: {msi_input}")
    principal_id, display_name = resolve_msi_to_principal_id(msi_input, debug=debug)

    if not principal_id:
        print("\n[ERROR] Could not resolve MSI to a principal ID.")
        print("\nTips:")
        print("  - Provide the Object (principal) ID GUID directly for fastest resolution")
        print("  - Use format 'identity-name|resource-group' for UAMI name lookup")
        print("  - Ensure 'az login' is authenticated with sufficient permissions")
        sys.exit(1)

    print(f"[OK] Resolved:  {display_name}  ({principal_id})\n")

    # ── Connect to ADLS Gen2 ──────────────────────────────────────────────
    account_url = f"https://{storage}.dfs.core.windows.net"
    print(f"[INFO] Connecting to {account_url}")
    try:
        cred    = get_credential()
        svc     = DataLakeServiceClient(
            account_url=account_url,
            credential=cred,
            audience="https://storage.azure.com/",
        )
        fs_client = svc.get_file_system_client(container)
    except Exception as exc:
        print(f"[ERROR] Connection failed: {exc}")
        sys.exit(1)

    include_mask    = bool(args.include_mask)
    include_special = bool(args.include_special)
    all_results: List[Dict] = []

    def do_scan(pth: str, is_dir: bool) -> None:
        rows, _ = scan_path_for_principal(
            fs_client=fs_client, path=pth,
            target_principal_id=principal_id,
            storage=storage, container=container,
            is_directory=is_dir,
            include_mask=include_mask,
            include_special=include_special,
            debug=debug,
        )
        all_results.extend(rows)

    # ── Scan ──────────────────────────────────────────────────────────────
    if args.path:
        print(f"[INFO] Scanning path: {args.path}")
        is_dir = not args.path.endswith((".txt", ".csv", ".json", ".parquet", ".xml", ".log"))
        do_scan(args.path, is_dir)

    elif args.root_only:
        print("[INFO] Scanning container root only")
        do_scan("/", True)

    else:
        print(f"[INFO] Full recursive scan of '{container}'")
        if args.exclude_files: print("[INFO] Files excluded (--exclude-files)")
        if args.max_depth:     print(f"[INFO] Max depth: {args.max_depth}")
        if include_mask:       print("[INFO] Mask entries included (--include-mask)")
        if include_special:    print("[INFO] Special entries included (--include-special)")
        print()

        print("[PROGRESS] Scanning container root...")
        do_scan("/", True)
        print(f"[PROGRESS] {len(all_results)} rows collected so far.\n")

        print("[PROGRESS] Scanning all paths recursively...")
        scanned = skipped = errors = 0

        try:
            for item in fs_client.get_paths(recursive=True):
                depth = item.name.count("/")
                if args.max_depth and depth > args.max_depth:
                    skipped += 1
                    continue
                if args.exclude_files and not item.is_directory:
                    skipped += 1
                    continue

                rows, ok = scan_path_for_principal(
                    fs_client=fs_client,
                    path=f"/{item.name}",
                    target_principal_id=principal_id,
                    storage=storage, container=container,
                    is_directory=bool(item.is_directory),
                    include_mask=include_mask,
                    include_special=include_special,
                    debug=debug,
                )
                if ok:
                    all_results.extend(rows)
                    scanned += 1
                else:
                    errors += 1

                total = scanned + errors
                if total % 100 == 0:
                    print(
                        f"[PROGRESS] Scanned {scanned:,} | "
                        f"Rows {len(all_results):,} | "
                        f"Skipped {skipped:,} | Errors {errors:,}"
                    )

        except Exception as exc:
            print(f"[ERROR] Path enumeration failed: {exc}")
            if debug:
                import traceback
                traceback.print_exc()

        print(f"\n[INFO] Scan complete — scanned {scanned:,}, skipped {skipped:,}, errors {errors:,}")

    # ── Write CSV ─────────────────────────────────────────────────────────
    if args.output:
        out_file = args.output
    else:
        safe_name = "".join(c if c.isalnum() else "_" for c in (display_name or "msi"))[:30]
        out_file  = f"{storage}_{container}_{safe_name}_{principal_id[:8]}_acl.csv"

    columns = [
        "storage_account", "container", "path", "object_type",
        "principal_id", "principal_type", "permissions", "is_default",
    ]
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in all_results:
            writer.writerow({k: row.get(k, "") for k in columns})

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("ACL Extraction Complete")
    print("=" * 70)
    print(f"  Storage Account : {storage}")
    print(f"  Container       : {container}")
    print(f"  Identity        : {display_name}")
    print(f"  Principal ID    : {principal_id}")
    print(f"  Rows written    : {len(all_results):,}")
    print(f"  Output file     : {out_file}")
    print("=" * 70)

    if not all_results:
        print("\n[NOTE] No ACL entries found for this identity.")
        print("       Confirm the identity has explicit ACEs on at least one path in this container.")


if __name__ == "__main__":
    main()
