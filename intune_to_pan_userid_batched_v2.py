# filename: intune_to_pan_userid_batched_v2.py
import os
import sys
import time
import json
import ipaddress
from typing import List, Dict, Tuple, Any
from datetime import datetime, timedelta, timezone

import requests
import msal

# -------------------- CONFIG --------------------
TENANT_ID     = os.getenv("INTUNE_TENANT_ID",     "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
CLIENT_ID     = os.getenv("INTUNE_CLIENT_ID",     "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
CLIENT_SECRET = os.getenv("INTUNE_CLIENT_SECRET", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
DEBUG = "false"

# Lookback window (minutes). Set to 5 for scheduled runs; bump while testing.
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "5"))

# PAN (posting to PAN)
PAN_HOST            = os.getenv("PAN_HOST", "x.x.x.x")
PAN_API_KEY         = os.getenv("PAN_API_KEY", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")  # set your real key
PAN_VERIFY_SSL      = os.getenv("PAN_VERIFY_SSL", "false").lower() == "true"  # false mimics curl -k
PAN_UID_TIMEOUT     = int(os.getenv("PAN_UID_TIMEOUT", "600"))  # seconds to keep mapping
PAN_TARGET          = os.getenv("PAN_TARGET", "").strip()   # optional: firewall serial/hostname for PAN
PAN_VSYS            = os.getenv("PAN_VSYS", "").strip()     # optional: e.g., "vsys1"
PAN_USERNAME_PREFIX = os.getenv("PAN_USERNAME_PREFIX", "").strip()  # optional: e.g., "wireless/" or "domain\\"

# Allowed wired subnets
ALLOWED_SUBNETS = [
    "192.168.11.0/24",
    "192.168.15.0/24",
    "192.168.100.0/24",
    "192.168.111.0/24",
    "192.168.112.0/24",
    "192.168.120.0/24",
]

# Microsoft Graph constants
GRAPH_RESOURCE = "https://graph.microsoft.com"
GRAPH_BASE     = f"{GRAPH_RESOURCE}/beta"  # beta required for hardwareInformation.wiredIPv4Addresses
AUTHORITY      = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE          = [f"{GRAPH_RESOURCE}/.default"]

# $batch settings
BATCH_ENDPOINT           = f"{GRAPH_RESOURCE}/beta/$batch"
GRAPH_BATCH_SIZE         = int(os.getenv("GRAPH_BATCH_SIZE", "20"))  # Graph limit is 20
BATCH_MAX_RETRIES        = int(os.getenv("BATCH_MAX_RETRIES", "3"))
FALLBACK_TO_SINGLE_CALLS = os.getenv("FALLBACK_TO_SINGLE_CALLS", "true").lower() == "true"

# Debug
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

# HTTP session
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})
SESSION.timeout = 60


# -------------------- AUTH --------------------
def get_token() -> str:
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"Token acquisition failed: {result}")
    return result["access_token"]


def auth_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# -------------------- UTIL --------------------
def since_iso_utc(minutes: int) -> str:
    dt = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def backoff_sleep(attempt: int, retry_after_header: str | None = None):
    if retry_after_header:
        try:
            delay = float(retry_after_header)
            time.sleep(max(0.1, min(30.0, delay)))
            return
        except Exception:
            pass
    time.sleep(min(8.0, 0.5 * (2 ** attempt)))


# -------------------- GRAPH LIST (IDs) --------------------
def list_recent_device_ids(token: str, since_iso: str) -> List[Dict[str, str]]:
    """
    Returns dicts: { id, userPrincipalName, userDisplayName } for devices with lastSyncDateTime >= since_iso
    """
    url = f"{GRAPH_BASE}/deviceManagement/managedDevices"
    params = {
        "$select": "id,userPrincipalName,userDisplayName,lastSyncDateTime",
        "$filter": f"lastSyncDateTime ge {since_iso}",
        "$orderby": "lastSyncDateTime desc",
        "$top": "200",
    }

    items: List[Dict[str, str]] = []
    while True:
        resp = SESSION.get(url, headers=auth_headers(token), params=params, timeout=60)
        if resp.status_code in (429, 500, 502, 503, 504):
            if DEBUG:
                print(f"[DEBUG] list IDs got {resp.status_code}; retry-after={resp.headers.get('Retry-After')}")
            backoff_sleep(1, resp.headers.get("Retry-After"))
            continue
        if resp.status_code != 200:
            raise RuntimeError(f"GET {resp.url} -> {resp.status_code} {resp.text}")
        data = resp.json()
        vals = data.get("value", [])
        for v in vals:
            items.append({
                "id": v.get("id", ""),
                "userPrincipalName": v.get("userPrincipalName") or "",
                "userDisplayName": v.get("userDisplayName") or "",
            })
        next_link = data.get("@odata.nextLink")
        if not next_link:
            break
        url, params = next_link, None
    return items


# -------------------- GRAPH PER-ID DETAILS VIA $BATCH --------------------
def batch_fetch_device_details(token: str, ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Returns: managedDeviceId -> { userPrincipalName, userDisplayName, hardwareInformation }
    Uses /beta/$batch with versionless sub-request URLs.
    Retries sub-responses 429/5xx; falls back to single GETs if still failing.
    """
    results: Dict[str, Dict[str, Any]] = {}
    headers = {**auth_headers(token), "Content-Type": "application/json"}

    def build_body(id_list: List[str]) -> Dict[str, Any]:
        reqs = []
        for idx, dev_id in enumerate(id_list, start=1):
            # IMPORTANT: versionless sub-request URL since we're posting to /beta/$batch
            reqs.append({
                "id": str(idx),
                "method": "GET",
                "url": f"/deviceManagement/managedDevices/{dev_id}?$select=userPrincipalName,userDisplayName,hardwareInformation",
            })
        return {"requests": reqs}

    for offset in range(0, len(ids), GRAPH_BATCH_SIZE):
        chunk = ids[offset: offset + GRAPH_BATCH_SIZE]
        remaining = chunk[:]
        attempt = 0

        while remaining and attempt < BATCH_MAX_RETRIES:
            body = build_body(remaining)
            resp = SESSION.post(BATCH_ENDPOINT, headers=headers, data=json.dumps(body), timeout=60)
            if resp.status_code in (429, 500, 502, 503, 504):
                if DEBUG:
                    print(f"[DEBUG] $batch HTTP {resp.status_code}; retry-after={resp.headers.get('Retry-After')}")
                attempt += 1
                backoff_sleep(attempt, resp.headers.get("Retry-After"))
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"$batch HTTP {resp.status_code}: {resp.text}")

            data = resp.json()
            if DEBUG:
                st_counts: Dict[int, int] = {}
            next_round: List[str] = []

            for item in data.get("responses", []):
                sub_id = item.get("id")
                status = int(item.get("status", 0))
                body = item.get("body", {})
                idx = int(sub_id) - 1
                dev_id = remaining[idx]

                if DEBUG:
                    st_counts[status] = st_counts.get(status, 0) + 1

                if status == 200:
                    results[dev_id] = {
                        "userPrincipalName": body.get("userPrincipalName"),
                        "userDisplayName": body.get("userDisplayName"),
                        "hardwareInformation": body.get("hardwareInformation") or {},
                    }
                elif status in (429, 500, 502, 503, 504):
                    next_round.append(dev_id)
                else:
                    if DEBUG:
                        print(f"[DEBUG] $batch sub status {status} for device {dev_id}: {json.dumps(body)[:400]}")

            if DEBUG and st_counts:
                print(f"[DEBUG] $batch sub-status counts: {st_counts}")

            remaining = next_round
            attempt += 1
            if remaining:
                backoff_sleep(attempt)

        if remaining and FALLBACK_TO_SINGLE_CALLS:
            if DEBUG:
                print(f"[DEBUG] Falling back to single GET for {len(remaining)} device(s).")
            for dev_id in remaining:
                try:
                    single = single_get_device_detail(token, dev_id)
                    if single:
                        results[dev_id] = single
                except Exception as e:
                    if DEBUG:
                        print(f"[DEBUG] single GET failed for {dev_id}: {e}")
                time.sleep(0.02)

        time.sleep(0.02)

    return results


def single_get_device_detail(token: str, dev_id: str) -> Dict[str, Any] | None:
    url = f"{GRAPH_BASE}/deviceManagement/managedDevices/{dev_id}"
    params = {"$select": "userPrincipalName,userDisplayName,hardwareInformation"}
    resp = SESSION.get(url, headers=auth_headers(token), params=params, timeout=60)
    if resp.status_code == 200:
        b = resp.json()
        return {
            "userPrincipalName": b.get("userPrincipalName"),
            "userDisplayName": b.get("userDisplayName"),
            "hardwareInformation": b.get("hardwareInformation") or {},
        }
    if DEBUG:
        print(f"[DEBUG] single GET {resp.status_code} for {dev_id}: {resp.text[:200]}")
    return None


# -------------------- FILTERS / USERNAME --------------------
ALLOWED_NETWORKS = [ipaddress.ip_network(n) for n in ALLOWED_SUBNETS]

def ip_is_allowed_wired(ip: str) -> bool:
    try:
        ip_obj = ipaddress.ip_address(ip)
        return ip_obj.version == 4 and any(ip_obj in net for net in ALLOWED_NETWORKS)
    except ValueError:
        return False


def to_first_last_from_displayname(display_name: str | None) -> str | None:
    if not display_name:
        return None
    parts = [p for p in display_name.strip().split() if p]
    if len(parts) >= 2:
        first = parts[0].strip(".,").lower()
        last  = parts[-1].strip(".,").lower()
        if first and last:
            return f"{first}.{last}"
    return None


def normalize_username(upn: str | None, user_display_name: str | None) -> str:
    """
    Priority:
      1) UPN local part contains a dot -> return 'first.last'
      2) Else derive 'first.last' from display name 'First Last'
      3) Else return full UPN (email) if present
      4) Else return ''
    """
    if upn:
        local = upn.split("@")[0]
        if "." in local and local:
            return local
    fl = to_first_last_from_displayname(user_display_name)
    if fl:
        return fl
    return upn or ""


def apply_prefix(username: str) -> str:
    if not username:
        return ""
    if PAN_USERNAME_PREFIX:
        return f"{PAN_USERNAME_PREFIX}{username}"
    return username


def get_wired_ips(hwinfo: Dict[str, Any]) -> List[str]:
    arr = hwinfo.get("wiredIPv4Addresses") or []
    return [str(x).strip() for x in arr if str(x).strip()]


# -------------------- PAN USER-ID --------------------
def build_pan_url(host: str, api_key: str) -> str:
    # Be explicit with action=set; include target/vsys when posting to PAN
    base = f"https://{host}/api/?type=user-id&action=set&key={api_key}"
    if PAN_TARGET:
        base += f"&target={PAN_TARGET}"
    if PAN_VSYS:
        base += f"&vsys={PAN_VSYS}"
    return base


def build_uid_xml(user_ips: Dict[str, List[str]], timeout_seconds: int) -> str:
    """
    Emits one <entry> per IP address for each user (no grouped <ip> children).
    Example:
      <login>
        <entry name="first.last" ip="192.168.11.11" timeout="600"/> # set TTL for user by second
        <entry name="first.last" ip="192.168.15.12" timeout="600"/>
        <entry name="other.user" ip="192.168.111.4" timeout="600"/>
      </login>
    """
    def esc(s: str) -> str:
        return (s.replace("&", "&amp;")
                 .replace('"', "&quot;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;"))

    lines = []
    lines.append("<uid-message>")
    lines.append("  <version>1.0</version>")
    lines.append("  <type>update</type>")
    lines.append("  <payload>")
    lines.append("    <login>")
    for user, ips in sorted(user_ips.items(), key=lambda kv: kv[0].lower()):
        eu = esc(user)
        for ip in sorted(set(ips)):
            lines.append(f'      <entry name="{eu}" ip="{ip}" timeout="{timeout_seconds}"/>')
    lines.append("    </login>")
    lines.append("  </payload>")
    lines.append("</uid-message>")
    return "\n".join(lines)


def post_uid_xml_to_pan(xml_body: str, host: str, api_key: str, verify_ssl: bool) -> tuple[int, str]:
    url = build_pan_url(host, api_key)
    files = {"file": ("userid.xml", xml_body.encode("utf-8"), "text/xml")}
    resp = requests.post(url, files=files, timeout=45, verify=verify_ssl)
    return resp.status_code, resp.text


# -------------------- MAIN --------------------
def main():
    # Guards
    if any(v.startswith("<YOUR_") or v == "" for v in [TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
        print("Set INTUNE_TENANT_ID / INTUNE_CLIENT_ID / INTUNE_CLIENT_SECRET.")
        sys.exit(1)
    if not PAN_HOST or not PAN_API_KEY or PAN_API_KEY in ("xxxxx", "xxx"):
        print("Set PAN_HOST and PAN_API_KEY.")
        sys.exit(1)

    token = get_token()
    since = since_iso_utc(LOOKBACK_MINUTES)

    # Step 1: list device IDs in the window
    id_rows = list_recent_device_ids(token, since)
    if DEBUG:
        print(f"[DEBUG] Since {since}, IDs returned: {len(id_rows)}")

    if not id_rows:
        print(f"No devices synced since {since}. Nothing to send.")
        return

    # Step 2: fetch per-ID details (batched with retries + fallback)
    ids = [r["id"] for r in id_rows if r.get("id")]
    details_by_id = batch_fetch_device_details(token, ids)

    # Aggregate all allowed wired IPs per user (multiple <entry> per user)
    user_ips: Dict[str, List[str]] = {}

    for r in id_rows:
        dev_id = r["id"]
        detail = details_by_id.get(dev_id)
        if not detail:
            continue

        hw = detail.get("hardwareInformation") or {}
        wired = get_wired_ips(hw)
        if not wired:
            continue

        username = normalize_username(
            detail.get("userPrincipalName") or r.get("userPrincipalName"),
            detail.get("userDisplayName")   or r.get("userDisplayName"),
        )
        username = apply_prefix(username)
        if not username:
            continue

        for ip in wired:
            if ip_is_allowed_wired(ip):
                user_ips.setdefault(username, []).append(ip)

    if DEBUG:
        total_entries = sum(len(set(v)) for v in user_ips.values())
        print(f"[DEBUG] users: {len(user_ips)}, total entries: {total_entries}")
        shown = 0
        for u, ips in user_ips.items():
            print(f"[DEBUG] {u} -> {sorted(set(ips))[:5]}")
            shown += 1
            if shown >= 5:
                break

    if not user_ips:
        print(f"No wired IP mappings found in last {LOOKBACK_MINUTES} minutes (since {since}) for allowed subnets.")
        return

    # Build XML and POST to PAN
    xml_body = build_uid_xml(user_ips, PAN_UID_TIMEOUT)
    status, text = post_uid_xml_to_pan(xml_body, PAN_HOST, PAN_API_KEY, PAN_VERIFY_SSL)

    print(f"PAN responded: HTTP {status}")
    print(text[:2000])


if __name__ == "__main__":
    main()
