"""
load_history.py  —  Bulk-load RedAlert history into Cloudflare D1
Run once for initial load, then daily for incremental updates.

Fill in the four CONFIG values below before running.
Requirements:  pip install requests
"""

import requests, json, time, sys
from datetime import datetime, timezone

# ─── CONFIG ──────────────────────────────────────────────────────────────────
REDALERT_KEY   = "YOUR_REDALERT_PRIVATE_KEY"
CF_ACCOUNT_ID  = "YOUR_CLOUDFLARE_ACCOUNT_ID"
CF_API_TOKEN   = "YOUR_CLOUDFLARE_API_TOKEN"    # needs D1 write permission
D1_DATABASE_ID = "YOUR_D1_DATABASE_ID"
OP_START       = "2026-02-28T00:00:00Z"         # operation start date
# ─────────────────────────────────────────────────────────────────────────────

REDALERT_BASE = "https://redalert.orielhaim.com"
D1_URL        = (f"https://api.cloudflare.com/client/v4/accounts/"
                 f"{CF_ACCOUNT_ID}/d1/database/{D1_DATABASE_ID}")

RA_HEADERS = {"Authorization": f"Bearer {REDALERT_KEY}", "Accept": "application/json"}
CF_HEADERS = {"Authorization": f"Bearer {CF_API_TOKEN}", "Content-Type": "application/json"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def d1_query(sql, params=None):
    """Run a single SQL statement on D1."""
    body = {"sql": sql}
    if params:
        body["params"] = params
    r = requests.post(f"{D1_URL}/query", headers=CF_HEADERS, json=body)
    r.raise_for_status()
    result = r.json()
    if not result.get("success"):
        raise RuntimeError(f"D1 error: {result.get('errors')}")
    return result["result"]


def d1_batch(statements):
    """Run a list of {sql, params} dicts in one D1 batch call (max ~100 at a time)."""
    r = requests.post(f"{D1_URL}/batch", headers=CF_HEADERS,
                      json={"requests": statements})
    r.raise_for_status()
    result = r.json()
    if not result.get("success"):
        raise RuntimeError(f"D1 batch error: {result.get('errors')}")


def fetch_page(start_date, offset, limit=1000):
    params = {"startDate": start_date, "limit": limit, "offset": offset, "order": "asc"}
    r = requests.get(f"{REDALERT_BASE}/api/stats/history",
                     headers=RA_HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def get_last_ts():
    """Return the timestamp of the most recent alert already in D1, or OP_START."""
    try:
        res = d1_query("SELECT MAX(ts) AS last_ts FROM alerts")
        val = res[0]["results"][0]["last_ts"] if res else None
        return val if val else OP_START
    except Exception:
        return OP_START


# ── Main ──────────────────────────────────────────────────────────────────────

def load(start_date):
    print(f"Starting load from {start_date}")
    offset      = 0
    total_new   = 0
    page_size   = 1000
    BATCH_SIZE  = 80   # D1 batch limit per request

    while True:
        print(f"  Fetching offset={offset} ...", end=" ", flush=True)
        page = fetch_page(start_date, offset, page_size)
        alerts    = page.get("data", [])
        pagination = page.get("pagination", {})
        print(f"{len(alerts)} alerts  (total={pagination.get('total')})")

        if not alerts:
            break

        # Build batch statements
        stmts = []
        for a in alerts:
            stmts.append({
                "sql":    "INSERT OR IGNORE INTO alerts (id, ts, type, origin) VALUES (?,?,?,?)",
                "params": [a["id"], a["timestamp"], a.get("type"), a.get("origin")]
            })
            for c in a.get("cities", []):
                stmts.append({
                    "sql":    "INSERT OR IGNORE INTO alert_cities (alert_id, city_id, city_name) VALUES (?,?,?)",
                    "params": [a["id"], c["id"], c.get("name")]
                })

        # Send in chunks
        for i in range(0, len(stmts), BATCH_SIZE):
            chunk = stmts[i:i + BATCH_SIZE]
            d1_batch(chunk)
            time.sleep(0.15)   # be polite to D1 rate limits

        total_new += len(alerts)
        offset    += len(alerts)

        if not pagination.get("hasMore"):
            break

        time.sleep(0.5)   # avoid hammering RedAlert API

    print(f"\nDone. Inserted/skipped {total_new} alert records.")


def sync_zones_from_summary():
    """
    Pull topCities from the RedAlert summary (which includes zone names)
    and upsert them into the city_zones lookup table.
    """
    print("\nSyncing city→zone mappings from summary ...")
    params = {
        "startDate": OP_START,
        "include":   "topCities",
        "topLimit":  200,          # get as many as possible
    }
    r = requests.get(f"{REDALERT_BASE}/api/stats/summary",
                     headers=RA_HEADERS, params=params, timeout=20)
    r.raise_for_status()
    top_cities = r.json().get("topCities", [])

    stmts = []
    for city in top_cities:
        if city.get("zone"):
            stmts.append({
                "sql":    ("INSERT INTO city_zones (city_id, city_name, zone) VALUES (?,?,?) "
                           "ON CONFLICT(city_id) DO UPDATE SET zone=excluded.zone"),
                "params": [None, city["city"], city["zone"]]  # no city_id in summary
            })

    # summary topCities doesn't return city_id, only name — update by name instead
    for city in top_cities:
        if city.get("zone"):
            d1_query(
                "INSERT OR REPLACE INTO city_zones (city_id, city_name, zone) "
                "SELECT ac.city_id, ?, ? FROM alert_cities ac "
                "WHERE ac.city_name = ? LIMIT 1",
                [city["city"], city["zone"], city["city"]]
            )
    print(f"  Synced {len(top_cities)} city→zone mappings.")


if __name__ == "__main__":
    start = get_last_ts()
    print(f"Last record in D1: {start}")
    load(start)
    sync_zones_from_summary()
    print("\nAll done!")
