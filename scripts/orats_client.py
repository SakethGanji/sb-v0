#!/usr/bin/env python3
"""
Minimal ORATS Data API v2 client (Branch B access layer — branchB-plan.md).
Token lives in ~/.orats_token (mode 600, never in the repo or transcripts).
Tier: Delayed Data API ($99/mo, 20,000 requests/month) — hist EOD endpoints only;
every call counts against the monthly quota, so bulk pulls must be request-budgeted
(see branchB-plan.md §Access). Usage:

    from orats_client import get, quota_note
    meta = get("tickers", ticker="AAPL")          # -> parsed JSON (list of dicts)
    # hist endpoints accept ticker plus tradeDate ranges, e.g.
    # get("hist/smvsummaries", ticker="AAPL", tradeDate="2018-01-02,2020-12-31")

Discipline: NO IV/skew values for the 2018-2020 test window may be fetched before
branchB-preregistration.md is committed (schema/metadata calls are allowed).
"""
from __future__ import annotations
import json, time, urllib.parse, urllib.request
from pathlib import Path

BASE = "https://api.orats.io/datav2"
TOKEN = Path("~/.orats_token").expanduser().read_text().strip()


def get(endpoint: str, retries: int = 3, **params):
    params["token"] = TOKEN
    url = f"{BASE}/{endpoint}?{urllib.parse.urlencode(params)}"
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                payload = json.loads(r.read())
                return payload.get("data", payload)
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:   # quota/rate pressure
                time.sleep(5 * (attempt + 1)); continue
            raise RuntimeError(f"ORATS {endpoint} HTTP {e.code}: {e.read()[:200]}") from e
        except urllib.error.URLError:
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1)); continue
            raise


if __name__ == "__main__":
    print(json.dumps(get("tickers", ticker="AAPL"), indent=2))
