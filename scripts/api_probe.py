"""One-off test: does the API fit our project? Saves sample responses.

Run from the repo root:  python scripts/api_probe.py
Requires API_KEY in your .env file. The key is never printed.
"""
import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()
KEY = os.getenv("API_KEY")
if not KEY:
    sys.exit("API_KEY not found. Put API_KEY=your_key in the .env file (repo root).")

BASE = "https://api.twelvedata.com/time_series"
OUT = Path("tests/fixtures")
OUT.mkdir(parents=True, exist_ok=True)


def call(label: str, filename: str, **params) -> None:
    """Make one request, print what matters, save the raw JSON."""
    params.setdefault("apikey", KEY)
    r = requests.get(BASE, params=params, timeout=30)
    print(f"\n== {label}: HTTP {r.status_code}")
    try:
        data = r.json()
    except ValueError:
        print("Response is not JSON:", r.text[:200])
        return

    (OUT / filename).write_text(json.dumps(data, indent=2))
    print("Saved to:", OUT / filename)
    print("Top-level keys:", list(data.keys()))
    print("status field:", data.get("status"), "| code:", data.get("code"))

    values = data.get("values", [])
    if values:
        print("Rows:", len(values))
        print("Newest row:", values[0])
        print("Oldest row:", values[-1])
        print("Type of 'close':", type(values[0]["close"]).__name__)
    else:
        print("Message:", data.get("message"))


call("Good request (AAPL, max history)", "good_AAPL.json",
     symbol="AAPL", interval="1day", outputsize=5000)
time.sleep(8)  # stay under the free-tier per-minute limit
call("Bad symbol", "error_bad_symbol.json",
     symbol="ZZZZNOTREAL", interval="1day", outputsize=10)
time.sleep(8)
call("Bad API key", "error_bad_key.json",
     symbol="AAPL", interval="1day", outputsize=10, apikey="wrong_key")

print("\nDone. Copy the output above and bring it back for review.")
