"""Look at real market data: fetch one symbol, show it, save a CSV and a chart.

Run from the repo root:
    python scripts/view_data.py            (AAPL, last 750 trading days)
    python scripts/view_data.py MSFT 1500  (symbol, number of rows)

Outputs go to the scratch/ folder. Do NOT commit that folder.
Uses 1 API call per run. The API key is never printed.
"""
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # draw to a file, no window needed
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from ingestion.provider import fetch_symbol

load_dotenv()

SYMBOL = sys.argv[1].upper() if len(sys.argv) > 1 else "AAPL"
ROWS = int(sys.argv[2]) if len(sys.argv) > 2 else 750
OUT = Path("scratch")
OUT.mkdir(exist_ok=True)

data = fetch_symbol(SYMBOL)
data["values"] = data["values"][:ROWS]

print("\n=== META (what the API says about this series) ===")
for k, v in data["meta"].items():
    print(f"{k}: {v}")

# Raw values are all strings. Convert types the way the Transformation step will.
df = pd.DataFrame(data["values"])
print("\n=== RAW TYPES (as received) ===")
print(df.dtypes.to_string())

df["datetime"] = pd.to_datetime(df["datetime"])
for col in ["open", "high", "low", "close", "volume"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")
df = df.sort_values("datetime").reset_index(drop=True)

df["daily_return"] = df["close"].pct_change()
df["ma_7"] = df["close"].rolling(7).mean()
df["ma_30"] = df["close"].rolling(30).mean()

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

print("\n=== FIRST 5 ROWS ===")
print(df.head().to_string(index=False))
print("\n=== LAST 5 ROWS ===")
print(df.tail().to_string(index=False))

print("\n=== SUMMARY STATS ===")
print(df[["close", "volume", "daily_return"]].describe().to_string())

print("\n=== QUALITY CHECKS ===")
print("Rows:", len(df))
print("Date range:", df["datetime"].min().date(), "to", df["datetime"].max().date())
print("Missing values per column:")
print(df.isna().sum().to_string())
print("Duplicate dates:", int(df["datetime"].duplicated().sum()))
gaps = df["datetime"].diff().dt.days.value_counts().sort_index()
print("Days between consecutive rows (3 = weekend, bigger = holiday or gap):")
print(gaps.to_string())

csv_path = OUT / f"{SYMBOL}_daily.csv"
df.to_csv(csv_path, index=False)

fig, (ax1, ax2) = plt.subplots(
    2, 1, figsize=(11, 7), sharex=True, gridspec_kw={"height_ratios": [3, 1]}
)
ax1.plot(df["datetime"], df["close"], label="Close", linewidth=1.2)
ax1.plot(df["datetime"], df["ma_7"], label="7-day MA", linewidth=1)
ax1.plot(df["datetime"], df["ma_30"], label="30-day MA", linewidth=1)
ax1.set_title(f"{SYMBOL} daily close with moving averages")
ax1.legend()
ax1.grid(alpha=0.3)
ax2.bar(df["datetime"], df["volume"], width=1.0)
ax2.set_title("Volume")
ax2.grid(alpha=0.3)
fig.tight_layout()
chart_path = OUT / f"{SYMBOL}_chart.png"
fig.savefig(chart_path, dpi=120)

print(f"\nSaved CSV:   {csv_path}  (open it in Excel)")
print(f"Saved chart: {chart_path}")

if sys.platform.startswith("win"):
    try:
        os.startfile(chart_path)  # opens the chart image on Windows
    except OSError:
        pass
