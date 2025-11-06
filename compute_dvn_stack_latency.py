#!/usr/bin/env python3
# compute_dvn_stack_latency.py (updated)
import pandas as pd
import numpy as np
from pathlib import Path

INPUT_FILE = "expanded_per_dvn_joined.csv"
OUT_STACK = "stack_latency_summary.csv"
OUT_DVN = "dvn_stack_reliability.csv"

# Load data
df = pd.read_csv(INPUT_FILE, dtype=str, keep_default_na=False, na_values=['','NA','N/A'])
print(f"Loaded {len(df)} rows from {INPUT_FILE}")

# Ensure columns exist and normalize types
if 'ROLE' not in df.columns or 'DVN_NAME' not in df.columns:
    raise SystemExit("Missing ROLE or DVN_NAME columns in input file.")

# Normalize GUID and DVN_NAME and ROLE
df['GUID'] = df['GUID'].astype(str).str.lower().str.strip()
df['DVN_NAME'] = df['DVN_NAME'].astype(str).str.strip()
df['ROLE'] = df['ROLE'].astype(str).str.strip().str.lower()

# Parse and normalize latency into numeric seconds
lat_col = 'LATENCYTODELIVERY_SECONDS'
if lat_col in df.columns:
    df['LATENCY_S'] = pd.to_numeric(df[lat_col].astype(str).str.replace(r'[^0-9\.]', '', regex=True).replace('', np.nan), errors='coerce')
else:
    df['LATENCY_S'] = np.nan

# --- Build required-DVN stack per GUID ---
# Take only rows where ROLE == 'required', group DVN_NAME per GUID, produce sorted joined string
req = df[df['ROLE'] == 'required'].groupby('GUID')['DVN_NAME'] \
         .apply(lambda names: ' + '.join(sorted(set([n for n in names if n and n.lower() != 'nan'])))) \
         .reset_index(name='Required_Stack')

# If some GUIDs have no required DVNs (unlikely), mark them as Unknown
req['Required_Stack'] = req['Required_Stack'].replace('', 'Unknown')

# --- Build transaction-level latency table (one row per GUID) ---
# Some GUIDs may appear many times (one per DVN). Get first non-null latency per GUID
tx_latency = df[['GUID', 'LATENCY_S']].copy()
tx_latency = tx_latency[tx_latency['LATENCY_S'].notna()].drop_duplicates(subset=['GUID'], keep='first')
# If a GUID has no latency rows, it will be absent in tx_latency

# Merge stacks with tx_latency to get per-transaction stacks with latency
txs = req.merge(tx_latency, on='GUID', how='left')
# Keep only transactions with numeric latency (we need them to compute percentiles)
txs_valid = txs[txs['LATENCY_S'].notna()].copy()
print(f"Transactions with valid latency & required stack: {len(txs_valid)}")

# --- Stack-level aggregation ---
agg = (
    txs_valid.groupby('Required_Stack')['LATENCY_S']
    .agg(transactions='count',
         median_latency='median',
         avg_latency='mean',
         p95_latency=lambda s: float(np.percentile(s, 95)) if len(s.dropna())>0 else np.nan)
    .reset_index()
    .sort_values('transactions', ascending=False)
)

agg.to_csv(OUT_STACK, index=False)
print(f"Saved stack-level summary → {OUT_STACK}")
print(agg.head(12).to_string(index=False))

# --- DVN-level reliability derived from stacks ---
# Expand each stack row into per-DVN rows so we can compute per-DVN averages across stacks they appear in
rows = []
for _, row in agg.iterrows():
    stack = row['Required_Stack']
    if not stack or stack == 'Unknown':
        continue
    names = [n.strip() for n in stack.split('+')]
    for name in names:
        if name:
            rows.append({
                'DVN_NAME': name,
                'stack': stack,
                'transactions': int(row['transactions']),
                'median_latency': float(row['median_latency']),
                'avg_latency': float(row['avg_latency']),
                'p95_latency': float(row['p95_latency'])
            })

if len(rows) == 0:
    print("No DVN rows produced from stacks — check ROLE/DVN_NAME parsing.")
    dvn_summary = pd.DataFrame(columns=['DVN_NAME','stacks_involved','total_transactions','avg_median_latency','avg_p95_latency'])
else:
    dvn_df = pd.DataFrame(rows)
    dvn_summary = (
        dvn_df.groupby('DVN_NAME')
        .agg(
            stacks_involved=('stack', 'nunique'),
            total_transactions=('transactions', 'sum'),
            avg_median_latency=('median_latency', 'mean'),
            avg_p95_latency=('p95_latency', 'mean')
        )
        .reset_index()
    )

dvn_summary.to_csv(OUT_DVN, index=False)
print(f"Saved per-DVN reliability summary → {OUT_DVN}")
print(dvn_summary.sort_values('total_transactions', ascending=False).head(20).to_string(index=False))
