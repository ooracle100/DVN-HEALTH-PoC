#!/usr/bin/env python3
# recompute_kpi_with_known_cols.py
import sys, re
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

IN = Path("expanded_per_dvn_joined.csv")
if not IN.exists():
    print("File expanded_per_dvn_joined.csv not found in current folder.")
    raise SystemExit(1)

pd.set_option('display.max_columns', 200)
df = pd.read_csv(IN, dtype=str, keep_default_na=False, na_values=['','NA','N/A'])

# Normalize latency: column exists as LATENCYTODELIVERY_SECONDS with numbers and "N/A"
lat_col = 'LATENCYTODELIVERY_SECONDS'
df['LATENCY_SECONDS_NUM'] = pd.to_numeric(df.get(lat_col, "").replace("N/A",""), errors='coerce')

# Normalize delivered boolean from DELIVERED_BOOL column (TRUE/FALSE)
status_col = 'DELIVERED_BOOL'
df['DELIVERED_BOOL_N'] = df.get(status_col, "").astype(str).str.upper().map({'TRUE': True, 'FALSE': False})
df['DELIVERED_BOOL_N'] = df['DELIVERED_BOOL_N'].fillna(False)

# Ensure DVN name column
if 'DVN_NAME' not in df.columns:
    df['DVN_NAME'] = df.get('DVN_ADDR', 'Unknown DVN')

# Ensure numeric fee columns exist (created by previous script); fallback to converting DVN_FEE_ETH
df['DVN_FEE_ETH_NUM'] = pd.to_numeric(df.get('DVN_FEE_ETH', df.get('DVN_FEE_IF_REQUIRED_ETH', df.get('DVN_FEE_IF_OPTIONAL_ETH'))), errors='coerce')
df['DVN_FEE_IF_REQUIRED_ETH_NUM'] = pd.to_numeric(df.get('DVN_FEE_IF_REQUIRED_ETH_NUM', df.get('DVN_FEE_IF_REQUIRED_ETH')), errors='coerce')
df['DVN_FEE_IF_OPTIONAL_ETH_NUM'] = pd.to_numeric(df.get('DVN_FEE_IF_OPTIONAL_ETH_NUM', df.get('DVN_FEE_IF_OPTIONAL_ETH')), errors='coerce')

# KPI aggregation
agg = df.groupby('DVN_NAME').agg(
    unique_messages=('GUID','nunique'),
    rows=('GUID','count'),
    total_fees_eth=('DVN_FEE_ETH_NUM', lambda s: float(s.dropna().sum()) if s.dropna().size>0 else 0.0),
    total_required_fees_eth=('DVN_FEE_IF_REQUIRED_ETH_NUM', lambda s: float(s.dropna().sum()) if s.dropna().size>0 else 0.0),
    total_optional_fees_eth=('DVN_FEE_IF_OPTIONAL_ETH_NUM', lambda s: float(s.dropna().sum()) if s.dropna().size>0 else 0.0),
    median_latency=('LATENCY_SECONDS_NUM', lambda s: float(s.dropna().median()) if s.dropna().size>0 else None),
    p95_latency=('LATENCY_SECONDS_NUM', lambda s: float(s.dropna().quantile(0.95)) if s.dropna().size>0 else None),
    delivered_messages=('DELIVERED_BOOL_N', lambda s: int(s.dropna().sum()))
).reset_index()

agg['delivered_rate'] = agg.apply(lambda r: float(r['delivered_messages']/r['unique_messages']) if r['unique_messages']>0 else None, axis=1)

agg.to_csv("kpi_by_dvn_final.csv", index=False)
print("Saved kpi_by_dvn_final.csv")
print(agg.sort_values('total_fees_eth', ascending=False).head(20).to_string(index=False))

# --- Charts ---
# 1) Fee split (required vs optional) top 10
top10 = agg.sort_values('total_fees_eth', ascending=False).head(10)['DVN_NAME'].tolist()
top_df = df[df['DVN_NAME'].isin(top10)]
pivot = top_df.pivot_table(index='DVN_NAME', values=['DVN_FEE_IF_REQUIRED_ETH_NUM','DVN_FEE_IF_OPTIONAL_ETH_NUM'], aggfunc='sum', fill_value=0)
if not pivot.empty:
    pivot.plot(kind='bar', stacked=True, figsize=(10,5))
    plt.title("Top 10 DVNs: Required vs Optional Fee revenue (ETH)")
    plt.ylabel("ETH")
    plt.tight_layout()
    plt.savefig("chart_fees_split_top10.png")
    plt.close()
    print("Saved chart_fees_split_top10.png")

# 2) Latency boxplot top 8 by rows
top8 = agg.sort_values('rows', ascending=False).head(8)['DVN_NAME'].tolist()
box_data = [df[df['DVN_NAME']==d]['LATENCY_SECONDS_NUM'].dropna().astype(float).values for d in top8]
labels = top8
if any(len(x)>0 for x in box_data):
    plt.figure(figsize=(10,5))
    plt.boxplot([x for x in box_data if len(x)>0], labels=[labels[i] for i,x in enumerate(box_data) if len(x)>0])
    plt.title("Latency (s) distribution for top DVNs")
    plt.ylabel("Latency (s)")
    plt.tight_layout()
    plt.savefig("chart_latency_boxplot_top8.png")
    plt.close()
    print("Saved chart_latency_boxplot_top8.png")

# 3) Messages per day top 5
if 'SOURCETIMESTAMP' in df.columns:
    df['day'] = pd.to_datetime(df['SOURCETIMESTAMP'], errors='coerce').dt.date
    top5 = agg.sort_values('rows', ascending=False).head(5)['DVN_NAME'].tolist()
    ts = df[df['DVN_NAME'].isin(top5)].groupby(['day','DVN_NAME']).size().unstack(fill_value=0)
    if not ts.empty:
        ts.plot(figsize=(10,4))
        plt.title("Messages per day for top 5 DVNs")
        plt.tight_layout()
        plt.savefig("chart_messages_per_day_top5.png")
        plt.close()
        print("Saved chart_messages_per_day_top5.png")
else:
    print("SOURCETIMESTAMP not present; skipping time series.")

print("Done.")
