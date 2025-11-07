# dvn_dashboard_viz.py
# Single script to produce:
#  - chart_latency_vs_fees_fixed_precision.png
#  - chart_required_optional_breakdown.png
#
# Safe: looks for multiple input files and computes required/optional counts if needed.

import os
import sys
import math
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from textwrap import shorten

# --- CONFIG ---
OUT_LATENCY_FEES = "chart_latency_vs_fees_fixed_precision.png"
OUT_REQ_OPT = "chart_required_optional_breakdown.png"
# candidate input files (in order of preference)
KPI_CANDIDATES = [
    "kpi_by_dvn_final.csv",
    "kpi_by_dvn_latency_added.csv",
    "kpi_combined_fees_latency_rolecount.csv"
]
REQOPT_CANDIDATES = [
    "dvn_required_optional_summary.csv",
    "dvn_enriched_v2_per_dvn_rows.csv",
    "expanded_per_dvn_joined.csv"
]

# small helper
def try_read_csv(paths):
    for p in paths:
        if os.path.exists(p):
            print(f"Loading: {p}")
            try:
                return pd.read_csv(p)
            except Exception as e:
                print(f"ERROR reading {p}: {e}")
                continue
    return None

# --- Load KPI dataframe (fees + median_latency expected) ---
df_kpi = try_read_csv(KPI_CANDIDATES)
if df_kpi is None:
    print("ERROR: No KPI file found among:", KPI_CANDIDATES)
    sys.exit(1)

# Normalize column names (lowercase keys)
df_kpi.columns = [c.strip() for c in df_kpi.columns]

# Ensure numeric types for plotting
for col in ['total_fees_eth', 'median_latency', 'required_count', 'optional_count', 'rows']:
    if col in df_kpi.columns:
        df_kpi[col] = pd.to_numeric(df_kpi[col], errors='coerce')

# --------------------
# Chart 1: latency vs fees
# --------------------
# Need: DVN_NAME, total_fees_eth, median_latency, required_count (for bubble size)
if 'DVN_NAME' not in df_kpi.columns:
    # try alternative name variants
    alt = [c for c in df_kpi.columns if c.lower() == 'dvn_name' or 'dvn' in c.lower() and 'name' in c.lower()]
    if alt:
        df_kpi = df_kpi.rename(columns={alt[0]: 'DVN_NAME'})
    else:
        print("ERROR: no DVN_NAME column found in KPI file.")
        sys.exit(1)

# fill missing numeric columns with zeros to avoid plotting errors
if 'total_fees_eth' not in df_kpi.columns:
    print("WARN: total_fees_eth missing; trying to compute from available columns...")
    if 'total_fees' in df_kpi.columns:
        df_kpi['total_fees_eth'] = pd.to_numeric(df_kpi['total_fees'], errors='coerce')
    else:
        df_kpi['total_fees_eth'] = 0.0

if 'median_latency' not in df_kpi.columns:
    print("WARN: median_latency missing in KPI file; chart will skip those DVNs.")
    # we'll drop NaNs later

if 'required_count' not in df_kpi.columns:
    # default fallback (so bubbles show)
    df_kpi['required_count'] = pd.Series(0, index=df_kpi.index)

# create plotting subset with numeric median_latency
plot_df = df_kpi.copy()
plot_df['total_fees_eth'] = pd.to_numeric(plot_df['total_fees_eth'], errors='coerce')
plot_df['median_latency'] = pd.to_numeric(plot_df['median_latency'], errors='coerce')

plot_df = plot_df.dropna(subset=['median_latency', 'total_fees_eth'])
if plot_df.empty:
    print("No numeric median_latency + total_fees_eth rows found — skipping latency vs fees chart.")
else:
    # ensure ordering for consistent color assignment by stack if available
    # sizes: scale by required_count for visibility
    sizes = ((plot_df['required_count'].fillna(0).astype(float) + 1.0) * 60).clip(lower=20, upper=2000)

    fig, ax = plt.subplots(figsize=(10,6))
    sc = ax.scatter(plot_df['total_fees_eth'], plot_df['median_latency'],
                    s=sizes, alpha=0.75, edgecolor='k', linewidth=0.3)

    # labels & formatters
    ax.set_xlabel('Total fees (ETH)')
    ax.set_ylabel('Median latency (s)')
    ax.set_title('DVN — total fees (ETH) vs median latency (s)')

    # show more precision on x-axis (fees)
    ax.xaxis.set_major_formatter(mtick.FormatStrFormatter('%.8f'))
    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.1f'))

    # annotate points; offset labels slightly to reduce overlap
    for _, row in plot_df.iterrows():
        x = row['total_fees_eth']
        y = row['median_latency']
        name = str(row['DVN_NAME'])
        # small x offset to avoid covering the bubble
        ax.text(x * 1.0006 if x!=0 else 0.0000001, y + 0.6, shorten(name, 24), fontsize=8, ha='left', va='bottom')

    plt.tight_layout()
    plt.savefig(OUT_LATENCY_FEES, dpi=200)
    print("Saved:", OUT_LATENCY_FEES)

# --------------------
# Chart 2: required vs optional breakdown (top 20 by required_count)
# --------------------
# Try to load a ready summary
df_reqopt = None
for p in REQOPT_CANDIDATES:
    if os.path.exists(p):
        print("Loading for req/opt chart:", p)
        try:
            df_reqopt = pd.read_csv(p)
            break
        except Exception as e:
            print("ERROR reading", p, e)

# If not found or doesn't have counts, try to compute from expanded per-dvn rows
if df_reqopt is None or not ({'DVN_NAME','required_count','optional_count'} <= set(df_reqopt.columns)):
    print("Attempting to compute required/optional counts from expanded_per_dvn_joined.csv...")
    if os.path.exists("expanded_per_dvn_joined.csv"):
        df_exp = pd.read_csv("expanded_per_dvn_joined.csv")
        # normalized columns
        if 'DVN_NAME' not in df_exp.columns and 'DVN_Name' in df_exp.columns:
            df_exp = df_exp.rename(columns={'DVN_Name':'DVN_NAME','DVN_Fee':'DVN_FEE_WEI'})
        # ROLE column expected: 'required' or 'optional' (case-insensitive)
        if 'ROLE' in df_exp.columns:
            df_exp['ROLE'] = df_exp['ROLE'].astype(str).str.lower()
            required = df_exp[df_exp['ROLE']=='required'].groupby('DVN_NAME').size().rename('required_count')
            optional = df_exp[df_exp['ROLE']=='optional'].groupby('DVN_NAME').size().rename('optional_count')
            df_reqopt = pd.concat([required, optional], axis=1).fillna(0).reset_index()
        else:
            print("ERROR: expanded_per_dvn_joined.csv missing 'ROLE' column. Cannot compute req/opt counts.")
            df_reqopt = None
    else:
        print("No source to compute required/optional counts found.")

# If we have req/opt dataframe, proceed to plot top 20 by required_count
if df_reqopt is None or df_reqopt.empty:
    print("No required/optional data available — skipping req/opt chart.")
else:
    # normalize column names
    df_reqopt.columns = [c.strip() for c in df_reqopt.columns]
    if 'DVN_NAME' not in df_reqopt.columns:
        # try to find any dvn name column
        possible = [c for c in df_reqopt.columns if 'dvn' in c.lower() and 'name' in c.lower()]
        if possible:
            df_reqopt = df_reqopt.rename(columns={possible[0]:'DVN_NAME'})
    # ensure numeric
    for c in ['required_count','optional_count']:
        if c in df_reqopt.columns:
            df_reqopt[c] = pd.to_numeric(df_reqopt[c], errors='coerce').fillna(0).astype(int)
        else:
            df_reqopt[c] = 0

    # select top 20 by required_count
    top = df_reqopt.sort_values('required_count', ascending=False).head(20).set_index('DVN_NAME')
    if top.empty:
        print("Req/Opt table empty after sorting — skipping chart.")
    else:
        fig, ax = plt.subplots(figsize=(12,6))
        # stacked bar: required bottom, optional on top
        ax.bar(top.index, top['required_count'], label='Required')
        ax.bar(top.index, top['optional_count'], bottom=top['required_count'], label='Optional')
        ax.set_ylabel('Message Count')
        ax.set_title('DVN Roles Count (Required vs Optional) - Top 20 by required count')
        ax.set_xticklabels(top.index, rotation=45, ha='right', fontsize=9)
        ax.legend()
        plt.tight_layout()
        plt.savefig(OUT_REQ_OPT, dpi=200)
        print("Saved:", OUT_REQ_OPT)

print("Done.")
