#!/usr/bin/env python3
# dvn_dashboard_viz.py
# Bubble chart (latency vs fees) + stacked bar. Color by two stack groups (only 2 colors + gray fallback).

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

OUT1 = "chart_latency_vs_fees.png"
OUT2 = "chart_required_optional_breakdown.png"

# Define stack membership sets (deterministic)
STACK_BLUE = set(['Deutsche Telekom','Canary','P2P'])
STACK_YELLOW = set(['LayerZero Labs','Google Cloud'])

def load_dvn_table():
    combined = Path("kpi_combined_fees_latency_rolecount.csv")
    fees_file = Path("kpi_by_dvn_final.csv")
    lat_file = Path("kpi_by_dvn_latency_added.csv")
    drv_file = Path("dvn_stack_reliability.csv")

    if combined.exists():
        df = pd.read_csv(combined)
    else:
        if not fees_file.exists():
            raise SystemExit("Missing kpi_by_dvn_final.csv - cannot proceed.")
        fees = pd.read_csv(fees_file)
        if lat_file.exists():
            lat = pd.read_csv(lat_file)
            df = fees.merge(lat, on='DVN_NAME', how='outer')
        else:
            df = fees.copy()

    df['DVN_NAME'] = df['DVN_NAME'].astype(str).str.strip()

    # Fill median_latency from dvn_stack_reliability if missing
    if 'median_latency' not in df.columns or df['median_latency'].isna().all():
        if drv_file.exists():
            drv = pd.read_csv(drv_file)
            drv['DVN_NAME'] = drv['DVN_NAME'].astype(str).str.strip()
            df = df.merge(drv[['DVN_NAME','avg_median_latency','avg_p95_latency']], on='DVN_NAME', how='left')
            if 'median_latency' not in df.columns:
                df['median_latency'] = df.get('avg_median_latency')
            if 'p95_latency' not in df.columns:
                df['p95_latency'] = df.get('avg_p95_latency')
        else:
            df['median_latency'] = np.nan
            df['p95_latency'] = np.nan

    # Ensure columns exist
    for c in ['total_fees_eth','median_latency','p95_latency','required_count','optional_count','delivered_rate']:
        if c not in df.columns:
            df[c] = 0

    df['total_fees_eth'] = pd.to_numeric(df['total_fees_eth'], errors='coerce').fillna(0.0)
    df['median_latency'] = pd.to_numeric(df['median_latency'], errors='coerce')
    df['p95_latency'] = pd.to_numeric(df['p95_latency'], errors='coerce').fillna(0.0)
    df['required_count'] = pd.to_numeric(df['required_count'], errors='coerce').fillna(0).astype(int)
    df['optional_count'] = pd.to_numeric(df['optional_count'], errors='coerce').fillna(0).astype(int)
    df['delivered_rate'] = pd.to_numeric(df['delivered_rate'], errors='coerce').fillna(0.0)

    return df

def dvn_color(name):
    # deterministic two-color mapping
    if name in STACK_BLUE:
        return '#1f77b4'  # matplotlib default blue
    if name in STACK_YELLOW:
        return '#ffbf00'  # warm yellow (distinct)
    return '#9e9e9e'      # neutral gray for others

def draw_bubble_chart(df):
    df_plot = df[(df['total_fees_eth'] > 0) | (df['median_latency'].notna()) | (df['required_count']>0) | (df['optional_count']>0)].copy()
    scatter_df = df_plot[df_plot['median_latency'].notna()].copy()

    print("Bubble chart: total DVNs:", len(df_plot), "with median_latency:", len(scatter_df))

    if scatter_df.empty:
        print("No numeric median_latency available for plotting bubble chart. Skipping.")
        return

    plt.figure(figsize=(12,8))
    ax = plt.gca()

    # compute colors per DVN from name membership
    colors = [dvn_color(n) for n in scatter_df['DVN_NAME'].tolist()]
    size_factor = 120
    sizes_vis = (scatter_df['required_count'].clip(lower=1) * size_factor).astype(float)

    sc = ax.scatter(
        scatter_df['median_latency'],
        scatter_df['total_fees_eth'],
        s=sizes_vis,
        c=colors,
        alpha=0.92,
        edgecolors='k',
        linewidths=0.5,
        zorder=2
    )

    # grouping nearby labels so they don't overlap
    xs = scatter_df['median_latency'].to_numpy()
    ys = scatter_df['total_fees_eth'].to_numpy()
    names = scatter_df['DVN_NAME'].tolist()

    xmin, xmax = xs.min(), xs.max()
    ymin, ymax = ys.min(), ys.max()
    def norm_x(v): return 0.0 if xmax==xmin else (v - xmin) / (xmax - xmin)
    def norm_y(v): return 0.0 if ymax==ymin else (v - ymin) / (ymax - ymin)
    coords = [(norm_x(x), norm_y(y)) for x,y in zip(xs,ys)]

    assigned = [False] * len(coords)
    groups = []
    threshold = 0.05
    for i,(xi,yi) in enumerate(coords):
        if assigned[i]:
            continue
        grp = [i]
        assigned[i] = True
        for j,(xj,yj) in enumerate(coords):
            if assigned[j]:
                continue
            if ((xi-xj)**2 + (yi-yj)**2)**0.5 < threshold:
                grp.append(j)
                assigned[j] = True
        groups.append(grp)

    always_label = set(['P2P','Deutsche Telekom','Canary'])
    fee_thresh = scatter_df['total_fees_eth'].quantile(0.6) if len(scatter_df)>0 else 0
    lat_thresh = scatter_df['median_latency'].quantile(0.6) if len(scatter_df)>0 else 0

    for grp in groups:
        names_grp = [names[k] for k in grp]
        x_pos = float(xs[grp].mean())
        y_pos = float(ys[grp].mean())
        show = any(n in always_label for n in names_grp) or len(names_grp) > 1 or (x_pos >= lat_thresh) or (y_pos >= fee_thresh)
        if not show:
            continue
        if len(names_grp) == 1:
            label = names_grp[0]
        else:
            label = "[ " + ",\n".join(names_grp) + " ]"
        x_offset = (xmax - xmin) * 0.01 if xmax>xmin else 0.5
        ax.text(x_pos + x_offset, y_pos, label, fontsize=9, va='center', zorder=3)

    # format axes
    ax.set_xlabel("Median Latency (s)")
    ax.set_ylabel("Total Fees (ETH)")
    ax.set_title("DVN: Median Latency vs Total Fees (bubble size = required_count)")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.6f'))
    ax.xaxis.set_major_formatter(mticker.FormatStrFormatter('%.1f'))
    ax.grid(axis='y', linestyle=':', alpha=0.6)

    # add tiny legend explaining color mapping
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0],[0], marker='o', color='w', label='DT stack: Deutsche/Canary/P2P', markerfacecolor='#1f77b4', markersize=8, markeredgecolor='k'),
        Line2D([0],[0], marker='o', color='w', label='LZ stack: LayerZero/Google', markerfacecolor='#ffbf00', markersize=8, markeredgecolor='k'),
        Line2D([0],[0], marker='o', color='w', label='Other DVNs', markerfacecolor='#9e9e9e', markersize=8, markeredgecolor='k'),
    ]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=9)

    plt.tight_layout()
    plt.savefig(OUT1, dpi=300)
    plt.close()
    print("Saved:", OUT1)

def draw_stacked_bar(df):
    df_bar = df.sort_values("required_count", ascending=False).head(20)
    if df_bar.empty:
        print("No data for stacked bar. Skipping.")
        return

    x = np.arange(len(df_bar))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 5))

    ax.bar(x, df_bar["required_count"], label="Required", color="#2b83ba")
    ax.bar(x, df_bar["optional_count"], bottom=df_bar["required_count"], label="Optional", color="#fdae61")

    ax.set_xticks(x)
    ax.set_xticklabels(df_bar["DVN_NAME"], rotation=45, ha="right", fontsize=9)
    ax.set_ylabel("Message Count")
    ax.set_title("DVN Roles Count (Required vs Optional) – Top 20 by Required Count")
    ax.legend()
    plt.tight_layout()

    plt.savefig(OUT2, dpi=300)
    plt.close()
    print("✅ Saved:", OUT2)




def main():
    df = load_dvn_table()
    draw_bubble_chart(df)
    draw_stacked_bar(df)

if __name__ == "__main__":
    main()
