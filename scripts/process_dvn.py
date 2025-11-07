#!/usr/bin/env python3
# process_dvn.py
import sys
import re
import ast
import json
import math
import pandas as pd
from datetime import datetime

if len(sys.argv) < 2:
    print("Usage: python3 process_dvn.py <input_csv>")
    sys.exit(1)

input_csv = sys.argv[1]
out_prefix = "dvn_processed"

pd.set_option('display.max_columns', 200)

def find_col(df, names):
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in cols:
            return cols[n.lower()]
    return None

def parse_array_field(s):
    """Parse a Flipside-style array field robustly.
    Accepts: JSON array string, or comma-separated values without brackets"""
    if pd.isna(s):
        return []
    s = str(s).strip()
    # try JSON / Python literal first
    try:
        if (s.startswith('[') and s.endswith(']')) or s.startswith('("') or s.startswith("['"):
            val = ast.literal_eval(s)
            return list(val) if val is not None else []
    except Exception:
        pass
    # if it's a simple comma separated list, split on commas
    if ',' in s:
        parts = [p.strip() for p in s.split(',') if p.strip()!='']
        return parts
    # single value
    return [s] if s != '' else []

def parse_int_safe(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    # remove any non-digit except minus
    s = re.sub(r'[^\d\-]', '', s)
    if s == '' or s == '-' :
        return None
    try:
        return int(s)
    except:
        try:
            return int(float(s))
        except:
            return None

def parse_datetime_safe(x):
    if pd.isna(x):
        return pd.NaT
    s = str(x).strip()
    # try common formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            pass
    # fallback: pandas to_datetime
    try:
        return pd.to_datetime(s, errors='coerce')
    except:
        return pd.NaT

print("Loading CSV:", input_csv)
df = pd.read_csv(input_csv, dtype=str, keep_default_na=False, na_values=['', 'NA', 'N/A', 'None'])

# locate important columns (case-insensitive)
col_map = {
    'source_tx': find_col(df, ['SOURCETXHASH','source_tx_hash','source_tx']),
    'source_ts': find_col(df, ['SOURCETIMESTAMP','source_timestamp','source_ts']),
    'dest_ts': find_col(df, ['DESTINATIONDELIVEREDTIMESTAMP','dest_timestamp','destinationdeliveredtimestamp']),
    'dvn_fees': find_col(df, ['DVN_FEES_ARRAY','DVN_FEES_ARRAY','dvn_fees','dvn_fees_array','dvN_fees']),
    'req_dvns': find_col(df, ['REQUIREDDVNS','required_dvns','requiredDvns']),
    'opt_dvns': find_col(df, ['OPTIONALDVNS','optional_dvns','optionalDvns']),
    'latency': find_col(df, ['LATENCYTODELIVERY_SECONDS','latencytodelivery seconds','latencytodelivery_seconds','latency']),
    'message_status': find_col(df, ['MESSAGESTATUS','message_status','status']),
}
print("Detected column mapping:", col_map)

# Normalize: fill missing mapped columns with None
for k,v in col_map.items():
    if v is None:
        print(f"Warning: could not find column for '{k}' -- some outputs may be limited.")

# parse arrays and numeric fields
def build_parsed_row(row):
    parsed = {}
    parsed['source_tx'] = row.get(col_map['source_tx']) if col_map['source_tx'] else None
    parsed['source_timestamp_raw'] = row.get(col_map['source_ts']) if col_map['source_ts'] else None
    parsed['dest_timestamp_raw'] = row.get(col_map['dest_ts']) if col_map['dest_ts'] else None
    parsed['source_timestamp'] = parse_datetime_safe(parsed['source_timestamp_raw'])
    parsed['dest_timestamp'] = parse_datetime_safe(parsed['dest_timestamp_raw'])
    parsed['latency_seconds'] = parse_int_safe(row.get(col_map['latency'])) if col_map['latency'] else None
    parsed['message_status'] = row.get(col_map['message_status']) if col_map['message_status'] else None

    # arrays (addresses)
    req = parse_array_field(row.get(col_map['req_dvns'])) if col_map['req_dvns'] else []
    opt = parse_array_field(row.get(col_map['opt_dvns'])) if col_map['opt_dvns'] else []
    parsed['required_dvns'] = req
    parsed['optional_dvns'] = opt
    parsed['all_dvns'] = req + opt

    # dvn fees array
    fees_raw = row.get(col_map['dvn_fees']) if col_map['dvn_fees'] else None
    fees_list = parse_array_field(fees_raw)
    fees_ints = [parse_int_safe(f) for f in fees_list]
    parsed['dvn_fees_array'] = fees_ints

    # keep original row for other columns
    parsed['__orig'] = row
    return parsed

print("Parsing rows and building expanded per-DVN rows...")
parsed_rows = []
for _, r in df.iterrows():
    p = build_parsed_row(r)
    dvns = p['all_dvns']
    fees = p['dvn_fees_array']
    # if lengths mismatch, allow mapping of min(len)
    n = max(len(dvns), len(fees))
    if n == 0:
        # no dvn info; skip
        continue
    for i in range(n):
        dvn_addr = dvns[i] if i < len(dvns) else None
        fee = fees[i] if i < len(fees) else None
        is_required = (i < len(p['required_dvns']))
        parsed_rows.append({
            'source_tx': p['source_tx'],
            'source_timestamp': p['source_timestamp'],
            'dest_timestamp': p['dest_timestamp'],
            'latency_seconds': p['latency_seconds'],
            'message_status': p['message_status'],
            'dvn_addr': dvn_addr,
            'dvn_fee': fee,
            'is_required': is_required
        })

expanded_df = pd.DataFrame(parsed_rows)
print("Expanded rows:", len(expanded_df))

# coerce types
expanded_df['dvn_fee'] = expanded_df['dvn_fee'].apply(lambda x: int(x) if (x is not None and not (isinstance(x, float) and math.isnan(x))) else None)
expanded_df['latency_seconds'] = expanded_df['latency_seconds'].apply(lambda x: int(x) if x is not None else None)

# basic KPIs per DVN operator
agg = expanded_df.groupby('dvn_addr').agg(
    messages=('source_tx','nunique'),
    rows=('dvn_addr','size'),
    total_fees=('dvn_fee', lambda s: sum([int(x) for x in s if x is not None])),
    avg_fee=('dvn_fee', lambda s: (sum([int(x) for x in s if x is not None]) / len([x for x in s if x is not None])) if len([x for x in s if x is not None])>0 else None),
).reset_index()

# latency stats (only delivered)
delivered = expanded_df[expanded_df['message_status'].str.upper()=='DELIVERED'] if 'message_status' in expanded_df.columns else expanded_df
latency_stats = delivered.groupby('dvn_addr')['latency_seconds'].agg(['count','median', lambda s: s.dropna().quantile(0.95)]).reset_index()
latency_stats.columns = ['dvn_addr','delivered_count','median_latency','p95_latency']

# delivered rate per dvn (unique messages delivered / unique messages seen)
msg_status = expanded_df.groupby(['dvn_addr']).apply(
    lambda g: pd.Series({
        'unique_messages': g['source_tx'].nunique(),
        'delivered_messages': g[g['message_status'].str.upper()=='DELIVERED']['source_tx'].nunique() if 'message_status' in g else 0
    })
).reset_index()
msg_status['delivered_rate'] = msg_status.apply(lambda r: r['delivered_messages']/r['unique_messages'] if r['unique_messages']>0 else None, axis=1)

# merge tables
kpi = agg.merge(latency_stats, on='dvn_addr', how='left').merge(msg_status[['dvn_addr','delivered_rate']], on='dvn_addr', how='left')

# Save outputs
expanded_df.to_csv(f"{out_prefix}_per_dvn_rows.csv", index=False)
kpi.to_csv(f"{out_prefix}_kpi_by_dvn.csv", index=False)

print("\nSaved:")
print(f" - expanded per-DVN rows -> {out_prefix}_per_dvn_rows.csv")
print(f" - KPI summary per DVN -> {out_prefix}_kpi_by_dvn.csv")

# print top 10 by messages
print("\nTop 10 DVNs by message rows:")
print(kpi.sort_values('rows', ascending=False).head(10).to_string(index=False))

# quick pre/post outage comparison (if source_timestamp present)
try:
    expanded_df['source_timestamp'] = pd.to_datetime(expanded_df['source_timestamp'])
    outage_start = pd.to_datetime("2025-10-19")
    outage_end = pd.to_datetime("2025-10-21")
    before = expanded_df[expanded_df['source_timestamp'] < outage_start]
    during = expanded_df[(expanded_df['source_timestamp'] >= outage_start) & (expanded_df['source_timestamp'] <= outage_end)]
    print("\nCounts around outage period:")
    print("Before period rows:", len(before), "During rows:", len(during))
    # example: per-DVN delivered_rate during outage (coarse)
    during_kpi = during.groupby('dvn_addr').apply(lambda g: pd.Series({
        'unique_messages': g['source_tx'].nunique(),
        'delivered_messages': g[g['message_status'].str.upper()=='DELIVERED']['source_tx'].nunique() if 'message_status' in g else 0
    })).reset_index()
    during_kpi['delivered_rate'] = during_kpi.apply(lambda r: r['delivered_messages']/r['unique_messages'] if r['unique_messages']>0 else None, axis=1)
    print("\nDelivered rate per DVN during outage (sample):")
    print(during_kpi.sort_values('unique_messages', ascending=False).head(10).to_string(index=False))
except Exception as e:
    print("Could not compute outage analysis:", e)

print("\nDone.")
