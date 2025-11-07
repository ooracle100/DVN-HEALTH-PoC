#!/usr/bin/env python3
# merge_expand_dvns_v2.py
import sys, ast, re
from decimal import Decimal, getcontext
from pathlib import Path
import pandas as pd
import numpy as np

getcontext().prec = 36

if len(sys.argv) < 3:
    print("Usage: python3 merge_expand_dvns_v2.py <dt_clean.csv> <dvnFeesMapped.csv>")
    sys.exit(1)

DT_PATH = Path(sys.argv[1])
FEES_PATH = Path(sys.argv[2])
OUT_PREFIX = "dvn_enriched_v2"

assert DT_PATH.exists(), f"{DT_PATH} not found"
assert FEES_PATH.exists(), f"{FEES_PATH} not found"

def find_col(df, candidates):
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None

def parse_array_field(s):
    """Handle formats like:
       - "[ 0xabc;0xdef ]"
       - "['0xabc','0xdef']"
       - "0xabc,0xdef"
    """
    if s is None:
        return []
    text = str(s).strip()
    if text == "" or text.lower() in ("nan","none"):
        return []
    # remove surrounding brackets
    text2 = re.sub(r'^[\[\(]\s*','', re.sub(r'\s*[\]\)]$','', text))
    # choose separator ; if present and likely used
    sep = ';' if ';' in text2 and text2.count(';') >= text2.count(',') else ','
    parts = [p.strip().strip("'\"") for p in re.split(r'[,;]+', text2) if p.strip()]
    return parts

def safe_parse_list_of_tuples(s):
    """Parse a string that looks like: [('Name','123'), ('Other','456')] or variants.
       Returns list of (name, fee_str)."""
    if s is None:
        return []
    text = str(s).strip()
    if text == "" or text.lower() in ("nan","none"):
        return []
    # try ast.literal_eval first
    try:
        val = ast.literal_eval(text)
        out=[]
        if isinstance(val, (list, tuple)):
            for item in val:
                if isinstance(item, (list, tuple)) and len(item)>=2:
                    out.append((str(item[0]).strip(), str(item[1]).strip()))
        return out
    except Exception:
        pass
    # fallback regex
    pairs = re.findall(r"['\"]?([^,'\"\)\(]+?)['\"]?\s*[,;]\s*['\"]?([0-9]+)['\"]?", text)
    if pairs:
        return [(p[0].strip(), p[1].strip()) for p in pairs]
    # last fallback: try to split into tokens and pair
    token_sep = ';' if ';' in text and text.count(';')>text.count(',') else ','
    tokens = [t.strip().strip("'\"") for t in re.split(r'[{},;\[\]\(\)]+', text) if t.strip()]
    out=[]
    for i in range(0, len(tokens)-1, 2):
        out.append((tokens[i], tokens[i+1]))
    return out

def wei_to_eth_decimal_str(x):
    if x is None:
        return None
    try:
        xi = int(re.sub(r'[^\d\-]', '', str(x)))
        eth = Decimal(xi) / Decimal(10**18)
        # return Decimal as float-string with trimmed zeros
        s = format(eth.normalize(), 'f')
        return s
    except Exception:
        return None

# Load files
dt = pd.read_csv(DT_PATH, dtype=str, keep_default_na=False, na_values=["", "NA", "N/A"])
fees = pd.read_csv(FEES_PATH, dtype=str, keep_default_na=False, na_values=["", "NA", "N/A"])

# identify key cols
guid_dt = find_col(dt, ["GUID","guid"])
guid_fees = find_col(fees, ["GUID","guid"])
tx_col = find_col(dt, ["SOURCETXHASH","source_tx_hash","source_tx","source_txhash"])
lat_col = find_col(dt, ["LATENCYTODELIVERY_SECONDS","latencytodelivery_seconds","latency"])

req_addr_col = find_col(fees, ["requiredDVNs","required_dvns","requireddvns","requireddvns"])
opt_addr_col = find_col(fees, ["optionalDVNs","optional_dvns","optionaldvns","optionaldvns"])
fees_arr_col = find_col(fees, ["DVN_FEES_ARRAY","dvn_fees_array","dvn_fees","fees_array","dvnfeesarray"])
req_map_col = find_col(fees, ["RequiredDVN_Mapping","requireddvn_mapping","requiredDvnMapping","RequiredDVN_Mapping"])
opt_map_col = find_col(fees, ["OptionalDVN_Mapping","optionaldvn_mapping","optionalDvnMapping","OptionalDVN_Mapping"])

print("Columns found (dt):", guid_dt, tx_col, lat_col)
print("Columns found (fees):", guid_fees, req_addr_col, opt_addr_col, fees_arr_col, req_map_col, opt_map_col)

if not guid_dt or not guid_fees:
    print("GUID column missing in one of the files. Aborting.")
    sys.exit(1)

# merge
merged = dt.merge(fees, left_on=guid_dt, right_on=guid_fees, how='left', suffixes=("","_fees"))

# build per-dvn rows
# ----------------- START REPLACEMENT LOOP -----------------
rows = []
row_counter = 0
for idx, r in merged.iterrows():
    row_counter += 1
    guid = r.get(guid_dt)
    tx = r.get(tx_col) if tx_col else None
    latency = None
    if lat_col and str(r.get(lat_col)).strip() != "":
        try:
            latency = int(re.sub(r'[^\d\-]','', str(r.get(lat_col))))
        except:
            latency = None
    message_status = r.get(find_col(merged, ["MESSAGESTATUS","message_status","status"]))

    # parse address arrays (required + optional)
    req_addrs = parse_array_field(r.get(req_addr_col)) if req_addr_col else []
    opt_addrs = parse_array_field(r.get(opt_addr_col)) if opt_addr_col else []
    all_addrs = req_addrs + opt_addrs

    # parse fee array (raw wei list)
    fees_arr = parse_array_field(r.get(fees_arr_col)) if fees_arr_col else []

    # parse mapping tuples (name, fee) for required and optional (may not include addresses)
    req_map = safe_parse_list_of_tuples(r.get(req_map_col)) if req_map_col else []
    opt_map = safe_parse_list_of_tuples(r.get(opt_map_col)) if opt_map_col else []

    # Determine counts
    n_req = max(len(req_addrs), len(req_map))
    n_opt = max(len(opt_addrs), len(opt_map))
    # If fees_arr present, its ordering is required followed by optional; use as fallback
    # Build required rows
    for i in range(n_req):
        addr = req_addrs[i] if i < len(req_addrs) else None
        name = req_map[i][0] if i < len(req_map) else (addr if addr is not None else None)
        fee_wei = None
        # prefer fee from mapping if available
        if i < len(req_map):
            fee_wei = req_map[i][1]
        else:
            # fallback to fees array by same index
            if i < len(fees_arr):
                fee_wei = fees_arr[i]
        rows.append({
            'GUID': guid,
            'SOURCETXHASH': tx,
            'DVN_ADDR': addr,
            'DVN_NAME': name,
            'ROLE': 'required',
            'DVN_FEE_WEI': fee_wei,
            'LATENCY_SECONDS': latency,
            'MESSAGESTATUS': message_status,
            'SOURCEBLOCKNUMBER': r.get(find_col(merged, ['SOURCEBLOCKNUMBER','sourceblocknumber'])),
            'SOURCETIMESTAMP': r.get(find_col(merged, ['SOURCETIMESTAMP','sourcetimestamp'])),
            'DEST_CHAIN_NAME': r.get(find_col(merged, ['DEST_CHAIN_NAME','dest_chain_name','destchainname']))
        })

    # Build optional rows (index offset into fees_arr = len(req_addrs))
    for j in range(n_opt):
        addr = opt_addrs[j] if j < len(opt_addrs) else None
        name = opt_map[j][0] if j < len(opt_map) else (addr if addr is not None else None)
        fee_wei = None
        # fee index in fees_arr is req_count + j
        fee_index = j + (len(req_addrs) if len(req_addrs)>0 else len(req_map))
        if j < len(opt_map):
            fee_wei = opt_map[j][1]
        else:
            if fee_index < len(fees_arr):
                fee_wei = fees_arr[fee_index]
        rows.append({
            'GUID': guid,
            'SOURCETXHASH': tx,
            'DVN_ADDR': addr,
            'DVN_NAME': name,
            'ROLE': 'optional',
            'DVN_FEE_WEI': fee_wei,
            'LATENCY_SECONDS': latency,
            'MESSAGESTATUS': message_status,
            'SOURCEBLOCKNUMBER': r.get(find_col(merged, ['SOURCEBLOCKNUMBER','sourceblocknumber'])),
            'SOURCETIMESTAMP': r.get(find_col(merged, ['SOURCETIMESTAMP','sourcetimestamp'])),
            'DEST_CHAIN_NAME': r.get(find_col(merged, ['DEST_CHAIN_NAME','dest_chain_name','destchainname']))
        })

    # small progress debug every 200 rows
    if row_counter % 200 == 0:
        print(f"Processed {row_counter} merged rows...")

# ----------------- END REPLACEMENT LOOP -----------------

per = pd.DataFrame(rows)

# Debug: report row/column counts so we can spot empty results quickly
print(f"DEBUG: per-dvn rows created = {len(per)}")
print("DEBUG: per columns =", per.columns.tolist())

# Ensure DVN_FEE_WEI column exists (create empty if not) to avoid KeyError on very sparse datasets
if 'DVN_FEE_WEI' not in per.columns:
    print("WARNING: 'DVN_FEE_WEI' column missing; creating empty column and continuing.")
    per['DVN_FEE_WEI'] = None

# Safely create cleaned numeric fee column (strip non-digits)
import re
per['DVN_FEE_WEI_CLEAN'] = per['DVN_FEE_WEI'].apply(
    lambda x: None if x is None or str(x).strip()=='' else int(re.sub(r'[^\d\-]', '', str(x)))
)

# Convert WEI -> ETH (Decimal used earlier in script)
from decimal import Decimal
def wei_to_eth_str_safe(x):
    try:
        if x is None:
            return None
        eth = (Decimal(int(x)) / Decimal(10**18))
        return format(eth.normalize(), 'f')
    except Exception:
        return None

per['DVN_FEE_ETH'] = per['DVN_FEE_WEI_CLEAN'].apply(lambda x: wei_to_eth_str_safe(x))

# separate required vs optional fee columns for quick pivoting/aggregation
if 'ROLE' not in per.columns:
    per['ROLE'] = None

per['DVN_FEE_IF_REQUIRED_ETH'] = per.apply(lambda r: r['DVN_FEE_ETH'] if str(r.get('ROLE')).lower()=='required' else None, axis=1)
per['DVN_FEE_IF_OPTIONAL_ETH'] = per.apply(lambda r: r['DVN_FEE_ETH'] if str(r.get('ROLE')).lower()=='optional' else None, axis=1)

# numeric helper columns
per['DVN_FEE_ETH_NUM'] = pd.to_numeric(per['DVN_FEE_ETH'], errors='coerce')
per['DVN_FEE_IF_REQUIRED_ETH_NUM'] = pd.to_numeric(per['DVN_FEE_IF_REQUIRED_ETH'], errors='coerce')
per['DVN_FEE_IF_OPTIONAL_ETH_NUM'] = pd.to_numeric(per['DVN_FEE_IF_OPTIONAL_ETH'], errors='coerce')
per['LATENCY_SECONDS'] = pd.to_numeric(per.get('LATENCY_SECONDS', None), errors='coerce')

# Final debug checkpoint
print(f"DEBUG after conversions: rows={len(per)}, columns={per.columns.tolist()}")
print("Sample rows (first 5):")
print(per.head(5).to_string(index=False))

# Save canonical per-DVN rows
per.to_csv(f"{OUT_PREFIX}_per_dvn_rows.csv", index=False)
merged.to_csv(f"{OUT_PREFIX}_merged_dt_enriched.csv", index=False)

# KPI aggregation per DVN_NAME
agg = per.groupby('DVN_NAME').agg(
    unique_messages=('GUID','nunique'),
    rows=('GUID','count'),
    total_fees_eth=('DVN_FEE_ETH_NUM', lambda s: float(s.dropna().sum()) if s.dropna().size>0 else 0.0),
    total_required_fees_eth=('DVN_FEE_IF_REQUIRED_ETH_NUM', lambda s: float(s.dropna().sum()) if s.dropna().size>0 else 0.0),
    total_optional_fees_eth=('DVN_FEE_IF_OPTIONAL_ETH_NUM', lambda s: float(s.dropna().sum()) if s.dropna().size>0 else 0.0),
    avg_fee_required=('DVN_FEE_IF_REQUIRED_ETH_NUM', lambda s: float(s.dropna().mean()) if s.dropna().size>0 else None),
    avg_fee_optional=('DVN_FEE_IF_OPTIONAL_ETH_NUM', lambda s: float(s.dropna().mean()) if s.dropna().size>0 else None),
    median_latency=('LATENCY_SECONDS', lambda s: float(s.dropna().median()) if s.dropna().size>0 else None),
    p95_latency=('LATENCY_SECONDS', lambda s: float(s.dropna().quantile(0.95)) if s.dropna().size>0 else None),
    delivered_messages=('MESSAGESTATUS', lambda s: int(s.dropna().apply(lambda x: 1 if str(x).upper()=='DELIVERED' else 0).sum()))
).reset_index()

# delivered rate per DVN
delivered_counts = per[per['MESSAGESTATUS'].astype(str).str.upper()=='DELIVERED'].groupby('DVN_NAME').agg(delivered_unique=('GUID','nunique')).reset_index()
msg_counts = per.groupby('DVN_NAME').agg(unique_messages=('GUID','nunique')).reset_index()
agg = agg.merge(delivered_counts, on='DVN_NAME', how='left').merge(msg_counts, on='DVN_NAME', how='left')
agg['delivered_rate'] = agg.apply(lambda r: float(r['delivered_unique']/r['unique_messages']) if r['unique_messages']>0 and not pd.isna(r['delivered_unique']) else None, axis=1)

agg.to_csv(f"{OUT_PREFIX}_kpi_by_dvn.csv", index=False)

print("Saved files:")
print(f" - {OUT_PREFIX}_per_dvn_rows.csv")
print(f" - {OUT_PREFIX}_merged_dt_enriched.csv")
print(f" - {OUT_PREFIX}_kpi_by_dvn.csv")
print("\nTop DVNs by total fees (ETH):")
print(agg.sort_values('total_fees_eth', ascending=False).head(20).to_string(index=False))
