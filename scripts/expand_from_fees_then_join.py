#!/usr/bin/env python3
# expand_from_fees_then_join.py
import sys, re, ast
from decimal import Decimal, getcontext
from pathlib import Path
import pandas as pd
import numpy as np

getcontext().prec = 36

if len(sys.argv) < 3:
    print("Usage: python expand_from_fees_then_join.py <dvnFeesMapped.csv> <dt_clean.csv>")
    sys.exit(1)

FEES_CSV = Path(sys.argv[1])
DT_CSV = Path(sys.argv[2])
OUT_PREFIX = "expanded"

assert FEES_CSV.exists(), f"{FEES_CSV} not found"
assert DT_CSV.exists(), f"{DT_CSV} not found"

pd.set_option('display.max_colwidth', 400)

def parse_array_field(s):
    if s is None: return []
    text = str(s).strip()
    if text == "" or text.lower() in ("nan","none"): return []
    # remove surrounding [] and whitespace; choose ';' if appears to be used
    t = re.sub(r'^[\[\(]\s*','', re.sub(r'\s*[\]\)]$','', text))
    # prefer semicolon separator when present
    sep = ';' if ';' in t and t.count(';') >= t.count(',') else ','
    parts = [p.strip().strip("'\"") for p in re.split(r'[;,]+', t) if p.strip()]
    return parts

def safe_parse_list_of_tuples(s):
    if s is None: return []
    text = str(s).strip()
    if text == "" or text.lower() in ("nan","none"): return []
    try:
        val = ast.literal_eval(text)
        out=[]
        if isinstance(val, (list, tuple)):
            for item in val:
                if isinstance(item, (list, tuple)) and len(item)>=2:
                    out.append((str(item[0]).strip(), str(item[1]).strip()))
        return out
    except Exception:
        # regex fallback: ('Name','123')
        pairs = re.findall(r"['\"]?([^,'\"\)\(]+?)['\"]?\s*[,;]\s*['\"]?([0-9]+)['\"]?", text)
        return [(p[0].strip(), p[1].strip()) for p in pairs]

def wei_to_eth_decimal_str(x):
    if x is None or str(x).strip()=="":
        return None
    try:
        xi = int(re.sub(r'[^\d\-]', '', str(x)))
        eth = Decimal(xi) / Decimal(10**18)
        return format(eth.normalize(), 'f')
    except Exception:
        return None

# load fees file
fees = pd.read_csv(FEES_CSV, dtype=str, keep_default_na=False, na_values=['','NA','N/A'])
# load dt
dt = pd.read_csv(DT_CSV, dtype=str, keep_default_na=False, na_values=['','NA','N/A'])

# normalize GUIDs (strip ="" wrappers if any, lowercase)
def norm_guid(s):
    if s is None: return None
    s = str(s).strip()
    if s.startswith('='):
        s = s[1:].strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1]
    return s.lower()

fees = fees.assign(GUID = fees['GUID'].astype(str).apply(norm_guid))
dt = dt.assign(GUID = dt['GUID'].astype(str).apply(norm_guid))

rows = []
for i, r in fees.iterrows():
    guid = r['GUID']
    # parse arrays and maps
    req_addrs = parse_array_field(r.get('requiredDVNs'))
    opt_addrs = parse_array_field(r.get('optionalDVNs'))
    fees_arr = parse_array_field(r.get('DVN_FEES_ARRAY'))
    req_map = safe_parse_list_of_tuples(r.get('RequiredDVN_Mapping'))
    opt_map = safe_parse_list_of_tuples(r.get('OptionalDVN_Mapping'))
    # required: pair by index; name from req_map if available else addr; fee from map or fees_arr
    n_req = max(len(req_addrs), len(req_map))
    n_opt = max(len(opt_addrs), len(opt_map))
    # required
    for j in range(n_req):
        addr = req_addrs[j] if j < len(req_addrs) else None
        name = req_map[j][0] if j < len(req_map) else (addr if addr is not None else None)
        fee_wei = None
        if j < len(req_map):
            fee_wei = req_map[j][1]
        else:
            if j < len(fees_arr):
                fee_wei = fees_arr[j]
        rows.append({
            'GUID': guid, 'DVN_ADDR': addr, 'DVN_NAME': name, 'ROLE': 'required', 'DVN_FEE_WEI': fee_wei
        })
    # optional
    for j in range(n_opt):
        addr = opt_addrs[j] if j < len(opt_addrs) else None
        name = opt_map[j][0] if j < len(opt_map) else (addr if addr is not None else None)
        fee_index = j + max(len(req_addrs), len(req_map))
        fee_wei = None
        if j < len(opt_map):
            fee_wei = opt_map[j][1]
        else:
            if fee_index < len(fees_arr):
                fee_wei = fees_arr[fee_index]
        rows.append({
            'GUID': guid, 'DVN_ADDR': addr, 'DVN_NAME': name, 'ROLE': 'optional', 'DVN_FEE_WEI': fee_wei
        })

expanded = pd.DataFrame(rows)
print("Expanded rows:", len(expanded))
# convert fees
expanded['DVN_FEE_WEI_CLEAN'] = expanded['DVN_FEE_WEI'].apply(lambda x: None if x is None or str(x).strip()=='' else int(re.sub(r'[^\d\-]','', str(x))))
expanded['DVN_FEE_ETH'] = expanded['DVN_FEE_WEI_CLEAN'].apply(lambda x: wei_to_eth_decimal_str(x))
# separate required/optional fee columns
expanded['DVN_FEE_IF_REQUIRED_ETH'] = expanded.apply(lambda r: r['DVN_FEE_ETH'] if r['ROLE']=='required' else None, axis=1)
expanded['DVN_FEE_IF_OPTIONAL_ETH'] = expanded.apply(lambda r: r['DVN_FEE_ETH'] if r['ROLE']=='optional' else None, axis=1)
# numeric
expanded['DVN_FEE_ETH_NUM'] = pd.to_numeric(expanded['DVN_FEE_ETH'], errors='coerce')
expanded['DVN_FEE_IF_REQUIRED_ETH_NUM'] = pd.to_numeric(expanded['DVN_FEE_IF_REQUIRED_ETH'], errors='coerce')
expanded['DVN_FEE_IF_OPTIONAL_ETH_NUM'] = pd.to_numeric(expanded['DVN_FEE_IF_OPTIONAL_ETH'], errors='coerce')

# join with dt on GUID to pick up latency/tx/timestamps etc
joined = expanded.merge(dt, on='GUID', how='left', suffixes=('','_dt'))

# Save files
expanded.to_csv(f"{OUT_PREFIX}_per_dvn.csv", index=False)
joined.to_csv(f"{OUT_PREFIX}_per_dvn_joined.csv", index=False)

# KPI aggregation by DVN_NAME
joined['LATENCY_SECONDS'] = pd.to_numeric(joined.get('LATENCYTODELIVERY_SECONDS', joined.get('LATENCY_SECONDS')), errors='coerce')
kpi = joined.groupby('DVN_NAME').agg(
    unique_messages=('GUID','nunique'),
    rows=('GUID','count'),
    total_fees_eth=('DVN_FEE_ETH_NUM', lambda s: float(s.dropna().sum()) if s.dropna().size>0 else 0.0),
    total_required_fees_eth=('DVN_FEE_IF_REQUIRED_ETH_NUM', lambda s: float(s.dropna().sum()) if s.dropna().size>0 else 0.0),
    total_optional_fees_eth=('DVN_FEE_IF_OPTIONAL_ETH_NUM', lambda s: float(s.dropna().sum()) if s.dropna().size>0 else 0.0),
    median_latency=('LATENCY_SECONDS', lambda s: float(s.dropna().median()) if s.dropna().size>0 else None),
    p95_latency=('LATENCY_SECONDS', lambda s: float(s.dropna().quantile(0.95)) if s.dropna().size>0 else None),
    delivered_messages=('MESSAGESTATUS', lambda s: int(s.dropna().apply(lambda x: 1 if str(x).upper()=='DELIVERED' else 0).sum()))
).reset_index()

kpi.to_csv(f"{OUT_PREFIX}_kpi_by_dvn.csv", index=False)

print("Saved:", f"{OUT_PREFIX}_per_dvn.csv", f"{OUT_PREFIX}_per_dvn_joined.csv", f"{OUT_PREFIX}_kpi_by_dvn.csv")
print("Top DVNs by total_fees_eth:")
print(kpi.sort_values('total_fees_eth', ascending=False).head(20).to_string(index=False))
