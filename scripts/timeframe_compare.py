# timeframe_compare.py
import pandas as pd
import numpy as np
from datetime import datetime

df = pd.read_csv("expanded_per_dvn_joined.csv", dtype=str, keep_default_na=False)
# clean then parse timestamps (ensure UTC tz-aware)
df['SOURCETIMESTAMP'] = df['SOURCETIMESTAMP'].astype(str).str.strip().str.replace(r'^\=+','',regex=True).str.replace(r'^[\"\']+|[\"\']+$','',regex=True)
df['SOURCETIMESTAMP'] = pd.to_datetime(df['SOURCETIMESTAMP'], errors='coerce', utc=True)

# normalize role and latency
df['ROLE'] = df['ROLE'].astype(str).str.lower().fillna('')

df['LATENCY_S'] = pd.to_numeric(df['LATENCYTODELIVERY_SECONDS'].astype(str).str.replace(r'[^0-9\.]','',regex=True), errors='coerce')

# time windows (adjust dates to exact outage period you want)
start = pd.Timestamp("2025-09-26", tz="UTC")
outage_start = pd.Timestamp("2025-10-19", tz="UTC")
outage_end = pd.Timestamp("2025-10-21", tz="UTC")
end = pd.Timestamp("2025-10-25", tz="UTC")


windows = {
  'before': (start, outage_start - pd.Timedelta(days=1)),
  'during': (outage_start, outage_end),
  'after': (outage_end + pd.Timedelta(days=1), end)
}


def compute_for_window(dfw):
    # Build required stack per GUID
    req = (dfw[dfw['ROLE'].str.lower()=='required']
           .groupby('GUID')['DVN_NAME']
           .apply(lambda s: ' + '.join(sorted(set([x for x in s if x and x!='']))))
           .reset_index(name='Required_Stack'))

    # attach latency (first available numeric latency per GUID)
    tx_latency = (dfw[['GUID','LATENCY_S']].dropna(subset=['LATENCY_S']).drop_duplicates('GUID',keep='first'))

    # merge and keep only GUIDs that have numeric latency
    txs = req.merge(tx_latency, on='GUID', how='left').dropna(subset=['LATENCY_S'])

    # if nothing left, return empty shaped DataFrames (prevents KeyErrors)
    if txs.empty:
        stack_cols = ['Required_Stack','transactions','median_latency','p95_latency']
        dvn_cols = ['DVN_NAME','stacks_involved','total_transactions','avg_median_latency','avg_p95_latency']
        return pd.DataFrame(columns=stack_cols), pd.DataFrame(columns=dvn_cols)

    # compute stack-level stats
    stack = (txs.groupby('Required_Stack')['LATENCY_S']
             .agg(transactions='count', median_latency='median', p95_latency=lambda s: float(np.percentile(s,95)))
             .reset_index().sort_values('transactions',ascending=False))

    # expand stacks into per-DVN rows to compute DVN-level averages
    rows=[]
    for _,r in stack.iterrows():
        names=[n.strip() for n in r['Required_Stack'].split('+')]
        for n in names:
            if n:
                rows.append({'DVN_NAME':n,'stack':r['Required_Stack'],'transactions':int(r['transactions']),
                             'median_latency':float(r['median_latency']),'p95_latency':float(r['p95_latency'])})
    if len(rows)==0:
        dvn = pd.DataFrame(columns=['DVN_NAME','stacks_involved','total_transactions','avg_median_latency','avg_p95_latency'])
    else:
        dvn = (pd.DataFrame(rows).groupby('DVN_NAME')
               .agg(stacks_involved=('stack','nunique'), total_transactions=('transactions','sum'),
                    avg_median_latency=('median_latency','mean'), avg_p95_latency=('p95_latency','mean'))
               .reset_index())
    return stack, dvn



results = {}
for name,(s,e) in windows.items():
    mask = (df['SOURCETIMESTAMP'] >= pd.to_datetime(s)) & (df['SOURCETIMESTAMP'] <= pd.to_datetime(e))
    stacked, dvn = compute_for_window(df.loc[mask])
    results[name] = (stacked, dvn)
    stacked.to_csv(f"stack_{name}.csv", index=False)
    dvn.to_csv(f"dvn_{name}.csv", index=False)
    print(f"{name}: stacks={len(stacked)}, dvns={len(dvn)}")
