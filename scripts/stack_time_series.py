# stack_time_series.py
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

IN = "expanded_per_dvn_joined.csv"
OUT_CSV = "stack_time_series_top.csv"
OUT_PNG = "stack_time_series_top.png"

df = pd.read_csv(IN, dtype=str, keep_default_na=False)
# clean timestamps
df['SOURCETIMESTAMP'] = df['SOURCETIMESTAMP'].astype(str).str.strip().str.replace(r'^\=+','',regex=True).str.replace(r'^[\"\']+|[\"\']+$','',regex=True)
df['SOURCETIMESTAMP'] = pd.to_datetime(df['SOURCETIMESTAMP'], errors='coerce', utc=True)
df['LATENCY_S'] = pd.to_numeric(df.get('LATENCYTODELIVERY_SECONDS','').astype(str).str.replace(r'[^0-9.]','',regex=True), errors='coerce')
df['ROLE'] = df['ROLE'].astype(str).str.lower().fillna('')
df['day'] = df['SOURCETIMESTAMP'].dt.date

# build required stack per GUID
req = (df[df['ROLE']=='required']
       .groupby('GUID')['DVN_NAME']
       .apply(lambda s: ' + '.join(sorted(set([x for x in s if x and x.lower()!='nan']))))
       .reset_index(name='Required_Stack'))

tx_latency = df[['GUID','day','LATENCY_S']].dropna(subset=['LATENCY_S']).drop_duplicates('GUID',keep='first')
txs = req.merge(tx_latency, on='GUID', how='left').dropna(subset=['LATENCY_S'])

# pick top stacks by transaction count
top_stacks = txs['Required_Stack'].value_counts().head(6).index.tolist()
ts = txs[txs['Required_Stack'].isin(top_stacks)].groupby(['day','Required_Stack'])['LATENCY_S'].median().unstack(fill_value=np.nan)

# save CSV
ts.reset_index().to_csv(OUT_CSV, index=False)
print("Saved:", OUT_CSV)

# plot time-series (each stack its own line)
plt.figure(figsize=(12,6))
for col in ts.columns:
    plt.plot(ts.index, ts[col], marker='o', label=col)
plt.xticks(rotation=45)
plt.xlabel("Day")
plt.ylabel("Median Latency (s)")
plt.title("Daily median latency — top required DVN stacks")
plt.legend(fontsize=8, loc='upper left')
plt.tight_layout()
plt.savefig(OUT_PNG, dpi=300)
plt.close()
print("Saved:", OUT_PNG)
