import pandas as pd
from pathlib import Path
import numpy as np
import numpy as np
import re

input_file = "expanded_per_dvn_joined.csv"
output_file = "kpi_by_dvn_latency_added.csv"

# Load the joined per-DVN dataset
df = pd.read_csv(input_file)
print(f"Loaded {len(df)} rows from {input_file}")

# Normalize latency column
if 'LATENCYTODELIVERY_SECONDS' in df.columns:
    df['LATENCY_SECONDS_NUM'] = (
        df['LATENCYTODELIVERY_SECONDS']
        .astype(str)
        .str.replace(r'[^0-9\.]', '', regex=True)   # strip "N/A", "None", etc.
        .replace('', np.nan)
        .astype(float)
    )
else:
    print("WARNING: LATENCYTODELIVERY_SECONDS column not found; creating empty column.")
    df['LATENCY_SECONDS_NUM'] = np.nan
# Normalize delivery boolean
if 'DELIVERED_BOOL' in df.columns:
    df['DELIVERED_BOOL_CLEAN'] = df['DELIVERED_BOOL'].astype(str).str.upper().map({'TRUE': True, 'FALSE': False})
else:
    df['DELIVERED_BOOL_CLEAN'] = False

# Compute per-DVN aggregated metrics
agg = (
    df.groupby('DVN_NAME')
    .agg(
        total_messages=('GUID', 'nunique'),
        delivered_messages=('DELIVERED_BOOL_CLEAN', lambda x: x.sum()),
        median_latency=('LATENCY_SECONDS_NUM', 'median'),
        p95_latency=('LATENCY_SECONDS_NUM', lambda x: np.nanpercentile(x.dropna(), 95) if len(x.dropna()) > 0 else np.nan),
        avg_latency=('LATENCY_SECONDS_NUM', 'mean'),
        min_latency=('LATENCY_SECONDS_NUM', 'min'),
        max_latency=('LATENCY_SECONDS_NUM', 'max')
    )
    .reset_index()
)

# Delivery rate
agg['delivered_rate'] = agg['delivered_messages'] / agg['total_messages']

# Save results
agg.to_csv(output_file, index=False)
print(f"Saved {len(agg)} DVN rows with latency metrics to {output_file}")

# Display quick summary
print(agg.head(10))
