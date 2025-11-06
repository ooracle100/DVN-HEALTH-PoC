import pandas as pd

# Input files
fees_file = "kpi_by_dvn_final.csv"
latency_file = "kpi_by_dvn_latency_added.csv"
expanded_file = "expanded_per_dvn_joined.csv"  # has ROLE info per DVN
output_file = "kpi_combined_fees_latency_rolecount.csv"

# Load data
fees_df = pd.read_csv(fees_file)
lat_df = pd.read_csv(latency_file)
exp_df = pd.read_csv(expanded_file)

# Normalize DVN names
for df in [fees_df, lat_df, exp_df]:
    df['DVN_NAME'] = df['DVN_NAME'].astype(str).str.strip().str.lower()

# ---- Add Required/Optional counts ----
role_counts = exp_df.groupby(['DVN_NAME', 'ROLE']).size().unstack(fill_value=0)
role_counts = role_counts.rename(columns={
    'required': 'required_count',
    'optional': 'optional_count'
}).reset_index()

# Merge everything
merged = (
    fees_df
    .merge(lat_df, on='DVN_NAME', suffixes=('_fees', '_lat'), how='outer')
    .merge(role_counts, on='DVN_NAME', how='left')
)

# Clean up column order
keep_cols = [
    'DVN_NAME',
    'unique_messages', 'total_messages',
    'required_count', 'optional_count',
    'total_fees_eth', 'total_required_fees_eth', 'total_optional_fees_eth',
    'median_latency', 'p95_latency', 'avg_latency',
    'min_latency', 'max_latency',
    'delivered_messages', 'delivered_rate'
]
merged = merged[[c for c in keep_cols if c in merged.columns]]

# Save and print
merged.to_csv(output_file, index=False)
print(f"✅ Combined dataset saved as: {output_file}")
print(merged.head(10))
