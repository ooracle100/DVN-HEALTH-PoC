import pandas as pd

# Step 1: Load exploded DVN-fee data
df = pd.read_csv("dvnFeesMapped_Exploded.csv")

# Step 2: Group by DVN_Name and DVN_Type to get total and average fees
grouped = df.groupby(['DVN_Name', 'DVN_Type']).agg(
    total_fee_eth=pd.NamedAgg(column='DVN_Fee_ETH', aggfunc='sum'),
    avg_fee_eth=pd.NamedAgg(column='DVN_Fee_ETH', aggfunc='mean')
).reset_index()

# Step 3: Add dummy index for pivoting
grouped['dummy'] = 1

# Step 4: Pivot the data to wide format - separate columns per DVN and DVN type for totals
total_pivot = grouped.pivot(index='dummy', columns=['DVN_Name', 'DVN_Type'], values='total_fee_eth')
avg_pivot = grouped.pivot(index='dummy', columns=['DVN_Name', 'DVN_Type'], values='avg_fee_eth')

# Step 5: Reset index to flatten dataframes (drop dummy)
total_pivot = total_pivot.reset_index(drop=True)
avg_pivot = avg_pivot.reset_index(drop=True)

# Step 6: Flatten multi-level column indices into single strings for easier CSV columns
total_pivot.columns = ['TotalFee_' + '_'.join(col).replace(' ', '') for col in total_pivot.columns]
avg_pivot.columns = ['AvgFee_' + '_'.join(col).replace(' ', '') for col in avg_pivot.columns]

# Step 7: Concatenate total and average fee dataframes side by side
summary = pd.concat([total_pivot, avg_pivot], axis=1)

# Step 8: Save summary to CSV
summary.to_csv("dvnFees_Summary_ByDVN.csv", index=False)

print("DVN fees summary file created: dvnFees_Summary_ByDVN.csv")
