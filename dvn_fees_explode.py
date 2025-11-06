import pandas as pd

# Step 1: Load existing mapped CSV file
fees_df = pd.read_csv("dvnFeesMapped.csv")

# Step 2: Explode RequiredDVN_Mapping column (list of tuples)
fees_df['RequiredDVN_Mapping'] = fees_df['RequiredDVN_Mapping'].apply(eval)  # convert string repr back to list
req_expanded = fees_df[['GUID', 'RequiredDVN_Mapping']].explode('RequiredDVN_Mapping')
req_expanded[['DVN_Name', 'DVN_Fee']] = pd.DataFrame(req_expanded['RequiredDVN_Mapping'].tolist(), index=req_expanded.index)
req_expanded['DVN_Type'] = 'RequiredDVN'
req_expanded.drop(columns=['RequiredDVN_Mapping'], inplace=True)

# Step 3: Explode OptionalDVN_Mapping column
fees_df['OptionalDVN_Mapping'] = fees_df['OptionalDVN_Mapping'].apply(eval)
opt_expanded = fees_df[['GUID', 'OptionalDVN_Mapping']].explode('OptionalDVN_Mapping')
opt_expanded[['DVN_Name', 'DVN_Fee']] = pd.DataFrame(opt_expanded['OptionalDVN_Mapping'].tolist(), index=opt_expanded.index)
opt_expanded['DVN_Type'] = 'OptionalDVN'
opt_expanded.drop(columns=['OptionalDVN_Mapping'], inplace=True)

# Step 4: Combine exploded dataframes
combined = pd.concat([req_expanded, opt_expanded])

# Step 5: Convert fees to numeric for easier analysis
combined['DVN_Fee'] = pd.to_numeric(combined['DVN_Fee'])

combined['DVN_Fee_ETH'] = combined['DVN_Fee'] / 10**18

# Step 6: Save exploded version to new CSV
combined.to_csv("dvnFeesMapped_Exploded.csv", index=False)

print("Exploded DVN mapping saved to dvnFeesMapped_Exploded.csv")
print(combined.head())
