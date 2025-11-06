import pandas as pd

# Load your CSV files
fees_df = pd.read_csv("dvnFeesReqOp-Sheet1.csv")
names_df = pd.read_csv("dvnNames-Sheet2.csv")

# Clean column headers (strip spaces if any)
fees_df.columns = fees_df.columns.str.strip()
names_df.columns = names_df.columns.str.strip()

# Create address-to-name dictionary, lowercase for consistency
address_to_name = dict(zip(names_df['DVN_Address'].str.lower(), names_df['DVN_Name']))

# Function to map DVNs to names and pair with fees
def map_dvns_and_fees(dvn_string, fee_string):
    if pd.isna(dvn_string) or pd.isna(fee_string):
        return []
    # Clean and split addresses and fees
    addresses = [addr.strip().lower() for addr in dvn_string.strip("[]").split(";")]
    fees = [fee.strip() for fee in fee_string.strip("[]").split(";")]
    
    length = min(len(addresses), len(fees))  # ensure matching length
    
    mapped = []
    for i in range(length):
        name = address_to_name.get(addresses[i], "Unknown DVN")
        mapped.append((name, fees[i]))
    return mapped

# Apply mapping function on requiredDVNs and optionalDVNs columns separately
fees_df['RequiredDVN_Mapping'] = fees_df.apply(
    lambda row: map_dvns_and_fees(row['requiredDVNs'], row['DVN_FEES_ARRAY']),
    axis=1
)
fees_df['OptionalDVN_Mapping'] = fees_df.apply(
    lambda row: map_dvns_and_fees(row['optionalDVNs'], row['DVN_FEES_ARRAY']),
    axis=1
)

# Save result to CSV - will overwrite by default
fees_df.to_csv("dvnFeesMapped.csv", index=False, mode='w')

print("Mapped DVNs saved to dvnFeesMapped.csv")
print(fees_df[['RequiredDVN_Mapping', 'OptionalDVN_Mapping']].head())
