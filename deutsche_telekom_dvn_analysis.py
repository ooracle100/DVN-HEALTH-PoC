import pandas as pd
import ast

# Load your full dataset and DVN names mapping
df = pd.read_csv('dt_clean.csv')
names_df = pd.read_csv('dvnNames-Sheet2.csv')

# Clean column headers
df.columns = df.columns.str.strip()
names_df.columns = names_df.columns.str.strip()

# Create address-to-name dictionary with lowercase keys
address_to_name = dict(zip(names_df['DVN_Address'].str.lower(), names_df['DVN_Name']))

# Parse stringified list columns to real lists
for col in ['REQUIREDDVNS', 'OPTIONALDVNS', 'DVN_FEES_ARRAY']:
    df[col] = df[col].apply(
        lambda x: ast.literal_eval(x.strip()) if pd.notna(x) and str(x).strip().startswith('[') else []
    )

# Map DVN addresses to names with fees per row (return list of tuples)
def map_dvns_and_fees(dvn_addresses, dvn_fees):
    if not dvn_addresses or not dvn_fees:
        return []
    addresses = [addr.lower().strip() for addr in dvn_addresses]
    fees = [float(fee)/1e18 for fee in dvn_fees]
    length = min(len(addresses), len(fees))
    return [(address_to_name.get(addresses[i], "Unknown DVN"), fees[i]) for i in range(length)]

df['RequiredDVN_Mapping'] = df.apply(
    lambda row: map_dvns_and_fees(row['REQUIREDDVNS'], row['DVN_FEES_ARRAY']), axis=1
)

df['OptionalDVN_Mapping'] = df.apply(
    lambda row: map_dvns_and_fees(row['OPTIONALDVNS'], row['DVN_FEES_ARRAY'][len(row['REQUIREDDVNS']):]), axis=1
)

# Deutsche Telekom address and presence flags
dt_address = '0xc2a0c36f5939a14966705c7cec813163faeea1f0'

df['DT_Required'] = df['REQUIREDDVNS'].apply(
    lambda x: dt_address in [addr.lower() for addr in x]
)
df['DT_Optional'] = df['OPTIONALDVNS'].apply(
    lambda x: dt_address in [addr.lower() for addr in x]
)

# Filter rows with DT participation
dt_rows = df[df['DT_Required'] | df['DT_Optional']].copy()

# Extract scalar Deutsche Telekom fee per row
def get_dt_fee(row):
    if row['DT_Required']:
        for name, fee in row['RequiredDVN_Mapping']:
            if name == "Deutsche Telekom":
                return float(fee)
    if row['DT_Optional']:
        for name, fee in row['OptionalDVN_Mapping']:
            if name == "Deutsche Telekom":
                return float (fee)
    return None

# Create separate scalar fee column for DT
dt_rows['DT_Fee_ETH'] = dt_rows.apply(get_dt_fee, axis=1)



# Compute some example stats
total_dt = len(dt_rows)
delivered_dt = dt_rows['DELIVERED_BOOL'].sum()
delivery_rate_dt = delivered_dt / total_dt if total_dt > 0 else 0
avg_latency_dt = dt_rows.loc[dt_rows['DELIVERED_BOOL'], 'LATENCYTODELIVERY_SECONDS'].mean()
avg_fee_required = dt_rows.loc[dt_rows['DT_Required'], 'DT_Fee_ETH'].mean()
avg_fee_optional = dt_rows.loc[dt_rows['DT_Optional'], 'DT_Fee_ETH'].mean()

print("=== Deutsche Telekom DVN Performance Summary ===")
print(f"Total DT-involved transactions: {total_dt}")
print(f"Delivered transactions: {delivered_dt} ({delivery_rate_dt:.2%})")
print(f"Average delivery latency (seconds) [delivered only]: {avg_latency_dt:.2f}")
print(f"Average fee (ETH) when DT Required: {avg_fee_required:.6f}")
print(f"Average fee (ETH) when DT Optional: {avg_fee_optional:.6f}")

# Save enriched dataframe with mappings and DT scalar fee
dt_rows.to_csv('deutsche_telekom_transactions_detailed.csv', index=False)
print("Detailed DT transaction data including DVN mappings saved to 'deutsche_telekom_transactions_detailed.csv'")
