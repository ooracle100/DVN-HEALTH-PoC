import pandas as pd
import ast

# Load datasets
df = pd.read_csv('dt_clean.csv')
names_df = pd.read_csv('dvnNames-Sheet2.csv')

# Normalize headers
df.columns = df.columns.str.strip()
names_df.columns = names_df.columns.str.strip()

# Build address->name dictionary
address_to_name = dict(zip(names_df['DVN_Address'].str.lower(), names_df['DVN_Name']))

# Parse stringified lists safely
for col in ['REQUIREDDVNS', 'OPTIONALDVNS', 'DVN_FEES_ARRAY']:
    df[col] = df[col].apply(
        lambda x: ast.literal_eval(x.strip()) if pd.notna(x) and str(x).strip().startswith('[') else []
    )

# Map addresses to names paired with fees, returns list of tuples
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

# DT official address and flag columns
dt_address = '0xc2a0c36f5939a14966705c7cec813163faeea1f0'
df['DT_Required'] = df['REQUIREDDVNS'].apply(lambda x: dt_address in [addr.lower() for addr in x])
df['DT_Optional'] = df['OPTIONALDVNS'].apply(lambda x: dt_address in [addr.lower() for addr in x])

print("Total rows in dataset:", len(df))
print("Rows with DT Required:", df['DT_Required'].sum())
print("Rows with DT Optional:", df['DT_Optional'].sum())


# Filter transactions with DT involvement
dt_rows = df[df['DT_Required'] | df['DT_Optional']].copy()

# Extract scalar DT fee
def get_dt_fee(row):
    if row['DT_Required']:
        for name, fee in row['RequiredDVN_Mapping']:
            if name == "Deutsche Telekom":
                return fee
    if row['DT_Optional']:
        for name, fee in row['OptionalDVN_Mapping']:
            if name == "Deutsche Telekom":
                return fee
    return None

# Apply extraction separately and assign
dt_fee_series = dt_rows.apply(get_dt_fee, axis=1)
print(dt_fee_series.head(10))
print(type(dt_fee_series.iloc[0]))

dt_rows['DT_Fee_ETH'] = dt_fee_series

# Example stats
total_dt = len(dt_rows)
delivered_dt = dt_rows['DELIVERED_BOOL'].sum()
delivery_rate_dt = delivered_dt / total_dt if total_dt > 0 else 0
avg_latency_dt = dt_rows.loc[dt_rows['DELIVERED_BOOL'], 'LATENCYTODELIVERY_SECONDS'].mean()
avg_fee_required = dt_rows.loc[dt_rows['DT_Required'], 'DT_Fee_ETH'].mean()
avg_fee_optional = dt_rows.loc[dt_rows['DT_Optional'], 'DT_Fee_ETH'].mean()

# Print summary
print("=== Deutsche Telekom DVN Performance Summary ===")
print(f"Total DT-involved transactions: {total_dt}")
print(f"Delivered transactions: {delivered_dt} ({delivery_rate_dt:.2%})")
print(f"Average delivery latency (seconds) [delivered only]: {avg_latency_dt:.2f}")
print(f"Average fee (ETH) when DT Required: {avg_fee_required:.6f}")
print(f"Average fee (ETH) when DT Optional: {avg_fee_optional:.6f}")

# Save detailed results to CSV
dt_rows.to_csv('deutsche_telekom_transactions_detailed.csv', index=False)
print("Detailed Deutsche Telekom transaction data saved to 'deutsche_telekom_transactions_detailed.csv'")

print("Total rows in dataset:", len(df))
print("Rows with DT Required:", df['DT_Required'].sum())
print("Rows with DT Optional:", df['DT_Optional'].sum())

