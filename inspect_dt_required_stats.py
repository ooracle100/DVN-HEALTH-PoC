
import pandas as pd
df = pd.read_csv("expanded_per_dvn_joined.csv", dtype=str)
df['ROLE'] = df['ROLE'].str.lower().fillna('')
df['LATENCY_S'] = pd.to_numeric(df.get('LATENCYTODELIVERY_SECONDS','').astype(str).str.replace(r'[^0-9.]','',regex=True), errors='coerce')
mask_required = (df['DVN_NAME']=='Deutsche Telekom') & (df['ROLE']=='required')
mask_optional = (df['DVN_NAME']=='Deutsche Telekom') & (df['ROLE']=='optional')
print("DT total GUIDs (any role):", df[df['DVN_NAME']=='Deutsche Telekom']['GUID'].nunique())
print("DT required GUIDs:", df[mask_required]['GUID'].nunique())
print("DT required GUIDs with numeric latency:", df[mask_required & df['LATENCY_S'].notna()]['GUID'].nunique())
print("DT optional GUIDs:", df[mask_optional]['GUID'].nunique())
print("DT optional delivered GUIDs:", df[mask_optional & (df['DELIVERED_BOOL'].astype(str).str.upper()=='TRUE')]['GUID'].nunique())

