#!/usr/bin/env python3
import sys, pandas as pd
pd.set_option('display.max_colwidth', 400)

if len(sys.argv) < 2:
    print("Usage: python inspect_fees_preview.py <dvnFeesMapped.csv>")
    raise SystemExit(1)

fn = sys.argv[1]
df = pd.read_csv(fn, dtype=str, keep_default_na=False, na_values=['','NA','N/A'])

cols_of_interest = [
    'GUID','requiredDVNs','optionalDVNs','DVN_FEES_ARRAY',
    'RequiredDVN_Mapping','OptionalDVN_Mapping'
]

print("File loaded:", fn)
print("Columns in file:", df.columns.tolist())
print("\nShowing first 8 rows for key columns (raw):\n")
for i, row in df.head(8).iterrows():
    print("---- ROW", i, "----")
    for c in cols_of_interest:
        if c in df.columns:
            print(f"{c}: {row[c]}")
        else:
            print(f"{c}: <MISSING>")
    print()
