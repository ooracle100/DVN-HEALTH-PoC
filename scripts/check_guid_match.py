#!/usr/bin/env python3
import sys, pandas as pd

if len(sys.argv) < 3:
    print("Usage: python check_guid_match.py <dt_clean.csv> <dvnFeesMapped.csv>")
    sys.exit(1)

a = pd.read_csv(sys.argv[1], dtype=str, keep_default_na=False)
b = pd.read_csv(sys.argv[2], dtype=str, keep_default_na=False)

def find_guid_col(df):
    for cand in ["GUID","guid","Guid","guid_hex","guidhash","sourceguid"]:
        if cand in df.columns:
            return cand
    # fallback: try to find any column that looks like hex starting with 0x
    for c in df.columns:
        sample = str(df[c].dropna().astype(str).head(20).tolist())
        if "0x" in sample and len(sample) > 5:
            return c
    return None

col_a = find_guid_col(a)
col_b = find_guid_col(b)
print("Detected GUID columns:", "file1:", col_a, "file2:", col_b)
if col_a is None or col_b is None:
    print("Could not detect GUID column in one of the files. Columns file1:", a.columns.tolist(), "file2:", b.columns.tolist())
    sys.exit(1)

def norm(s):
    if s is None: return None
    s = str(s).strip()
    if s.startswith('='):
        s = s[1:].strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1]
    return s.lower()

a_guids = a[col_a].astype(str).apply(norm).dropna().unique()
b_guids = b[col_b].astype(str).apply(norm).dropna().unique()

print("\nSample GUIDs from file1 (first 5):", a_guids[:5].tolist())
print("Sample GUIDs from file2 (first 5):", b_guids[:5].tolist())

set_a = set(a_guids)
set_b = set(b_guids)
common = set_a & set_b
print(f"\nCounts: file1 unique GUIDs = {len(set_a)}, file2 unique GUIDs = {len(set_b)}, common = {len(common)}")
if len(set_a) > 0:
    print("Join rate relative to file1: {:.2%}".format(len(common)/len(set_a)))
if len(set_b) > 0:
    print("Join rate relative to file2: {:.2%}".format(len(common)/len(set_b)))

# show up to 10 sample mismatches from each side
only_a = list(set_a - set_b)[:10]
only_b = list(set_b - set_a)[:10]
print("\nSample GUIDs present only in file1 (up to 10):", only_a)
print("Sample GUIDs present only in file2 (up to 10):", only_b)
