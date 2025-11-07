# dvn_required_optional_chart.py
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# === Load the summary ===
df = pd.read_csv("dvn_required_optional_summary.csv")

# === Sort and keep top 20 by required count ===
df_sorted = df.sort_values("required_count", ascending=False).head(20)

# === Plot ===
x = np.arange(len(df_sorted))
width = 0.35

fig, ax = plt.subplots(figsize=(10, 5))

# Bars for required and optional
ax.bar(x - width/2, df_sorted["required_count"], width, label="Required", color="#2b83ba")
ax.bar(x + width/2, df_sorted["optional_count"], width, label="Optional", color="#fdae61")

# Labels and style
ax.set_xticks(x)
ax.set_xticklabels(df_sorted["DVN_NAME"], rotation=45, ha="right", fontsize=9)
ax.set_ylabel("Message Count")
ax.set_title("DVN Roles Count (Required vs Optional) – Top 20 by Required Count")
ax.legend()
plt.tight_layout()

plt.savefig("chart_required_optional_breakdown.png", dpi=300)
plt.close()
print("✅ Saved: chart_required_optional_breakdown.png")
