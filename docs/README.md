README.md
DVN Health Proof of Concept (PoC) — Deutsche Telekom LayerZero Stack
Overview
This project presents a reproducible Proof of Concept (PoC) for monitoring the health of Deutsche Telekom’s DVN within the LayerZero security stack. It generates auditable KPIs related to fees, role distribution, latency, delivery rates, and stack dependencies to support operational and security risk analysis for Web3 infrastructure teams.

Features
Per-DVN fee calculations (ETH-based) and role counts (required vs. optional)

Delivery rate metrics and latency analysis (median and 95th percentile) derived from Flipside decoded Ethereum logs

Stack co-occurrence and impact assessment during outage windows

Fully reproducible analysis with SQL queries, Python/JavaScript scripts, and sample sanitized datasets

Visual dashboard graphics summarizing key insights

Getting Started
Clone this repository

Install Python dependencies (e.g., pandas, matplotlib)

Run the Python scripts in the scripts/ folder to reproduce KPIs and charts using the CSV files in data/

View PNG charts in the charts/ folder for visual insights

Included Files
scripts/: Processing and visualization scripts

data/: Curated sample datasets for reproducibility

charts/: Visual report images in PNG format

README.md: This overview

limitations.md: Methodology limitations and disclaimers

Limitations
Please review limitations.md for crucial information on proxy latency calculations, data scope, and ethical considerations.

Contributing
Currently maintained by ooracle100. Contributions and feedback are welcome via GitHub issues.

License
This project is licensed under the MIT License. See the LICENSE file for full license details.

limitations.md
Limitations and Disclaimers
Proxy Latency Metrics: Latency values are estimated at the stack level using timestamp deltas from decoded on-chain logs. Direct per-node verification timestamps are unavailable on-chain, so latency attribution per DVN is a proxy.

Correlation vs. Causation: Outage analyses (e.g., Oct 19–21 AWS incidents) are based on timing correlations; definitive cause requires provider logs and telemetry.

Data Privacy: Raw full datasets and provider logs remain private and are not included. Shared sample CSVs are sanitized for reproducibility without exposing all raw data points.

No Overclaiming: The PoC presents evidence-based insights avoiding causal assertions where data is incomplete or proxy-derived.

Reproducibility: All SQL queries, scripts, and sanitized data samples are included to allow independent verification of KPIs and visualizations.
