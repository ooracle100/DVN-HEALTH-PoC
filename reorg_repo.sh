#!/usr/bin/env bash
set -euo pipefail

# Run from repo root. This script is defensive: it checks for files before git-mv'ing.
# It moves KEEP files into sensible folders, archives junk files into archive_unused/,
# creates .gitignore entries for env and OS junk, commits the reorg, and pushes.

# --- CONFIG: adjust if you renamed files ---
DATA_DIR="data"
SCRIPTS_DIR="scripts"
CHARTS_DIR="charts"
DOCS_DIR="docs"
ARCHIVE_DIR="archive_unused"

mkdir -p "$DATA_DIR" "$SCRIPTS_DIR" "$CHARTS_DIR" "$DOCS_DIR" "$ARCHIVE_DIR"

# --- files to move (source -> dest directory) ---
declare -A MOVE_MAP=(
  # Data files
  ["dt_clean.csv"]="$DATA_DIR"
  ["expanded_per_dvn_joined.csv"]="$DATA_DIR"
  ["dvn_enriched_v2_per_dvn_rows.csv"]="$DATA_DIR"
  ["dvn_enriched_v2_merged_dt_enriched.csv"]="$DATA_DIR"
  ["dvn_required_optional_summary.csv"]="$DATA_DIR"
  ["stack_latency_summary.csv"]="$DATA_DIR"

  # Scripts
  ["timeframe_compare.py"]="$SCRIPTS_DIR"
  ["stack_time_series.py"]="$SCRIPTS_DIR"
  ["dvn_dashboard_viz.py"]="$SCRIPTS_DIR"
  ["merge_latency_with_kpi.py"]="$SCRIPTS_DIR"
  ["summarize_dvn_required_optional.py"]="$SCRIPTS_DIR"
  ["preview_keep_remove.sh"]="$SCRIPTS_DIR"
  ["check_guid_match.py"]="$SCRIPTS_DIR"
  ["inspect_fees_preview.py"]="$SCRIPTS_DIR"
  ["dvn_fee_summary.py"]="$SCRIPTS_DIR"
  ["dvn_fees_explode.py"]="$SCRIPTS_DIR"
  ["expand_from_fees_then_join.py"]="$SCRIPTS_DIR"
  ["process_dvn.py"]="$SCRIPTS_DIR"
  ["merge_expand_dvns_v2.py"]="$SCRIPTS_DIR"
  ["compute_dvn_stack_latency.py"]="$SCRIPTS_DIR"
  ["timeframe_compare.py"]="$SCRIPTS_DIR"

  # Charts
  ["chart_latency_vs_fees.png"]="$CHARTS_DIR"
  ["chart_required_optional_breakdown.png"]="$CHARTS_DIR"
  ["chart_latency_vs_dvn.png"]="$CHARTS_DIR"
  ["chart_fees_split_top10.png"]="$CHARTS_DIR"
  ["stack_time_series_top.png"]="$CHARTS_DIR"

  # Docs / queries
  ["flipside_dvn_query.sql"]="$DOCS_DIR"
  ["README.md"]="$DOCS_DIR"
  ["limitations.md"]="$DOCS_DIR"
)

# archive candidates (move to archive_unused)
ARCHIVE_LIST=(
  "query-DT-NOV2.json"
  "query-DT2-NOV2.csv"
  ".DS_Store"
  ".ipynb_checkpoints"
  "env"
)

echo ">>> Preparing to move files. This will use git mv (preserves history) where possible."
echo ">>> If a file is missing it will be skipped."

# Helper: git mv if exists, else mv (non-git) if not in git index
git_mv_or_mv() {
  src="$1"
  dst="$2"
  if [ ! -e "$src" ]; then
    echo "SKIP (not found): $src"
    return 0
  fi
  # use git mv if file is tracked or repo exists
  if git ls-files --error-unmatch "$src" >/dev/null 2>&1; then
    echo "git mv $src -> $dst/"
    git mv "$src" "$dst/"
  else
    echo "mv (untracked) $src -> $dst/"
    mv "$src" "$dst/"
    # add to git so it appears in commit (new path)
    git add "$dst/$(basename "$src")"
  fi
}

# Move files according to MOVE_MAP
for f in "${!MOVE_MAP[@]}"; do
  dest="${MOVE_MAP[$f]}"
  git_mv_or_mv "$f" "$dest"
done

# Move archive candidates into archive dir
for f in "${ARCHIVE_LIST[@]}"; do
  if [ -e "$f" ]; then
    # move into archive dir (git mv if tracked)
    git_mv_or_mv "$f" "$ARCHIVE_DIR"
  fi
done

# Add .gitignore entries (append only if not present)
GITIGNORE=".gitignore"
touch "$GITIGNORE"
for entry in "env/" ".DS_Store" "__pycache__/" "*.pyc"; do
  if ! grep -Fxq "$entry" "$GITIGNORE"; then
    echo "$entry" >> "$GITIGNORE"
    git add "$GITIGNORE"
  fi
done

# Stage all moved files (some were auto-added)
git add -A

# Commit & push
MSG="chore: repo reorg — move canonical data/scripts/charts into folders; archive old raw files"
if git diff --cached --quiet; then
  echo "No changes staged for commit (nothing to do)."
else
  git commit -m "$MSG"
  echo "Committed reorg. Now pushing to origin HEAD..."
  git push origin HEAD
fi

echo "Done. Kept files moved to: $DATA_DIR, $SCRIPTS_DIR, $CHARTS_DIR, $DOCS_DIR. Archive: $ARCHIVE_DIR"
