#!/usr/bin/env bash
# Delete the msds682 homework cluster (and its API keys) so nothing further
# accrues. Lists what it found and asks you to confirm before deleting.
#
# Usage:
#   ./teardown.sh                 # find cluster by CLUSTER_NAME, confirm, delete
#   ./teardown.sh lkc-xxxxx       # delete a specific cluster id, confirm, delete
#   ./teardown.sh --force         # skip the interactive confirmation (careful!)
#
# Requires: confluent CLI (logged in) and jq.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cluster_config.sh"

FORCE=false
CLUSTER_ARG=""
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=true ;;
    lkc-*)   CLUSTER_ARG="$arg" ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

# ---------------------------------------------------------------- preflight ---
command -v confluent >/dev/null || { echo "ERROR: confluent CLI not found."; exit 1; }
command -v jq        >/dev/null || { echo "ERROR: jq not found. Install with: brew install jq"; exit 1; }
confluent kafka cluster list >/dev/null 2>&1 || { echo "ERROR: not logged in. Run: confluent login"; exit 1; }

# ---------------------------------------------------- resolve target cluster ---
if [[ -n "$CLUSTER_ARG" ]]; then
  cluster_id="$CLUSTER_ARG"
else
  matches=$(confluent kafka cluster list --output json \
    | jq -r --arg n "$CLUSTER_NAME" '.[] | select(.name == $n) | (.id // .cluster_id)')
  count=$(printf '%s\n' "$matches" | grep -c . || true)
  if [[ "$count" -eq 0 ]]; then
    echo "No cluster named '$CLUSTER_NAME' found. Nothing to delete."; exit 0
  elif [[ "$count" -gt 1 ]]; then
    echo "Multiple clusters named '$CLUSTER_NAME':"; printf '  %s\n' $matches
    echo "Re-run with an explicit id:  ./teardown.sh lkc-xxxxx"; exit 1
  fi
  cluster_id="$matches"
fi

# ------------------------------------------------------------- show + confirm ---
echo "About to delete this cluster and ALL of its topics and messages:"
confluent kafka cluster describe "$cluster_id" 2>/dev/null || { echo "ERROR: cluster $cluster_id not found."; exit 1; }

keys=$(confluent api-key list --resource "$cluster_id" --output json 2>/dev/null \
  | jq -r '.[] | (.key // .api_key)' || true)
if [[ -n "$keys" ]]; then
  echo; echo "API keys bound to this cluster (will be deleted too):"; printf '  %s\n' $keys
fi

if ! $FORCE; then
  echo
  read -r -p "Type the cluster id ($cluster_id) to confirm deletion: " reply
  if [[ "$reply" != "$cluster_id" ]]; then echo "Confirmation did not match. Aborted."; exit 1; fi
fi

# ---------------------------------------------------------------- delete ---
for k in $keys; do
  echo "Deleting API key $k ..."
  confluent api-key delete "$k" --force >/dev/null 2>&1 || confluent api-key delete "$k" >/dev/null 2>&1 || true
done

echo "Deleting cluster $cluster_id ..."
confluent kafka cluster delete "$cluster_id" --force >/dev/null 2>&1 \
  || confluent kafka cluster delete "$cluster_id"

echo "Done. Cluster $cluster_id and its topics are gone; no further charges will accrue."
