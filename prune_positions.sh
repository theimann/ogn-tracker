#!/bin/bash
# Prune ogn_positions rows older than 30 days
# Runs in batches to avoid long locks

set -euo pipefail

ENV_FILE="/opt/ogn-collector/.env"
source <(grep -v '^#' "$ENV_FILE" | sed 's/^/export /')

BATCH=50000
CUTOFF=$(date -d '30 days ago' '+%Y-%m-%d %H:%M:%S')

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Pruning ogn_positions older than $CUTOFF"

total=0
while true; do
    deleted=$(mysql -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -sNe \
        "DELETE FROM ogn_positions WHERE received_at < '$CUTOFF' LIMIT $BATCH;" 2>/dev/null)
    # mysql DELETE doesn't return rows affected via -sNe; use ROW_COUNT
    rows=$(mysql -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -sNe \
        "SELECT ROW_COUNT();" 2>/dev/null)
    total=$((total + rows))
    if [ "$rows" -eq 0 ]; then
        break
    fi
    sleep 0.1  # brief pause between batches
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Done — deleted $total rows"
