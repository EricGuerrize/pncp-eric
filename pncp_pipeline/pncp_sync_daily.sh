#!/bin/bash
# pncp_sync_daily.sh — Coleta PNCP D-1 e sincroniza com Firebase (todos os municípios MT).
# Agendado via launchd: roda todo dia útil às 07:00.

set -e

PYTHON="/Users/ericguerrize/.pyenv/versions/3.10.13/bin/python3"
SCRIPT_DIR="/Users/ericguerrize/pncp bruno/pncp_pipeline"
LOG_DIR="$SCRIPT_DIR/logs"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/sync_$(date +%Y%m%d).log"

echo "=== $(date) ===" >> "$LOG_FILE"
cd "$SCRIPT_DIR"
"$PYTHON" firebase_sync.py >> "$LOG_FILE" 2>&1
echo "=== FIM ===" >> "$LOG_FILE"
