#!/usr/bin/env bash
# Append a row to BUILD_LOG.md marking a finished block.
# usage: scripts/mark.sh "<block>" "<prd refs>" "<notes>"
set -euo pipefail
cd "$(dirname "$0")/.."
log=BUILD_LOG.md
n=$(( $(grep -c '^| [0-9]' "$log" || true) + 1 ))
ts=$(date '+%Y-%m-%d %H:%M')
printf '| %s | %s | %s | %s | %s |\n' "$n" "$ts" "$1" "${2:-}" "${3:-}" >> "$log"
echo "marked #$n at $ts: $1"
