#!/usr/bin/env bash
# Download the dataset and verify its checksum. Idempotent.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/raw

URL="https://huggingface.co/datasets/lukebarousse/data_jobs/resolve/main/data_jobs.csv"
EXPECTED="635241ed09ccee18bdae1f83b45f26d6759e0aa2513c529f6190e9054062436c"
DEST="data/raw/data_jobs.csv"

[[ -f "$DEST" ]] || curl -sL -o "$DEST" "$URL"

ACTUAL=$(shasum -a 256 "$DEST" | cut -d' ' -f1)
if [[ "$ACTUAL" != "$EXPECTED" ]]; then
  echo "checksum mismatch: $ACTUAL"; exit 1
fi
echo "dataset ok: $DEST ($(wc -l < "$DEST" | tr -d ' ') lines, $(du -h "$DEST" | cut -f1))"
