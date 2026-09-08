#!/bin/zsh
cd "/Users/suryachandrakasan/GitHub Projects/MarketsBot"
while true; do
  if [ "$(ls data/dataset/prices 2>/dev/null | wc -l)" -ge 3 ]; then
    RUST_LOG=warn ./target/release/mbot universe > logs/universe-last.log 2>&1
    echo "$(date -u +%FT%TZ) universe re-run: $(grep -E 'strategies generated' logs/universe-last.log | head -1)"
  fi
  pgrep -f "mbot build-dataset" >/dev/null || { RUST_LOG=warn ./target/release/mbot universe > logs/universe-last.log 2>&1; echo "$(date -u +%FT%TZ) final universe run"; break; }
  sleep 300
done
