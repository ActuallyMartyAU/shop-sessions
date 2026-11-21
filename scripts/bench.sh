#!/bin/bash
# Reproducible benchmark: bench.sh BIN [RUNS] [DATA]
# Default data: generate --seed 7 --events 20000000 --clicks 5000000 --output data/p20
# Prints per-phase durations (from the binary's cumulative stderr stamps) and wall time per run, warm cache.
set -eu
BIN=${1:?binary}; RUNS=${2:-5}; DATA=${3:-data/p20}
cat "$DATA"/*/*.csv > /dev/null   # warm the page cache
for i in $(seq 1 "$RUNS"); do
  rm -rf out/bench
  s=$(date +%s.%N)
  NCPU=${NCPU:-8} DATA_DIR=$DATA OUTPUT_DIR=out/bench "$BIN" 2> out/bench.log
  e=$(date +%s.%N)
  awk -v wall="$(echo "$e - $s" | bc)" '/^\[/ { gsub(/[\[\]]/, ""); t = $1; printf "%s %.3f  ", $2, t - prev; prev = t } END { printf "| wall %.3f\n", wall }' out/bench.log
done
