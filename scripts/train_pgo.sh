#!/bin/sh
# Regenerate profiles/pipeline-main.gcda (profile-guided optimisation) with the same compiler
# the Dockerfile uses. Re-run after every change to src/main.cpp:  scripts/train_pgo.sh DATA_DIR
# DATA_DIR should be a practice-size input (e.g. generator --events 20000000 --clicks 5000000).
set -eu
DATA=$(cd "$1" && pwd)
REPO=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$REPO/profiles"
docker run --rm --network none --user "$(id -u):$(id -g)" \
  -v "$REPO/src":/in:ro -v "$DATA":/data:ro -v "$REPO/profiles":/prof \
  gcc:13-bookworm sh -ec '
    mkdir -p /tmp/src && cp /in/main.cpp /tmp/src/ && cd /tmp/src
    # Same flags as the Dockerfile plus instrumentation; -u keeps the profile writer linked in.
    g++ -O2 -march=x86-64-v3 -std=c++17 -pthread -static \
        -fprofile-generate -fprofile-update=atomic -Wl,-u,__gcov_dump -o pipeline main.cpp
    NCPU=8 DATA_DIR=/data OUTPUT_DIR=/tmp/out ./pipeline
    cp pipeline-main.gcda /prof/pipeline-main.gcda'
ls -l "$REPO/profiles/pipeline-main.gcda"
