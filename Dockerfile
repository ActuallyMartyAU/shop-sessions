# Shopping Sessions: native C++ pipeline (no runtime dependencies).
# Build stage: compile a static binary. x86-64-v3 (AVX2) runs on every scorer (Cascade Lake or newer).
FROM gcc:13-bookworm AS build
WORKDIR /src
COPY src/ /src/
# Profile-guided optimisation: profiles/pipeline-main.gcda comes from scripts/train_pgo.sh run on
# practice-size data with this same compiler. A stale profile only warns (and is ignored per function).
COPY profiles/ /src/
RUN g++ -O2 -march=x86-64-v3 -std=c++17 -pthread -static \
        -fprofile-use -fprofile-partial-training -Wno-missing-profile -Wno-error=coverage-mismatch \
        -o pipeline main.cpp

FROM debian:bookworm-slim
COPY --from=build /src/pipeline /app/pipeline
# Runs as uid 1000 with a read-only root; reads $DATA_DIR, writes $OUTPUT_DIR. Logs go to stderr only.
CMD ["/app/pipeline"]
