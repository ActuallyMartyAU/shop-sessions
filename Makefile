IMAGE ?= shopping-sessions:latest
CXX ?= g++
CXXFLAGS ?= -O2 -march=x86-64-v3 -std=c++17 -pthread -Wall

.PHONY: bin fixtures check-dev build run-dev
bin:
	mkdir -p build && $(CXX) $(CXXFLAGS) -o build/pipeline src/main.cpp

## all 36 edge-case fixtures, natively (needs python3 with polars)
fixtures: bin
	python3 scripts/run_fixtures.py fixtures -- build/pipeline

## exact diff of out/ against the published dev answer
check-dev:
	python3 scripts/compare.py out dev-answer

build:
	docker buildx build --platform linux/amd64 --load -t $(IMAGE) .
run-dev: build
	rm -rf out && time scripts/docker_run.sh dev out
