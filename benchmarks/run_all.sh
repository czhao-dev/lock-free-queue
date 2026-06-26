#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_DIR/build-release"
RESULTS_DIR="$SCRIPT_DIR/results"

THROUGHPUT_BIN="$BUILD_DIR/benchmarks/throughput_bench"
PADDING_BIN="$BUILD_DIR/benchmarks/padding_bench"
LATENCY_BIN="$BUILD_DIR/benchmarks/latency_bench"

if [ ! -f "$THROUGHPUT_BIN" ] || [ ! -f "$PADDING_BIN" ] || [ ! -f "$LATENCY_BIN" ]; then
    echo "Benchmark binaries not found. Building Release..."
    cmake -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -S "$PROJECT_DIR"
    cmake --build "$BUILD_DIR" --parallel
fi

mkdir -p "$RESULTS_DIR"

echo "=== Throughput ==="
"$THROUGHPUT_BIN" | tee "$RESULTS_DIR/throughput.csv"

echo ""
echo "=== Padding ==="
"$PADDING_BIN" | tee "$RESULTS_DIR/padding.csv"

echo ""
echo "=== Latency ==="
"$LATENCY_BIN" | tee "$RESULTS_DIR/latency.csv"

echo ""
echo "Results saved to $RESULTS_DIR/"
