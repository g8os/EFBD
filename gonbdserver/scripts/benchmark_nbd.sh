#!/bin/bash

# Default args
TEST_BINARY='./nbd.test'

if [ -n "$1" ]; then
    TEST_BINARY=$1
fi

command -v "$TEST_BINARY" >/dev/null 2>&1 || \
    { echo >&2 "'$TEST_BINARY' is required but not existent."; \
      echo >&2 "Please build it using: go test ./nbd -c -o nbd.test"; \
      exit 1; }

echo -e "Running benchmarks using '$TEST_BINARY'..."

BENCHMARK_TESTS=(TestConnectionIntegrity TestConnectionIntegrityHuge)

echo -e "Benchmark tests to run: ${BENCHMARK_TESTS[@]}"
echo -e

for test in "${BENCHMARK_TESTS[@]}"; do
    echo -e "$test:"
    for i in 1 2 3 4 5 ; do
	sp=`$TEST_BINARY --test.timeout 6000m -test.run '^'$test'$' -test.v -longtests 2>&1 | grep "integrity_test.go:.* read=" | awk '{print $6}' | awk -F= '{print $2}' | sed -e 's/MBps/ MBps/'`
	echo -e "\t$i)\t$sp"
    done
    echo -e
done
