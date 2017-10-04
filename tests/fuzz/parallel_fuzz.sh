#!/bin/bash

# uses 8 cpu cores to start
export AFL_SKIP_CPUFREQ=yes

afl-fuzz -i ./in -o ./sync_dir -M fuzzer01 target/debug/redis_fuzzer 2>&1 > /dev/null &
afl-fuzz -i ./in -o ./sync_dir -S fuzzer02 target/debug/redis_fuzzer 2>&1 > /dev/null &
afl-fuzz -i ./in -o ./sync_dir -S fuzzer03 target/debug/redis_fuzzer 2>&1 > /dev/null &
afl-fuzz -i ./in -o ./sync_dir -S fuzzer04 target/debug/redis_fuzzer 2>&1 > /dev/null &
afl-fuzz -i ./in -o ./sync_dir -S fuzzer05 target/debug/redis_fuzzer 2>&1 > /dev/null &
afl-fuzz -i ./in -o ./sync_dir -S fuzzer06 target/debug/redis_fuzzer 2>&1 > /dev/null &
afl-fuzz -i ./in -o ./sync_dir -S fuzzer07 target/debug/redis_fuzzer 2>&1 > /dev/null &
afl-fuzz -i ./in -o ./sync_dir -S fuzzer08 target/debug/redis_fuzzer 2>&1 > /dev/null &

afl-whatsup sync_dir