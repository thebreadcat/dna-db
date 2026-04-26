.PHONY: test-all test-all-quick bench-local bench-local-100k bench-matrix bench-v2-100k bench-v2-1m bench-v2-matrix bench-v2-concurrent-100k test-spec-compliance

THREADS ?= 16
PREP_BATCH ?= 10000
WAL_INTERVAL_MS ?= 0
MMAP_BYTES_100K ?= 384217728
MMAP_BYTES_1M ?= 2634217728
BENCH_ROOT ?= /tmp

test-all:
	python3 scripts/test_everything.py

test-all-quick:
	python3 scripts/test_everything.py --quick

bench-local:
	cd engine && cargo run --release --bin load_bench -- --mode strict --batch-size 1 --records 10000 --read-sample 2000 --data-dir ./bench-data-local

bench-local-100k:
	cd engine && cargo run --release --bin load_bench -- --mode strict --batch-size 1 --records 100000 --read-sample 10000 --data-dir ./bench-data-local-100k

bench-matrix:
	python3 scripts/bench_matrix.py --records 100000 --read-sample 10000

bench-v2-100k:
	cd engine && cargo run --release --bin load_bench -- --mode balanced --records 100000 --read-sample 10000 --batch-size 1000 --threads $(THREADS) --prep-batch $(PREP_BATCH) --wal-interval-ms $(WAL_INTERVAL_MS) --mmap-bytes $(MMAP_BYTES_100K) --data-dir $(BENCH_ROOT)/dnadb-bench-100k

bench-v2-1m:
	cd engine && cargo run --release --bin load_bench -- --mode balanced --records 1000000 --read-sample 10000 --batch-size 1000 --threads $(THREADS) --prep-batch $(PREP_BATCH) --wal-interval-ms $(WAL_INTERVAL_MS) --mmap-bytes $(MMAP_BYTES_1M) --data-dir $(BENCH_ROOT)/dnadb-bench-1m

bench-v2-matrix:
	python3 scripts/bench_matrix.py --records 100000 --read-sample 10000 --threads $(THREADS) --prep-batch $(PREP_BATCH) --wal-interval-ms $(WAL_INTERVAL_MS) --data-dir $(BENCH_ROOT)/dnadb-bench-matrix --save

bench-v2-concurrent-100k:
	cd engine && cargo run --release --bin load_bench -- --mode balanced --records 100000 --read-sample 10000 --batch-size 1000 --threads $(THREADS) --prep-batch $(PREP_BATCH) --concurrent-writers --wal-interval-ms $(WAL_INTERVAL_MS) --mmap-bytes $(MMAP_BYTES_100K) --data-dir $(BENCH_ROOT)/dnadb-bench-concurrent-100k

test-spec-compliance:
	python3 scripts/test_spec_compliance.py --records 100000 --data-dir $(BENCH_ROOT)/dnadb-spec-compliance
