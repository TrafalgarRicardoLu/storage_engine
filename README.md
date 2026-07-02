# storage_engine

A compact C++20 single-node key-value storage engine prototype focused on a
small, explicit durable write path.

The current implementation is intentionally narrow: writes are appended to a
WAL, synced with `fdatasync`, then applied to an in-memory table. The project is
Linux-only and uses a direct raw-syscall `io_uring` wrapper instead of depending
on packaged `liburing` development files.

## Current Features

- Single-process embedded KV API: `Put`, `Delete`, `Write`, and `Get`.
- Coroutine-first async API with synchronous wrappers over the same path.
- WAL recovery with CRC validation and torn-tail truncation.
- Hard-durability write contract: a successful write returns only after WAL
  `writev` and `fdatasync` complete successfully.
- Multi-writer group commit with adaptive high-pressure batching.
- Shared-lock read path over an in-memory table.
- RocksDB-style benchmark names for local comparisons:
  `fillseq`, `fillrandom`, `readrandom`, `overwrite`,
  `concurrent_fillseq`, and `concurrent_readrandom`.

## Current Non-Goals

These are not implemented yet:

- SSTables, manifest, flush, and compaction.
- Replication or multi-process concurrent open of one database directory.
- MVCC snapshots, iterators, transactions, merge operators, compression, and
  secondary indexes.
- Production crash-consistency coverage beyond the single WAL file path.

## Build

Requirements:

- Linux with `io_uring` kernel headers available.
- CMake 3.19 or newer.
- A C++20 compiler.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

## Test

```bash
ctest --test-dir build --output-on-failure
```

The test binary covers basic put/get/delete behavior, WAL recovery, torn WAL
tail handling, coroutine completion, buffer reuse, and group commit behavior.

## Benchmark

Run a small write benchmark:

```bash
./build/storage_engine_bench \
  --benchmarks=fillseq \
  --num=10000 \
  --key_size=16 \
  --value_size=100 \
  --db=/tmp/storage_engine_bench_fillseq
```

Run concurrent writes and print group-commit stats:

```bash
./build/storage_engine_bench \
  --benchmarks=concurrent_fillseq \
  --num=16000 \
  --threads=16 \
  --key_size=16 \
  --value_size=100 \
  --group_commit_window_us=100 \
  --group_commit_target_size=8 \
  --adaptive_group_commit=1 \
  --db=/tmp/storage_engine_bench_concurrent_fillseq
```

Useful write-path tuning flags:

- `--group_commit_window_us=100`
- `--group_commit_target_size=8`
- `--adaptive_group_commit=1`
- `--high_pressure_group_commit_window_us=200`
- `--high_pressure_group_commit_target_size=16`
- `--high_pressure_group_commit_queue_threshold=10`
- `--uring_sqpoll=0`

## API Sketch

```cpp
#include "storage_engine/db.h"

auto dbResult = storage_engine::DB::Open("/tmp/example_db");
if (!dbResult) {
  return dbResult.error();
}

auto db = std::move(dbResult).value();
auto status = db->Put("alpha", "one");
if (!status.ok()) {
  return status;
}

auto value = db->Get("alpha");
if (value) {
  // value.value() == "one"
}
```

## Repository Layout

```text
include/storage_engine/   Public API, Status/Result, coroutine Task
src/                      DB, WAL, and raw io_uring implementation
benchmarks/               RocksDB-style benchmark driver
tests/                    Unit and smoke coverage
docs/superpowers/specs/   Design notes and v0 planning context
```

## Reliability Notes

The engine uses explicit-offset WAL writes. A successful write requires:

1. WAL record encoding.
2. `io_uring` `writev` completion with the expected byte count.
3. `io_uring` `fdatasync` completion.
4. Memtable apply.

If a process restarts after a torn or incomplete trailing WAL record, recovery
keeps complete records and truncates the tail. A checksum mismatch in the middle
of the WAL is treated as corruption.

The reliability contract depends on the filesystem and storage device honoring
Linux `fdatasync` semantics.
