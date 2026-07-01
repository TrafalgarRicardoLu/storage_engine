# Storage Engine v0 Design

Date: 2026-07-01

## 1. Goal

Build a compact, high-performance, single-node embedded key-value storage
engine in C++20.

The v0 engine is an ordered KV store based on a simplified LSM tree. It uses
C++20 coroutines for asynchronous control flow and Linux `io_uring` for file
I/O. The reliability contract is strict: once a write API returns success, the
data must survive process crash, machine reboot, and power loss, assuming the
underlying filesystem and storage device honor `fsync`/`fdatasync` correctly.

## 2. Non-Goals

v0 intentionally excludes:

- SQL or document APIs.
- Distributed replication.
- Multi-process concurrent open of the same database directory.
- Full transactions and MVCC snapshots.
- Multiple column families.
- TTL, merge operators, secondary indexes, and compression.
- Multi-level RocksDB-style compaction tuning.
- `io_uring` advanced features such as SQPOLL, IOPOLL, fixed buffers, and
  registered files.

These can be added later only after the reliability and data-path core is
stable.

## 3. Public API

The engine exposes asynchronous APIs first. Synchronous APIs are thin wrappers
around the asynchronous path, so there is only one write implementation.

```cpp
class DB {
 public:
  Task<Status> PutAsync(std::string_view key, std::string_view value);
  Task<Status> DeleteAsync(std::string_view key);
  Task<Status> WriteAsync(WriteBatch batch);
  Task<Result<std::string>> GetAsync(std::string_view key);
  Task<std::unique_ptr<Iterator>> NewIteratorAsync();

  Status Put(std::string_view key, std::string_view value);
  Status Delete(std::string_view key);
  Status Write(WriteBatch batch);
  Result<std::string> Get(std::string_view key);
  std::unique_ptr<Iterator> NewIterator();
};
```

The synchronous methods run the corresponding coroutine to completion on the
engine runtime.

## 4. Architecture

```text
User threads
    |
    v
DBImpl
    |
    +-- WriterQueue / WriteGroup
    |       |
    |       v
    |     WAL writev via io_uring
    |       |
    |       v
    |     WAL fdatasync via io_uring
    |       |
    |       v
    |     MemTable apply
    |
    +-- active MemTable
    +-- immutable MemTable
    +-- SSTable files
    +-- Manifest
    +-- background flush / compaction coroutines
    |
    v
UringExecutor
    |
    v
Linux io_uring
```

Core modules:

- `async/`: minimal `Task<T>`, awaiters, and coroutine runtime glue.
- `io/`: `UringExecutor`, `UringFile`, directory sync helpers.
- `db/`: `DBImpl`, writer queue, recovery, open/close lifecycle.
- `wal/`: WAL record format, encoder, reader, replay.
- `memtable/`: arena-backed skiplist ordered by internal key.
- `table/`: SSTable builder, reader, blocks, footer, table iterator.
- `version/`: manifest edits and current file set.
- `util/`: status, result, coding, CRC, byte utilities.

## 5. 3FS Style Baseline

The implementation should follow the local 3FS checkout at
`/data00/home/lujianhui.1/3FS` as the primary style reference.

Repository-level style:

- Use C++20, CMake, out-of-source builds, and exported compile commands.
- Use the 3FS clang-format baseline: Google-based style, 2-space indentation,
  120-column limit, right-aligned pointers, no bin-packed parameters or
  arguments, and constructor initializers broken before the colon.
- Keep `-Wall -Wextra -Werror -Wpedantic` as the target warning posture once
  the project has a CMake skeleton.
- Enable clang-tidy checks in the same spirit as 3FS: `bugprone`,
  `performance`, `modernize`, `readability`, selected `cert`, selected
  `google`, and selected `cppcoreguidelines` checks.
- Keep changes surgical. Do not apply broad formatting rewrites to unrelated
  files.

Code-level style:

- Use `#pragma once` in headers.
- Use subsystem namespaces rather than a flat global namespace. The v0 namespace
  should be `storage_engine`, with nested namespaces only when they clarify
  module ownership, such as `storage_engine::wal` or `storage_engine::table`.
- Use `PascalCase` for classes and types, `camelCase` for functions and local
  variables, and trailing underscore for private data members.
- Prefer `Status`, `Result<T>`, and coroutine task return types over exceptions
  for expected storage-engine errors.
- Prefer explicit ownership with `std::unique_ptr`, `std::shared_ptr`, and
  move-only operation state. Raw pointers are allowed only for non-owning views
  or C API boundaries such as `liburing`.
- Keep coroutine usage visible at API and I/O boundaries, similar to 3FS
  `CoTask`/`CoTryTask` style, while keeping CPU hot loops ordinary functions.
- Keep module boundaries explicit: headers expose contracts, `.cc` files own
  algorithms and state transitions.

## 6. Data Model

Keys and values are byte strings. The internal key is:

```text
user_key | sequence_number | value_type
```

`value_type` is either value or deletion tombstone. Higher sequence numbers
shadow lower sequence numbers for the same user key.

v0 assumes small values, from tens of bytes to a few KiB. Values are stored
directly in the WAL, MemTable, and SSTable. There is no separate value log in
v0.

## 7. Write Path

The write path uses multi-writer group commit.

1. A caller enters `PutAsync`, `DeleteAsync`, or `WriteAsync`.
2. The write is wrapped as a `WriteBatch` and enqueued into `WriterQueue`.
3. The queue leader collects waiting writers into one write group.
4. The leader assigns a contiguous sequence-number range to the group.
5. The leader encodes the group into WAL records.
6. The leader submits WAL `writev` through `io_uring`.
7. The leader waits for the write CQE and verifies full completion.
8. The leader submits WAL `fdatasync` through `io_uring`.
9. The leader waits for the sync CQE.
10. Only after sync succeeds, the leader applies the group to the MemTable.
11. The leader resumes all follower coroutines with the final group status.

The success point is after WAL sync and MemTable apply. A caller never observes
`Status::OK` before the WAL record is durable.

If any WAL write or sync step fails, the whole write group fails. v0 does not
return partial success for a write group.

## 8. io_uring Rules

v0 depends directly on `liburing` and is Linux-only.

The engine must obey these rules:

- Submitting an SQE is not a successful I/O.
- A successful write CQE is not a durability guarantee.
- A write API can return success only after the WAL `fdatasync` CQE succeeds.
- WAL writes use explicit offsets. The engine must not use implicit shared file
  offsets for asynchronous writes.
- Buffers referenced by SQEs must remain alive until their CQEs are consumed.
- Linked SQEs may be used for WAL `writev -> fdatasync`, but the implementation
  must still inspect completion results and fail the group on short write,
  cancellation, or sync error.
- Directory entries are part of durability. After rename, the parent directory
  must be synced where required.

The v0 `UringExecutor` has one event-loop thread and one `io_uring` instance.
All SQE submission is serialized through that executor for correctness. This is
sufficient for v0 and keeps lifetime management simple.

## 9. Coroutine Usage

Coroutines are used for asynchronous control flow, not for CPU hot loops.

Coroutine-based pieces:

- Public async APIs.
- Writer queue waiting and group completion.
- `UringFile` read, write, and sync operations.
- Background flush and compaction scheduling.

Non-coroutine pieces:

- Skiplist lookup and insertion.
- WAL record encoding and CRC.
- SSTable block parsing.
- Iterator merge inner loops.
- Compaction merge inner loops.

This keeps the data-path algorithms ordinary and testable while still giving
the engine an asynchronous API and direct `io_uring` integration.

## 10. WAL Format

Each WAL record contains:

```text
crc32c      : uint32
length      : uint32
record_type : uint8
payload     : bytes
```

The payload is a serialized `WriteBatch` with:

```text
base_sequence : uint64
entry_count   : uint32
entries       : repeated { type, key, value }
```

Recovery scans records in order, validates length and CRC, and replays complete
records. A torn or incomplete trailing record is ignored or truncated to the
last complete record. A checksum mismatch in the middle of the WAL is a hard
error.

## 11. MemTable

The active MemTable is an arena-backed skiplist sorted by internal key. It
supports:

- Insert value record.
- Insert deletion tombstone.
- Point lookup.
- Forward iteration.

When the active MemTable reaches a configured size, it becomes immutable and a
new active MemTable is installed. A background flush coroutine writes the
immutable MemTable into an L0 SSTable.

## 12. SSTable Format

v0 uses a block-based sorted table:

```text
data block 0
data block 1
...
index block
footer
```

Each data block stores sorted internal-key entries. The index block maps a
separator key to a data block offset and size. The footer points to the index
block and stores a magic value and format version.

Each block includes a checksum. Block compression and Bloom filters are excluded
from v0 to keep the first implementation small. Bloom filters are the first
planned read-performance extension.

SSTable creation is durable only after:

1. Write temporary table file.
2. Sync the table file.
3. Rename the temporary file to its final name.
4. Sync the parent directory.
5. Append and sync the manifest edit that publishes the table.

## 13. Read Path

`Get` checks sources in freshness order:

1. Active MemTable.
2. Immutable MemTable.
3. L0 SSTables from newest to oldest.
4. L1 SSTables by key range.

The first visible record for the user key wins. A deletion tombstone means the
key is not found.

`Iterator` merges MemTables and SSTables by internal key order, suppresses older
versions of the same user key, and hides deletion tombstones.

## 14. Manifest and Versioning

The manifest is an append-only sequence of version edits. It records:

- Next file number.
- Last durable sequence number.
- Added SSTable files.
- Deleted SSTable files.
- Current WAL file information.

On open, the engine replays the manifest to reconstruct the current file set.
If a referenced SSTable is missing or corrupt, open fails. v0 does not attempt
silent repair.

## 15. Recovery

Open recovery:

1. Read and replay the manifest.
2. Validate referenced SSTables.
3. Locate WAL files that may contain unapplied writes.
4. Replay complete WAL records into a new MemTable.
5. Ignore or truncate only an incomplete trailing WAL record.
6. Fail on checksum mismatch in the middle of a WAL file.

After recovery completes, all writes that returned success before the crash must
be readable unless the underlying filesystem or storage device violated sync
semantics.

## 16. Compaction v0

v0 implements only the minimum compaction needed to avoid unbounded L0 growth:

- Flush immutable MemTables to L0 SSTables.
- When L0 file count exceeds a threshold, merge overlapping L0 files into L1.
- Preserve newest sequence for each key and tombstones needed to hide older
  values.

Multi-level compaction, parallel compaction, size-tiered tuning, and tombstone
drop optimization are deferred.

## 17. Testing Plan

Unit tests:

- WAL encode/decode.
- WAL CRC validation.
- WAL torn-tail recovery.
- Internal-key ordering.
- MemTable put, delete, lookup, and iteration.
- SSTable build/read/block checksum.
- Manifest replay.

Integration tests:

- Single-thread put/get/delete.
- Multi-thread writes through group commit.
- Restart after successful writes.
- Restart after partial WAL tail.
- Restart after temporary SSTable exists but is unpublished.
- Restart after manifest edit is missing or partial.
- Iterator ordered output after flush and compaction.

Crash-simulation tests should use fault-injection hooks around:

- WAL write completion.
- WAL sync completion.
- SSTable file sync.
- Rename.
- Directory sync.
- Manifest append and sync.

Benchmarks:

- Sync single write latency.
- Batch write throughput.
- Multi-thread group commit throughput.
- Random read latency.
- Forward range scan throughput.
- Recovery time versus WAL size.

## 18. First Implementation Milestones

1. Build minimal C++20 project, `Status`, `Result`, coding helpers, and tests.
2. Implement minimal coroutine `Task<T>` and `UringExecutor`.
3. Implement `UringFile` write, read, and fdatasync.
4. Implement WAL writer and reader.
5. Implement writer queue and group commit.
6. Implement MemTable.
7. Implement DB open, write, get, and WAL recovery.
8. Implement SSTable builder and reader.
9. Implement flush to L0.
10. Implement manifest.
11. Implement iterator.
12. Implement minimal L0 -> L1 compaction.
13. Add crash-simulation tests and benchmarks.

## 19. Main Risks

- `io_uring` buffer lifetime bugs can corrupt data or crash the process.
- Treating write completion as durability would violate the core reliability
  requirement.
- Manifest and directory sync mistakes can make published SSTables disappear
  after crash.
- Coroutine abstractions can grow too large. v0 should keep the runtime small
  and storage-specific.
- Group commit can create subtle ordering bugs if sequence assignment, WAL
  encoding, and MemTable apply are not handled as one ordered batch.

## 20. Recommended v0 Policy

Choose correctness over raw speed until crash tests pass. The intended first
performance optimization is larger write groups, not weaker sync semantics.

Do not add Bloom filters, compression, value-log separation, SQPOLL, IOPOLL, or
multi-level compaction until the base engine passes recovery and crash tests.
