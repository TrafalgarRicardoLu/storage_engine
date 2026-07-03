#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "storage_engine/status.h"
#include "storage_engine/task.h"

namespace storage_engine {

namespace io {
class UringExecutor;
}

namespace wal {
struct EncodedBatchFragments;
}

namespace internal {
class MemTable;
struct WriteGroupScratch;
}  // namespace internal

class WriteBatch {
 public:
  using ArenaString = std::pmr::string;

  enum class Type {
    kPut = 1,
    kDelete = 2,
  };

  struct Entry {
    Type type;
    ArenaString key;
    ArenaString value;
  };

  WriteBatch();
  WriteBatch(const WriteBatch &other);
  WriteBatch &operator=(const WriteBatch &other);

  void Put(std::string_view key, std::string_view value);
  void Delete(std::string_view key);

  bool empty() const { return entries_.empty(); }
  const std::pmr::vector<Entry> &entries() const { return entries_; }

 private:
  static constexpr size_t kInlineArenaSize = 512;

  void appendEntry(const Entry &entry);

  std::array<std::byte, kInlineArenaSize> inlineArena_{};
  std::pmr::monotonic_buffer_resource arena_;
  std::pmr::vector<Entry> entries_;
};

class DB {
 public:
  struct Options {
    bool uringSqPoll{false};
    uint32_t uringEntries{8};
    uint32_t uringSqPollIdleMs{2000};
    uint64_t groupCommitWindowMicros{100};
    size_t groupCommitTargetSize{8};
    bool adaptiveGroupCommit{true};
    uint64_t highPressureGroupCommitWindowMicros{200};
    size_t highPressureGroupCommitTargetSize{16};
    size_t highPressureGroupCommitQueueThreshold{10};
  };

  struct DebugStats {
    uint64_t uringExecutorCreations{0};
    bool uringSqPollEnabled{false};
    uint64_t asyncWriterSuspensions{0};
    uint64_t groupCommitWaits{0};
    uint64_t groupCommitWindowMicros{0};
    size_t groupCommitTargetSize{0};
    bool adaptiveGroupCommitEnabled{false};
    uint64_t adaptiveGroupCommitWaits{0};
    uint64_t highPressureGroupCommitWindowMicros{0};
    size_t highPressureGroupCommitTargetSize{0};
    size_t highPressureGroupCommitQueueThreshold{0};
    uint64_t writeGroups{0};
    uint64_t maxWriteGroupSize{0};
    uint64_t uringCompletionLoopCompletions{0};
    uint64_t writerThreadDrains{0};
    uint64_t inlineWriterDrains{0};
    uint64_t memtableApplyLocks{0};
    uint64_t walEncodeBufferReuses{0};
    size_t walEncodeFixedCapacity{0};
    size_t walEncodeIovecCapacity{0};
    uint64_t writeGroupTimingSamples{0};
    uint64_t writeGroupTotalMicros{0};
    uint64_t writeGroupWalEncodeMicros{0};
    uint64_t writeGroupDurableWaitMicros{0};
    uint64_t writeGroupMemtableApplyMicros{0};
    uint64_t writerResumeMicros{0};
    size_t memtableReservedBuckets{0};
    size_t memtableShardCount{0};
    size_t memtableEntries{0};
    uint64_t memtableApplyShardLocks{0};
    uint64_t writeGroupScratchReuses{0};
  };

  static Result<std::unique_ptr<DB>> Open(std::string path);
  static Result<std::unique_ptr<DB>> Open(std::string path, Options options);

  DB(const DB &) = delete;
  DB &operator=(const DB &) = delete;
  ~DB();

  Task<Status> PutAsync(std::string_view key, std::string_view value);
  Task<Status> DeleteAsync(std::string_view key);
  Task<Status> WriteAsync(WriteBatch batch);
  Task<Result<std::string>> GetAsync(std::string_view key);

  Status Put(std::string_view key, std::string_view value);
  Status Delete(std::string_view key);
  Status Write(const WriteBatch &batch);
  Result<std::string> Get(std::string_view key);
  DebugStats DebugStatsForTest() const;

 private:
  struct WriteAwaiter;

  static constexpr size_t kCacheLineSize = 64;

  struct alignas(kCacheLineSize) Writer {
    const WriteBatch *batch;
    Status status;
    std::coroutine_handle<> continuation;
  };

  DB(int walFd, std::unique_ptr<io::UringExecutor> executor, Options options);

  Status recover();
  bool enqueueAsyncWriter(Writer *writer);
  void writerLoop();
  Status writeGroup(const std::vector<Writer *> &writers);

  int walFd_{-1};
  std::unique_ptr<io::UringExecutor> executor_;
  uint64_t walOffset_{0};
  uint64_t nextSequence_{1};
  uint64_t uringExecutorCreations_{0};
  uint64_t asyncWriterSuspensions_{0};
  uint64_t groupCommitWaits_{0};
  uint64_t groupCommitWindowMicros_{100};
  size_t groupCommitTargetSize_{8};
  bool adaptiveGroupCommit_{true};
  uint64_t adaptiveGroupCommitWaits_{0};
  uint64_t highPressureGroupCommitWindowMicros_{200};
  size_t highPressureGroupCommitTargetSize_{16};
  size_t highPressureGroupCommitQueueThreshold_{10};
  uint64_t writeGroups_{0};
  uint64_t maxWriteGroupSize_{0};
  uint64_t writerThreadDrains_{0};
  uint64_t inlineWriterDrains_{0};
  uint64_t walEncodeBufferReuses_{0};
  uint64_t writeGroupTimingSamples_{0};
  uint64_t writeGroupTotalMicros_{0};
  uint64_t writeGroupWalEncodeMicros_{0};
  uint64_t writeGroupDurableWaitMicros_{0};
  uint64_t writeGroupMemtableApplyMicros_{0};
  uint64_t writerResumeMicros_{0};
  std::atomic<uint64_t> writeGroupScratchReuses_{0};

  mutable std::mutex writeMutex_;
  std::condition_variable writerCv_;
  std::deque<Writer *> writers_;
  bool writing_{false};
  bool groupCommitArmed_{false};
  bool stopWriter_{false};
  std::thread writerThread_;

  std::unique_ptr<internal::MemTable> memtable_;

  std::unique_ptr<wal::EncodedBatchFragments> walRecord_;
  std::unique_ptr<internal::WriteGroupScratch> writeScratch_;
  std::vector<std::coroutine_handle<>> continuationsScratch_;
};

}  // namespace storage_engine
