#pragma once

#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
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

class WriteBatch {
 public:
  enum class Type {
    kPut = 1,
    kDelete = 2,
  };

  struct Entry {
    Type type;
    std::string key;
    std::string value;
  };

  void Put(std::string_view key, std::string_view value);
  void Delete(std::string_view key);

  bool empty() const { return entries_.empty(); }
  const std::vector<Entry> &entries() const { return entries_; }

 private:
  std::vector<Entry> entries_;
};

class DB {
 public:
  struct Options {
    bool uringSqPoll{false};
    uint32_t uringEntries{8};
    uint32_t uringSqPollIdleMs{2000};
    uint64_t groupCommitWindowMicros{100};
    size_t groupCommitTargetSize{8};
  };

  struct DebugStats {
    uint64_t uringExecutorCreations{0};
    bool uringSqPollEnabled{false};
    uint64_t asyncWriterSuspensions{0};
    uint64_t groupCommitWaits{0};
    uint64_t groupCommitWindowMicros{0};
    size_t groupCommitTargetSize{0};
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
  struct MemEntry {
    uint64_t sequence{0};
    bool deleted{false};
    std::string value;
  };

  struct TransparentStringHash {
    using is_transparent = void;
    size_t operator()(std::string_view value) const noexcept { return std::hash<std::string_view>{}(value); }
    size_t operator()(const std::string &value) const noexcept { return std::hash<std::string_view>{}(value); }
  };

  struct TransparentStringEqual {
    using is_transparent = void;
    bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return lhs == rhs; }
  };

  struct WriteAwaiter;

  struct Writer {
    const WriteBatch *batch;
    bool done{false};
    Status status;
    std::coroutine_handle<> continuation;
  };

  DB(std::string path, int walFd, std::unique_ptr<io::UringExecutor> executor, Options options);

  Status recover();
  bool enqueueAsyncWriter(Writer *writer);
  void writerLoop();
  Status writeGroup(const std::vector<Writer *> &writers);
  void applyBatch(const WriteBatch &batch, uint64_t baseSequence);
  void applyBatches(const std::vector<Writer *> &writers, uint64_t baseSequence);
  void applyBatchLocked(const WriteBatch &batch, uint64_t &sequence);

  std::string path_;
  std::string walPath_;
  int walFd_{-1};
  std::unique_ptr<io::UringExecutor> executor_;
  uint64_t walOffset_{0};
  uint64_t nextSequence_{1};
  uint64_t uringExecutorCreations_{0};
  uint64_t asyncWriterSuspensions_{0};
  uint64_t groupCommitWaits_{0};
  uint64_t groupCommitWindowMicros_{100};
  size_t groupCommitTargetSize_{8};
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

  mutable std::mutex writeMutex_;
  std::condition_variable writerCv_;
  std::deque<Writer *> writers_;
  bool writing_{false};
  bool groupCommitArmed_{false};
  bool stopWriter_{false};
  std::thread writerThread_;

  mutable std::shared_mutex memMutex_;
  uint64_t memtableApplyLocks_{0};
  std::unordered_map<std::string, MemEntry, TransparentStringHash, TransparentStringEqual> memtable_;

  std::unique_ptr<wal::EncodedBatchFragments> walRecord_;
  std::vector<std::byte> walFallbackRecord_;
  std::vector<const WriteBatch *> walBatchScratch_;
};

}  // namespace storage_engine
