#include "storage_engine/db.h"

#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <span>

#include "io/uring_executor.h"
#include "wal.h"

namespace storage_engine {
namespace {

constexpr size_t kInitialMemtableReserve = 4096;

std::string errnoMessage(std::string_view operation) { return std::string(operation) + ": " + std::strerror(errno); }

uint64_t elapsedMicros(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end) {
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  return micros > 0 ? static_cast<uint64_t>(micros) : 1;
}

Result<uint64_t> fileSize(int fd) {
  struct stat st {};
  if (fstat(fd, &st) != 0) {
    return Status::IoError(errnoMessage("fstat"));
  }
  return static_cast<uint64_t>(st.st_size);
}

}  // namespace

WriteBatch::WriteBatch()
    : arena_(inlineArena_.data(), inlineArena_.size()),
      entries_(&arena_) {}

WriteBatch::WriteBatch(const WriteBatch &other)
    : WriteBatch() {
  for (const auto &entry : other.entries_) {
    appendEntry(entry);
  }
}

WriteBatch &WriteBatch::operator=(const WriteBatch &other) {
  if (this != &other) {
    entries_.clear();
    for (const auto &entry : other.entries_) {
      appendEntry(entry);
    }
  }
  return *this;
}

WriteBatch::WriteBatch(WriteBatch &&other)
    : WriteBatch() {
  for (const auto &entry : other.entries_) {
    appendEntry(entry);
  }
  other.entries_.clear();
}

WriteBatch &WriteBatch::operator=(WriteBatch &&other) {
  if (this != &other) {
    entries_.clear();
    for (const auto &entry : other.entries_) {
      appendEntry(entry);
    }
    other.entries_.clear();
  }
  return *this;
}

void WriteBatch::Put(std::string_view key, std::string_view value) {
  entries_.push_back(Entry{
      .type = Type::kPut,
      .key = ArenaString(key, &arena_),
      .value = ArenaString(value, &arena_),
  });
}

void WriteBatch::Delete(std::string_view key) {
  entries_.push_back(Entry{
      .type = Type::kDelete,
      .key = ArenaString(key, &arena_),
      .value = ArenaString(&arena_),
  });
}

void WriteBatch::appendEntry(const Entry &entry) {
  entries_.push_back(Entry{
      .type = entry.type,
      .key = ArenaString(std::string_view(entry.key.data(), entry.key.size()), &arena_),
      .value = ArenaString(std::string_view(entry.value.data(), entry.value.size()), &arena_),
  });
}

Result<std::unique_ptr<DB>> DB::Open(std::string path) { return Open(std::move(path), Options{}); }

Result<std::unique_ptr<DB>> DB::Open(std::string path, Options options) {
  std::filesystem::create_directories(path);
  auto walPath = (std::filesystem::path(path) / "wal.log").string();
  auto fd = open(walPath.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    return Status::IoError(errnoMessage("open WAL"));
  }

  auto executor = io::UringExecutor::Create(io::UringExecutor::Options{
      .entries = options.uringEntries,
      .sqPoll = options.uringSqPoll,
      .sqPollIdleMs = options.uringSqPollIdleMs,
  });
  if (!executor) {
    close(fd);
    return executor.error();
  }

  std::unique_ptr<DB> db(new DB(fd, std::make_unique<io::UringExecutor>(std::move(executor).value()), options));
  db->uringExecutorCreations_ = 1;
  auto status = db->recover();
  if (!status.ok()) {
    return status;
  }
  return db;
}

DB::DB(int walFd, std::unique_ptr<io::UringExecutor> executor, Options options)
    : walFd_(walFd),
      executor_(std::move(executor)),
      groupCommitWindowMicros_(options.groupCommitWindowMicros),
      groupCommitTargetSize_(options.groupCommitTargetSize),
      adaptiveGroupCommit_(options.adaptiveGroupCommit),
      highPressureGroupCommitWindowMicros_(options.highPressureGroupCommitWindowMicros),
      highPressureGroupCommitTargetSize_(options.highPressureGroupCommitTargetSize),
      highPressureGroupCommitQueueThreshold_(options.highPressureGroupCommitQueueThreshold),
      walRecord_(std::make_unique<wal::EncodedBatchFragments>()) {
  for (auto &shard : memtableShards_) {
    shard.entries.reserve(kInitialMemtableReserve / kMemtableShardCount);
  }
  writerThread_ = std::thread(&DB::writerLoop, this);
}

DB::~DB() {
  {
    std::lock_guard lock(writeMutex_);
    stopWriter_ = true;
  }
  writerCv_.notify_all();
  if (writerThread_.joinable()) {
    writerThread_.join();
  }
  if (walFd_ >= 0) {
    close(walFd_);
  }
}

struct DB::WriteAwaiter {
  DB *db;
  WriteBatch batch;
  Writer writer;

  WriteAwaiter(DB *db, WriteBatch batch)
      : db(db),
        batch(std::move(batch)),
        writer(Writer{
            .batch = &this->batch,
            .status = Status::Ok(),
            .continuation = {},
        }) {}

  bool await_ready() const { return batch.empty(); }
  bool await_suspend(std::coroutine_handle<> continuation) {
    writer.continuation = continuation;
    return db->enqueueAsyncWriter(&writer);
  }
  Status await_resume() { return writer.status; }
};

Task<Status> DB::PutAsync(std::string_view key, std::string_view value) {
  WriteBatch batch;
  batch.Put(key, value);
  co_return co_await WriteAwaiter(this, std::move(batch));
}

Task<Status> DB::DeleteAsync(std::string_view key) {
  WriteBatch batch;
  batch.Delete(key);
  co_return co_await WriteAwaiter(this, std::move(batch));
}

Task<Status> DB::WriteAsync(WriteBatch batch) { co_return co_await WriteAwaiter(this, std::move(batch)); }

Task<Result<std::string>> DB::GetAsync(std::string_view key) { co_return Get(key); }

Status DB::Put(std::string_view key, std::string_view value) { return PutAsync(key, value).run(); }

Status DB::Delete(std::string_view key) { return DeleteAsync(key).run(); }

Status DB::Write(const WriteBatch &batch) {
  WriteBatch copy = batch;
  return WriteAsync(std::move(copy)).run();
}

DB::DebugStats DB::DebugStatsForTest() const {
  DebugStats stats;
  auto executorStats = executor_->DebugStatsForTest();
  {
    std::lock_guard lock(writeMutex_);
    stats.uringExecutorCreations = uringExecutorCreations_;
    stats.uringSqPollEnabled = executorStats.sqPollEnabled;
    stats.asyncWriterSuspensions = asyncWriterSuspensions_;
    stats.groupCommitWaits = groupCommitWaits_;
    stats.groupCommitWindowMicros = groupCommitWindowMicros_;
    stats.groupCommitTargetSize = groupCommitTargetSize_;
    stats.adaptiveGroupCommitEnabled = adaptiveGroupCommit_;
    stats.adaptiveGroupCommitWaits = adaptiveGroupCommitWaits_;
    stats.highPressureGroupCommitWindowMicros = highPressureGroupCommitWindowMicros_;
    stats.highPressureGroupCommitTargetSize = highPressureGroupCommitTargetSize_;
    stats.highPressureGroupCommitQueueThreshold = highPressureGroupCommitQueueThreshold_;
    stats.writeGroups = writeGroups_;
    stats.maxWriteGroupSize = maxWriteGroupSize_;
    stats.writerThreadDrains = writerThreadDrains_;
    stats.inlineWriterDrains = inlineWriterDrains_;
    stats.walEncodeBufferReuses = walEncodeBufferReuses_;
    stats.walEncodeFixedCapacity = walRecord_->fixed.capacity();
    stats.walEncodeIovecCapacity = walRecord_->iovecs.capacity();
    stats.writeGroupTimingSamples = writeGroupTimingSamples_;
    stats.writeGroupTotalMicros = writeGroupTotalMicros_;
    stats.writeGroupWalEncodeMicros = writeGroupWalEncodeMicros_;
    stats.writeGroupDurableWaitMicros = writeGroupDurableWaitMicros_;
    stats.writeGroupMemtableApplyMicros = writeGroupMemtableApplyMicros_;
    stats.writerResumeMicros = writerResumeMicros_;
    stats.writeGroupScratchReuses = writeGroupScratchReuses_.load(std::memory_order_relaxed);
  }
  stats.memtableShardCount = memtableShards_.size();
  for (const auto &shard : memtableShards_) {
    std::shared_lock lock(shard.mutex);
    stats.memtableReservedBuckets += shard.entries.bucket_count();
    stats.memtableEntries += shard.entries.size();
  }
  stats.memtableApplyLocks = memtableApplyLocks_.load(std::memory_order_relaxed);
  stats.memtableApplyShardLocks = memtableApplyShardLocks_.load(std::memory_order_relaxed);
  stats.uringCompletionLoopCompletions = executorStats.completionLoopCompletions;
  return stats;
}

bool DB::enqueueAsyncWriter(Writer *writer) {
  {
    std::lock_guard lock(writeMutex_);
    ++asyncWriterSuspensions_;
    if (!writing_ && writers_.empty() && !groupCommitArmed_) {
      writing_ = true;
      ++inlineWriterDrains_;
    } else {
      writers_.push_back(writer);
      if (writing_ || writers_.size() > 1) {
        groupCommitArmed_ = true;
      }
      writerCv_.notify_one();
      return true;
    }
  }

  std::vector<Writer *> group{writer};
  auto status = writeGroup(group);

  {
    std::lock_guard lock(writeMutex_);
    ++writeGroups_;
    maxWriteGroupSize_ = std::max(maxWriteGroupSize_, uint64_t{1});
    writer->status = status;
    writing_ = false;
  }
  writerCv_.notify_one();
  return false;
}

void DB::writerLoop() {
  for (;;) {
    std::vector<Writer *> group;
    {
      std::unique_lock lock(writeMutex_);
      writerCv_.wait(lock, [this] { return stopWriter_ || (!writing_ && !writers_.empty()); });
      if (stopWriter_ && writers_.empty()) {
        return;
      }
      writing_ = true;
      if (groupCommitArmed_ || writers_.size() > 1) {
        groupCommitArmed_ = false;
        ++groupCommitWaits_;
        auto waitMicros = groupCommitWindowMicros_;
        auto targetSize = groupCommitTargetSize_;
        if (adaptiveGroupCommit_ && writers_.size() >= highPressureGroupCommitQueueThreshold_) {
          waitMicros = highPressureGroupCommitWindowMicros_;
          targetSize = highPressureGroupCommitTargetSize_;
          ++adaptiveGroupCommitWaits_;
        }
        writerCv_.wait_for(lock, std::chrono::microseconds(waitMicros), [this, targetSize] {
          return stopWriter_ || writers_.size() >= targetSize;
        });
      }
      while (!writers_.empty()) {
        group.push_back(writers_.front());
        writers_.pop_front();
      }
      ++writerThreadDrains_;
    }

    auto status = writeGroup(group);

    continuationsScratch_.clear();
    {
      std::lock_guard lock(writeMutex_);
      ++writeGroups_;
      maxWriteGroupSize_ = std::max(maxWriteGroupSize_, static_cast<uint64_t>(group.size()));
      for (auto *groupWriter : group) {
        groupWriter->status = status;
        if (groupWriter->continuation) {
          continuationsScratch_.push_back(groupWriter->continuation);
        }
      }
      if (group.size() > 1) {
        groupCommitArmed_ = true;
      }
      writing_ = false;
    }
    writerCv_.notify_one();

    auto resumeStart = std::chrono::steady_clock::now();
    for (auto continuation : continuationsScratch_) {
      continuation.resume();
    }
    auto resumeEnd = std::chrono::steady_clock::now();
    if (!continuationsScratch_.empty()) {
      std::lock_guard lock(writeMutex_);
      writerResumeMicros_ += elapsedMicros(resumeStart, resumeEnd);
    }
  }
}

Result<std::string> DB::Get(std::string_view key) {
  auto &shard = memtableShards_[memtableShard(key)];
  std::shared_lock lock(shard.mutex);
  auto iter = shard.entries.find(key);
  if (iter == shard.entries.end() || iter->second.deleted) {
    return Status::NotFound("key not found");
  }
  return iter->second.value;
}

Status DB::recover() {
  auto size = fileSize(walFd_);
  if (!size) {
    return size.error();
  }
  walOffset_ = size.value();
  if (walOffset_ == 0) {
    return Status::Ok();
  }

  std::vector<std::byte> bytes(walOffset_);
  auto read = executor_->ReadAt(walFd_, std::span<std::byte>(bytes), 0).run();
  if (!read) {
    return read.error();
  }
  bytes.resize(read.value());

  auto decoded = wal::DecodeLog(std::span<const std::byte>(bytes));
  if (!decoded) {
    return decoded.error();
  }

  uint64_t maxSequence = 0;
  for (const auto &batch : decoded.value().batches) {
    applyBatch(batch.batch, batch.baseSequence);
    maxSequence = std::max(maxSequence, batch.baseSequence + batch.batch.entries().size());
  }
  nextSequence_ = std::max(nextSequence_, maxSequence + 1);
  walOffset_ = decoded.value().validBytes;
  if (walOffset_ != bytes.size()) {
    if (ftruncate(walFd_, static_cast<off_t>(walOffset_)) != 0) {
      return Status::IoError(errnoMessage("truncate WAL"));
    }
    auto sync = executor_->FDataSync(walFd_).run();
    if (!sync.ok()) {
      return sync;
    }
  }
  return Status::Ok();
}

Status DB::writeGroup(const std::vector<Writer *> &writers) {
  auto groupStart = std::chrono::steady_clock::now();
  if (writeScratch_.batches.capacity() > 0) {
    writeGroupScratchReuses_.fetch_add(1, std::memory_order_relaxed);
  }
  writeScratch_.batches.clear();
  writeScratch_.batches.reserve(writers.size());
  uint64_t entryCount = 0;
  for (auto *writer : writers) {
    writeScratch_.batches.push_back(writer->batch);
    entryCount += writer->batch->entries().size();
  }

  auto baseSequence = nextSequence_;
  auto encodeStart = std::chrono::steady_clock::now();
  if (walRecord_->fixed.capacity() > 0 || walRecord_->iovecs.capacity() > 0) {
    ++walEncodeBufferReuses_;
  }
  wal::EncodeBatchFragmentsInto(baseSequence, writeScratch_.batches, *walRecord_);
  std::span<const iovec> iovecs(walRecord_->iovecs);
  if (walRecord_->iovecs.size() > IOV_MAX) {
    wal::EncodeBatchInto(baseSequence, writeScratch_.batches, writeScratch_.fallbackRecord);
    walRecord_->iovecs.clear();
    walRecord_->iovecs.push_back(iovec{
        .iov_base = writeScratch_.fallbackRecord.data(),
        .iov_len = writeScratch_.fallbackRecord.size(),
    });
    walRecord_->size = writeScratch_.fallbackRecord.size();
    iovecs = std::span<const iovec>(walRecord_->iovecs);
  }
  auto encodeEnd = std::chrono::steady_clock::now();

  auto durableStart = std::chrono::steady_clock::now();
  auto syncWrite = executor_->WritevAndFDataSync(walFd_, iovecs, walOffset_, walRecord_->size).run();
  auto durableEnd = std::chrono::steady_clock::now();
  if (!syncWrite.ok()) {
    return syncWrite;
  }

  auto applyStart = std::chrono::steady_clock::now();
  applyBatches(writers, baseSequence);
  auto applyEnd = std::chrono::steady_clock::now();

  walOffset_ += walRecord_->size;
  nextSequence_ += entryCount;
  auto groupEnd = std::chrono::steady_clock::now();
  {
    std::lock_guard lock(writeMutex_);
    ++writeGroupTimingSamples_;
    writeGroupTotalMicros_ += elapsedMicros(groupStart, groupEnd);
    writeGroupWalEncodeMicros_ += elapsedMicros(encodeStart, encodeEnd);
    writeGroupDurableWaitMicros_ += elapsedMicros(durableStart, durableEnd);
    writeGroupMemtableApplyMicros_ += elapsedMicros(applyStart, applyEnd);
  }
  return Status::Ok();
}

void DB::applyBatch(const WriteBatch &batch, uint64_t baseSequence) {
  Writer writer{
      .batch = &batch,
      .status = Status::Ok(),
      .continuation = {},
  };
  std::vector<Writer *> writers{&writer};
  applyBatches(writers, baseSequence);
}

void DB::applyBatches(const std::vector<Writer *> &writers, uint64_t baseSequence) {
  memtableApplyLocks_.fetch_add(1, std::memory_order_relaxed);
  auto sequence = baseSequence;
  for (auto *writer : writers) {
    for (const auto &entry : writer->batch->entries()) {
      auto key = std::string_view(entry.key.data(), entry.key.size());
      writeScratch_.memtableUpdates[memtableShard(key)].push_back(PendingMemtableUpdate{
          .entry = &entry,
          .sequence = sequence++,
      });
    }
  }

  for (size_t shardIndex = 0; shardIndex < writeScratch_.memtableUpdates.size(); ++shardIndex) {
    auto &updates = writeScratch_.memtableUpdates[shardIndex];
    if (updates.empty()) {
      continue;
    }
    auto &shard = memtableShards_[shardIndex];
    std::unique_lock lock(shard.mutex);
    memtableApplyShardLocks_.fetch_add(1, std::memory_order_relaxed);
    for (const auto &update : updates) {
      auto &entry = *update.entry;
      auto key = std::string_view(entry.key.data(), entry.key.size());
      auto iter = shard.entries.try_emplace(std::string(key)).first;
      auto &slot = iter->second;
      slot.sequence = update.sequence;
      slot.deleted = entry.type == WriteBatch::Type::kDelete;
      slot.value.assign(entry.value.data(), entry.value.size());
    }
    updates.clear();
  }
}

size_t DB::memtableShard(std::string_view key) const { return TransparentStringHash{}(key) % kMemtableShardCount; }

}  // namespace storage_engine
