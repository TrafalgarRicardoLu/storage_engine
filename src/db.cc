#include "storage_engine/db.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <span>

#include "io/uring_executor.h"
#include "wal.h"

namespace storage_engine {
namespace {

std::string errnoMessage(std::string_view operation) { return std::string(operation) + ": " + std::strerror(errno); }

Result<uint64_t> fileSize(int fd) {
  struct stat st {};
  if (fstat(fd, &st) != 0) {
    return Status::IoError(errnoMessage("fstat"));
  }
  return static_cast<uint64_t>(st.st_size);
}

}  // namespace

void WriteBatch::Put(std::string_view key, std::string_view value) {
  entries_.push_back(Entry{
      .type = Type::kPut,
      .key = std::string(key),
      .value = std::string(value),
  });
}

void WriteBatch::Delete(std::string_view key) {
  entries_.push_back(Entry{
      .type = Type::kDelete,
      .key = std::string(key),
      .value = {},
  });
}

Result<std::unique_ptr<DB>> DB::Open(std::string path) {
  std::filesystem::create_directories(path);
  auto walPath = (std::filesystem::path(path) / "wal.log").string();
  auto fd = open(walPath.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    return Status::IoError(errnoMessage("open WAL"));
  }

  std::unique_ptr<DB> db(new DB(std::move(path), fd));
  auto status = db->recover();
  if (!status.ok()) {
    return status;
  }
  return db;
}

DB::DB(std::string path, int walFd)
    : path_(std::move(path)),
      walPath_((std::filesystem::path(path_) / "wal.log").string()),
      walFd_(walFd) {}

DB::~DB() {
  if (walFd_ >= 0) {
    close(walFd_);
  }
}

Task<Status> DB::PutAsync(std::string_view key, std::string_view value) {
  WriteBatch batch;
  batch.Put(key, value);
  co_return Write(batch);
}

Task<Status> DB::DeleteAsync(std::string_view key) {
  WriteBatch batch;
  batch.Delete(key);
  co_return Write(batch);
}

Task<Status> DB::WriteAsync(WriteBatch batch) { co_return Write(batch); }

Task<Result<std::string>> DB::GetAsync(std::string_view key) { co_return Get(key); }

Status DB::Put(std::string_view key, std::string_view value) { return PutAsync(key, value).run(); }

Status DB::Delete(std::string_view key) { return DeleteAsync(key).run(); }

Status DB::Write(const WriteBatch &batch) {
  if (batch.empty()) {
    return Status::Ok();
  }

  Writer writer{
      .batch = &batch,
      .status = Status::Ok(),
  };

  std::unique_lock lock(writeMutex_);
  writers_.push_back(&writer);

  while (!writer.done && (writing_ || writers_.front() != &writer)) {
    writerCv_.wait(lock);
  }
  if (writer.done) {
    return writer.status;
  }
  writing_ = true;

  std::vector<Writer *> group;
  while (!writers_.empty()) {
    group.push_back(writers_.front());
    writers_.pop_front();
  }
  lock.unlock();

  auto status = writeGroup(group);

  lock.lock();
  for (auto *groupWriter : group) {
    groupWriter->status = status;
    groupWriter->done = true;
  }
  writing_ = false;
  lock.unlock();
  writerCv_.notify_all();
  return writer.status;
}

Result<std::string> DB::Get(std::string_view key) {
  std::lock_guard lock(memMutex_);
  auto iter = memtable_.find(std::string(key));
  if (iter == memtable_.end() || iter->second.deleted) {
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

  auto executor = io::UringExecutor::Create();
  if (!executor) {
    return executor.error();
  }

  std::vector<std::byte> bytes(walOffset_);
  auto read = executor.value().ReadAt(walFd_, std::span<std::byte>(bytes), 0).run();
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
    auto sync = executor.value().FDataSync(walFd_).run();
    if (!sync.ok()) {
      return sync;
    }
  }
  return Status::Ok();
}

Status DB::writeGroup(const std::vector<Writer *> &writers) {
  std::vector<const WriteBatch *> batches;
  batches.reserve(writers.size());
  uint64_t entryCount = 0;
  for (auto *writer : writers) {
    batches.push_back(writer->batch);
    entryCount += writer->batch->entries().size();
  }

  auto baseSequence = nextSequence_;
  auto record = wal::EncodeBatch(baseSequence, batches);
  iovec iov{
      .iov_base = record.data(),
      .iov_len = record.size(),
  };

  auto executor = io::UringExecutor::Create();
  if (!executor) {
    return executor.error();
  }

  auto write = executor.value().WritevAt(walFd_, std::span<const iovec>(&iov, 1), walOffset_, record.size()).run();
  if (!write.ok()) {
    return write;
  }
  auto sync = executor.value().FDataSync(walFd_).run();
  if (!sync.ok()) {
    return sync;
  }

  uint64_t sequence = baseSequence;
  for (auto *writer : writers) {
    applyBatch(*writer->batch, sequence);
    sequence += writer->batch->entries().size();
  }

  walOffset_ += record.size();
  nextSequence_ += entryCount;
  return Status::Ok();
}

void DB::applyBatch(const WriteBatch &batch, uint64_t baseSequence) {
  std::lock_guard lock(memMutex_);
  uint64_t sequence = baseSequence;
  for (const auto &entry : batch.entries()) {
    auto &slot = memtable_[entry.key];
    slot.sequence = sequence++;
    slot.deleted = entry.type == WriteBatch::Type::kDelete;
    slot.value = entry.value;
  }
}

}  // namespace storage_engine
