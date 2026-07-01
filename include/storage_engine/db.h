#pragma once

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "storage_engine/status.h"
#include "storage_engine/task.h"

namespace storage_engine {

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
  static Result<std::unique_ptr<DB>> Open(std::string path);

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

 private:
  struct MemEntry {
    uint64_t sequence{0};
    bool deleted{false};
    std::string value;
  };

  struct Writer {
    const WriteBatch *batch;
    bool done{false};
    Status status;
  };

  DB(std::string path, int walFd);

  Status recover();
  Status writeGroup(const std::vector<Writer *> &writers);
  void applyBatch(const WriteBatch &batch, uint64_t baseSequence);

  std::string path_;
  std::string walPath_;
  int walFd_{-1};
  uint64_t walOffset_{0};
  uint64_t nextSequence_{1};

  std::mutex writeMutex_;
  std::condition_variable writerCv_;
  std::deque<Writer *> writers_;
  bool writing_{false};

  std::mutex memMutex_;
  std::unordered_map<std::string, MemEntry> memtable_;
};

}  // namespace storage_engine
