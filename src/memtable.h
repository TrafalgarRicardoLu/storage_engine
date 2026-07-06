#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "storage_engine/db.h"
#include "storage_engine/status.h"

namespace storage_engine::internal {

struct PendingMemtableUpdate {
  const WriteBatch::Entry *entry{nullptr};
  uint64_t sequence{0};
};

struct MemTableStats {
  uint64_t applyLocks{0};
  uint64_t applyShardLocks{0};
  size_t reservedBuckets{0};
  size_t shardCount{0};
  size_t entries{0};
};

class MemTable {
 public:
  static constexpr size_t kShardCount = 16;
  using PendingUpdates = std::array<std::vector<PendingMemtableUpdate>, kShardCount>;

  MemTable();

  Result<std::string> Get(std::string_view key) const;
  void ApplyBatches(const std::vector<const WriteBatch *> &batches, uint64_t baseSequence, PendingUpdates &scratch);
  MemTableStats Stats() const;

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

  struct Shard {
    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, MemEntry, TransparentStringHash, TransparentStringEqual> entries;
  };

  static constexpr size_t kInitialBucketsPerShard = 256;

  size_t shardFor(std::string_view key) const;

  std::atomic<uint64_t> applyLocks_{0};
  std::atomic<uint64_t> applyShardLocks_{0};
  std::array<Shard, kShardCount> shards_;
};

}  // namespace storage_engine::internal
