#include "memtable.h"

#include <mutex>

namespace storage_engine::internal {
namespace {

constexpr size_t kInitialReserve = 4096;

}  // namespace

MemTable::MemTable() {
  for (auto &shard : shards_) {
    shard.entries.reserve(kInitialReserve / kShardCount);
  }
}

Result<std::string> MemTable::Get(std::string_view key) const {
  const auto &shard = shards_[shardFor(key)];
  std::shared_lock lock(shard.mutex);
  auto iter = shard.entries.find(key);
  if (iter == shard.entries.end() || iter->second.deleted) {
    return Status::NotFound("key not found");
  }
  return iter->second.value;
}

void MemTable::ApplyBatches(const std::vector<const WriteBatch *> &batches,
                            uint64_t baseSequence,
                            PendingUpdates &scratch) {
  applyLocks_.fetch_add(1, std::memory_order_relaxed);
  auto sequence = baseSequence;
  for (auto *batch : batches) {
    for (const auto &entry : batch->entries()) {
      auto key = std::string_view(entry.key.data(), entry.key.size());
      scratch[shardFor(key)].push_back(PendingMemtableUpdate{
          .entry = &entry,
          .sequence = sequence++,
      });
    }
  }

  for (size_t shardIndex = 0; shardIndex < scratch.size(); ++shardIndex) {
    auto &updates = scratch[shardIndex];
    if (updates.empty()) {
      continue;
    }
    auto &shard = shards_[shardIndex];
    std::unique_lock lock(shard.mutex);
    applyShardLocks_.fetch_add(1, std::memory_order_relaxed);
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

MemTableStats MemTable::Stats() const {
  MemTableStats stats{
      .applyLocks = applyLocks_.load(std::memory_order_relaxed),
      .applyShardLocks = applyShardLocks_.load(std::memory_order_relaxed),
      .reservedBuckets = 0,
      .shardCount = shards_.size(),
      .entries = 0,
  };
  for (const auto &shard : shards_) {
    std::shared_lock lock(shard.mutex);
    stats.reservedBuckets += shard.entries.bucket_count();
    stats.entries += shard.entries.size();
  }
  return stats;
}

size_t MemTable::shardFor(std::string_view key) const { return TransparentStringHash{}(key) % kShardCount; }

}  // namespace storage_engine::internal
