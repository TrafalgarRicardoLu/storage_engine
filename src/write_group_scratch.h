#pragma once

#include <cstddef>
#include <vector>

#include "memtable.h"
#include "storage_engine/db.h"

namespace storage_engine::internal {

struct WriteGroupScratch {
  std::vector<const WriteBatch *> batches;
  MemTable::PendingUpdates memtableUpdates;
  std::vector<std::byte> fallbackRecord;
};

}  // namespace storage_engine::internal
