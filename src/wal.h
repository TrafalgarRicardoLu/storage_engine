#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "storage_engine/db.h"
#include "storage_engine/status.h"

namespace storage_engine::wal {

struct DecodedBatch {
  uint64_t baseSequence{0};
  WriteBatch batch;
};

struct DecodeResult {
  std::vector<DecodedBatch> batches;
  size_t validBytes{0};
};

std::vector<std::byte> EncodeBatch(uint64_t baseSequence, const std::vector<const WriteBatch *> &batches);
size_t EncodedBatchSize(const std::vector<const WriteBatch *> &batches);
Result<DecodeResult> DecodeLog(std::span<const std::byte> bytes);

}  // namespace storage_engine::wal
