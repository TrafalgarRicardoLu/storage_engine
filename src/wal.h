#pragma once

#include <sys/uio.h>

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

struct EncodedBatchFragments {
  EncodedBatchFragments() = default;
  EncodedBatchFragments(const EncodedBatchFragments &) = delete;
  EncodedBatchFragments &operator=(const EncodedBatchFragments &) = delete;
  EncodedBatchFragments(EncodedBatchFragments &&) noexcept = default;
  EncodedBatchFragments &operator=(EncodedBatchFragments &&) noexcept = default;

  std::vector<std::byte> fixed;
  std::vector<iovec> iovecs;
  size_t size{0};
};

void EncodeBatchInto(uint64_t baseSequence,
                     const std::vector<const WriteBatch *> &batches,
                     std::vector<std::byte> &record);
void EncodeBatchFragmentsInto(uint64_t baseSequence,
                              const std::vector<const WriteBatch *> &batches,
                              EncodedBatchFragments &encoded);
size_t EncodedBatchSize(const std::vector<const WriteBatch *> &batches);
uint32_t Crc32C(std::span<const std::byte> bytes);
Result<DecodeResult> DecodeLog(std::span<const std::byte> bytes);

}  // namespace storage_engine::wal
