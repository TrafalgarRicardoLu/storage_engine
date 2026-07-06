#include "wal.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "common.h"

#if defined(__x86_64__) || defined(__i386__)
#include <nmmintrin.h>
#endif

namespace storage_engine::wal {
namespace {

constexpr uint8_t kBatchRecord = 1;
constexpr size_t kRecordHeaderSize = 4 + 4 + 1;
constexpr size_t kBatchHeaderSize = 8 + 4;
constexpr size_t kEntryHeaderSize = 1 + 4 + 4;

void putBytes(std::vector<std::byte> &out, std::string_view bytes) {
  out.insert(out.end(),
             reinterpret_cast<const std::byte *>(bytes.data()),
             reinterpret_cast<const std::byte *>(bytes.data() + bytes.size()));
}

constexpr std::array<uint32_t, 256> makeCrc32cTable() {
  std::array<uint32_t, 256> table{};
  for (uint32_t i = 0; i < table.size(); ++i) {
    auto crc = i;
    for (int bit = 0; bit < 8; ++bit) {
      auto mask = 0u - (crc & 1u);
      crc = (crc >> 1u) ^ (0x82f63b78u & mask);
    }
    table[i] = crc;
  }
  return table;
}

constexpr auto kCrc32cTable = makeCrc32cTable();

uint32_t extendCrc32cSoftware(uint32_t crc, std::span<const std::byte> bytes) {
  for (auto byte : bytes) {
    auto index = (crc ^ static_cast<uint8_t>(byte)) & 0xffu;
    crc = (crc >> 8u) ^ kCrc32cTable[index];
  }
  return crc;
}

#if defined(__x86_64__) || defined(__i386__)
__attribute__((target("sse4.2"))) uint32_t extendCrc32cHardware(uint32_t crc, std::span<const std::byte> bytes) {
  auto *data = bytes.data();
  auto size = bytes.size();

#if defined(__x86_64__)
  while (size >= sizeof(uint64_t)) {
    uint64_t chunk = 0;
    std::memcpy(&chunk, data, sizeof(chunk));
    crc = static_cast<uint32_t>(_mm_crc32_u64(crc, chunk));
    data += sizeof(chunk);
    size -= sizeof(chunk);
  }
#endif

  while (size >= sizeof(uint32_t)) {
    uint32_t chunk = 0;
    std::memcpy(&chunk, data, sizeof(chunk));
    crc = _mm_crc32_u32(crc, chunk);
    data += sizeof(chunk);
    size -= sizeof(chunk);
  }
  while (size > 0) {
    crc = _mm_crc32_u8(crc, static_cast<uint8_t>(*data));
    ++data;
    --size;
  }
  return crc;
}

bool hasHardwareCrc32c() { return __builtin_cpu_supports("sse4.2"); }
#endif

void appendIovec(std::vector<iovec> &iovecs, const void *data, size_t size) {
  if (size == 0) {
    return;
  }
  iovecs.push_back(iovec{
      .iov_base = const_cast<void *>(data),
      .iov_len = size,
  });
}

uint32_t countEntries(const std::vector<const WriteBatch *> &batches) {
  uint32_t count = 0;
  for (auto *batch : batches) {
    count += static_cast<uint32_t>(batch->entries().size());
  }
  return count;
}

}  // namespace

uint32_t Crc32C(std::span<const std::byte> bytes) {
  auto crc = 0xffffffffu;
#if defined(__x86_64__) || defined(__i386__)
  if (hasHardwareCrc32c()) {
    return ~extendCrc32cHardware(crc, bytes);
  }
#endif
  return ~extendCrc32cSoftware(crc, bytes);
}

size_t EncodedBatchSize(const std::vector<const WriteBatch *> &batches) {
  size_t size = kRecordHeaderSize + kBatchHeaderSize;
  for (auto *batch : batches) {
    for (const auto &entry : batch->entries()) {
      size += kEntryHeaderSize + entry.key.size() + entry.value.size();
    }
  }
  return size;
}

void EncodeBatchFragmentsInto(uint64_t baseSequence,
                              const std::vector<const WriteBatch *> &batches,
                              EncodedBatchFragments &encoded) {
  auto entryCount = countEntries(batches);

  encoded.size = EncodedBatchSize(batches);
  encoded.fixed.clear();
  encoded.iovecs.clear();
  encoded.fixed.resize(kRecordHeaderSize + kBatchHeaderSize + static_cast<size_t>(entryCount) * kEntryHeaderSize);
  encoded.iovecs.reserve(2 + static_cast<size_t>(entryCount) * 3);

  encoded.fixed[8] = static_cast<std::byte>(kBatchRecord);
  internal::writeFixed64LE(encoded.fixed, kRecordHeaderSize, baseSequence);
  internal::writeFixed32LE(encoded.fixed, kRecordHeaderSize + 8, entryCount);

  auto crc = 0xffffffffu;
  auto batchHeader = std::span<const std::byte>(encoded.fixed).subspan(kRecordHeaderSize, kBatchHeaderSize);
  crc = extendCrc32cSoftware(crc, batchHeader);

  appendIovec(encoded.iovecs, encoded.fixed.data(), kRecordHeaderSize + kBatchHeaderSize);

  auto entryHeaderOffset = kRecordHeaderSize + kBatchHeaderSize;
  for (auto *batch : batches) {
    for (const auto &entry : batch->entries()) {
      auto *entryHeader = encoded.fixed.data() + entryHeaderOffset;
      entryHeader[0] = static_cast<std::byte>(entry.type);
      internal::writeFixed32LE(encoded.fixed, entryHeaderOffset + 1, static_cast<uint32_t>(entry.key.size()));
      internal::writeFixed32LE(encoded.fixed, entryHeaderOffset + 5, static_cast<uint32_t>(entry.value.size()));

      auto entryHeaderBytes = std::span<const std::byte>(entryHeader, kEntryHeaderSize);
      auto keyBytes =
          std::span<const std::byte>(reinterpret_cast<const std::byte *>(entry.key.data()), entry.key.size());
      auto valueBytes =
          std::span<const std::byte>(reinterpret_cast<const std::byte *>(entry.value.data()), entry.value.size());
      crc = extendCrc32cSoftware(crc, entryHeaderBytes);
      crc = extendCrc32cSoftware(crc, keyBytes);
      crc = extendCrc32cSoftware(crc, valueBytes);

      appendIovec(encoded.iovecs, entryHeader, kEntryHeaderSize);
      appendIovec(encoded.iovecs, entry.key.data(), entry.key.size());
      appendIovec(encoded.iovecs, entry.value.data(), entry.value.size());
      entryHeaderOffset += kEntryHeaderSize;
    }
  }

  internal::writeFixed32LE(encoded.fixed, 0, ~crc);
  internal::writeFixed32LE(encoded.fixed, 4, static_cast<uint32_t>(encoded.size - kRecordHeaderSize));
}

void EncodeBatchInto(uint64_t baseSequence,
                     const std::vector<const WriteBatch *> &batches,
                     std::vector<std::byte> &record) {
  record.clear();
  record.reserve(EncodedBatchSize(batches));
  internal::appendFixed32LE(record, 0);
  internal::appendFixed32LE(record, 0);
  record.push_back(static_cast<std::byte>(kBatchRecord));
  auto payloadOffset = record.size();

  internal::appendFixed64LE(record, baseSequence);
  internal::appendFixed32LE(record, countEntries(batches));

  for (auto *batch : batches) {
    for (const auto &entry : batch->entries()) {
      record.push_back(static_cast<std::byte>(entry.type));
      internal::appendFixed32LE(record, static_cast<uint32_t>(entry.key.size()));
      internal::appendFixed32LE(record, static_cast<uint32_t>(entry.value.size()));
      putBytes(record, entry.key);
      putBytes(record, entry.value);
    }
  }

  auto payload = std::span<const std::byte>(record).subspan(payloadOffset);
  internal::writeFixed32LE(record, 0, Crc32C(payload));
  internal::writeFixed32LE(record, 4, static_cast<uint32_t>(payload.size()));
}

Result<DecodeResult> DecodeLog(std::span<const std::byte> bytes) {
  DecodeResult result;
  size_t offset = 0;

  while (offset < bytes.size()) {
    auto recordOffset = offset;
    uint32_t expectedCrc = 0;
    uint32_t length = 0;
    if (!internal::readFixed32LE(bytes, offset, expectedCrc) || !internal::readFixed32LE(bytes, offset, length)) {
      break;
    }
    if (offset >= bytes.size()) {
      break;
    }
    auto type = static_cast<uint8_t>(bytes[offset++]);
    if (offset + length > bytes.size()) {
      break;
    }
    if (type != kBatchRecord) {
      return Status::Corruption("unknown WAL record type");
    }

    auto payload = bytes.subspan(offset, length);
    if (Crc32C(payload) != expectedCrc) {
      if (recordOffset + 9 + length >= bytes.size()) {
        break;
      }
      return Status::Corruption("WAL checksum mismatch");
    }

    DecodedBatch decoded;
    size_t payloadOffset = 0;
    uint32_t entryCount = 0;
    if (!internal::readFixed64LE(payload, payloadOffset, decoded.baseSequence) ||
        !internal::readFixed32LE(payload, payloadOffset, entryCount)) {
      return Status::Corruption("invalid WAL batch header");
    }

    for (uint32_t i = 0; i < entryCount; ++i) {
      if (payloadOffset >= payload.size()) {
        return Status::Corruption("truncated WAL batch entry");
      }
      auto typeByte = static_cast<uint8_t>(payload[payloadOffset++]);
      uint32_t keySize = 0;
      uint32_t valueSize = 0;
      if (!internal::readFixed32LE(payload, payloadOffset, keySize) ||
          !internal::readFixed32LE(payload, payloadOffset, valueSize)) {
        return Status::Corruption("invalid WAL batch entry");
      }
      if (payloadOffset + keySize + valueSize > payload.size()) {
        return Status::Corruption("truncated WAL batch payload");
      }

      auto *keyData = reinterpret_cast<const char *>(payload.data() + payloadOffset);
      std::string key(keyData, keySize);
      payloadOffset += keySize;
      auto *valueData = reinterpret_cast<const char *>(payload.data() + payloadOffset);
      std::string value(valueData, valueSize);
      payloadOffset += valueSize;

      if (typeByte == static_cast<uint8_t>(WriteBatch::Type::kPut)) {
        decoded.batch.Put(key, value);
      } else if (typeByte == static_cast<uint8_t>(WriteBatch::Type::kDelete)) {
        decoded.batch.Delete(key);
      } else {
        return Status::Corruption("unknown WAL entry type");
      }
    }

    result.batches.push_back(std::move(decoded));
    offset += length;
    result.validBytes = offset;
  }

  return result;
}

}  // namespace storage_engine::wal
