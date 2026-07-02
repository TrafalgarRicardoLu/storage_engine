#include "wal.h"

#include <cstring>

namespace storage_engine::wal {
namespace {

constexpr uint8_t kBatchRecord = 1;

void putFixed32(std::vector<std::byte> &out, uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    out.push_back(static_cast<std::byte>((value >> (i * 8)) & 0xff));
  }
}

void putFixed64(std::vector<std::byte> &out, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    out.push_back(static_cast<std::byte>((value >> (i * 8)) & 0xff));
  }
}

void putBytes(std::vector<std::byte> &out, std::string_view bytes) {
  out.insert(out.end(),
             reinterpret_cast<const std::byte *>(bytes.data()),
             reinterpret_cast<const std::byte *>(bytes.data() + bytes.size()));
}

void writeFixed32(std::vector<std::byte> &out, size_t offset, uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    out[offset + static_cast<size_t>(i)] = static_cast<std::byte>((value >> (i * 8)) & 0xff);
  }
}

bool readFixed32(std::span<const std::byte> bytes, size_t &offset, uint32_t &value) {
  if (offset + 4 > bytes.size()) {
    return false;
  }
  value = 0;
  for (int i = 0; i < 4; ++i) {
    value |= static_cast<uint32_t>(bytes[offset + i]) << (i * 8);
  }
  offset += 4;
  return true;
}

bool readFixed64(std::span<const std::byte> bytes, size_t &offset, uint64_t &value) {
  if (offset + 8 > bytes.size()) {
    return false;
  }
  value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= static_cast<uint64_t>(bytes[offset + i]) << (i * 8);
  }
  offset += 8;
  return true;
}

uint32_t crc32(std::span<const std::byte> bytes) {
  uint32_t crc = 0xffffffffu;
  for (auto byte : bytes) {
    crc ^= static_cast<uint8_t>(byte);
    for (int bit = 0; bit < 8; ++bit) {
      auto mask = 0u - (crc & 1u);
      crc = (crc >> 1u) ^ (0xedb88320u & mask);
    }
  }
  return ~crc;
}

}  // namespace

size_t EncodedBatchSize(const std::vector<const WriteBatch *> &batches) {
  size_t size = 4 + 4 + 1 + 8 + 4;
  for (auto *batch : batches) {
    for (const auto &entry : batch->entries()) {
      size += 1 + 4 + 4 + entry.key.size() + entry.value.size();
    }
  }
  return size;
}

std::vector<std::byte> EncodeBatch(uint64_t baseSequence, const std::vector<const WriteBatch *> &batches) {
  std::vector<std::byte> record;
  record.reserve(EncodedBatchSize(batches));
  putFixed32(record, 0);
  putFixed32(record, 0);
  record.push_back(static_cast<std::byte>(kBatchRecord));
  auto payloadOffset = record.size();

  putFixed64(record, baseSequence);

  uint32_t entryCount = 0;
  for (auto *batch : batches) {
    entryCount += static_cast<uint32_t>(batch->entries().size());
  }
  putFixed32(record, entryCount);

  for (auto *batch : batches) {
    for (const auto &entry : batch->entries()) {
      record.push_back(static_cast<std::byte>(entry.type));
      putFixed32(record, static_cast<uint32_t>(entry.key.size()));
      putFixed32(record, static_cast<uint32_t>(entry.value.size()));
      putBytes(record, entry.key);
      putBytes(record, entry.value);
    }
  }

  auto payload = std::span<const std::byte>(record).subspan(payloadOffset);
  writeFixed32(record, 0, crc32(payload));
  writeFixed32(record, 4, static_cast<uint32_t>(payload.size()));
  return record;
}

Result<DecodeResult> DecodeLog(std::span<const std::byte> bytes) {
  DecodeResult result;
  size_t offset = 0;

  while (offset < bytes.size()) {
    auto recordOffset = offset;
    uint32_t expectedCrc = 0;
    uint32_t length = 0;
    if (!readFixed32(bytes, offset, expectedCrc) || !readFixed32(bytes, offset, length)) {
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
    if (crc32(payload) != expectedCrc) {
      if (recordOffset + 9 + length >= bytes.size()) {
        break;
      }
      return Status::Corruption("WAL checksum mismatch");
    }

    DecodedBatch decoded;
    size_t payloadOffset = 0;
    uint32_t entryCount = 0;
    if (!readFixed64(payload, payloadOffset, decoded.baseSequence) ||
        !readFixed32(payload, payloadOffset, entryCount)) {
      return Status::Corruption("invalid WAL batch header");
    }

    for (uint32_t i = 0; i < entryCount; ++i) {
      if (payloadOffset >= payload.size()) {
        return Status::Corruption("truncated WAL batch entry");
      }
      auto typeByte = static_cast<uint8_t>(payload[payloadOffset++]);
      uint32_t keySize = 0;
      uint32_t valueSize = 0;
      if (!readFixed32(payload, payloadOffset, keySize) || !readFixed32(payload, payloadOffset, valueSize)) {
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
