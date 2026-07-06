#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace storage_engine::internal {

std::string errnoMessage(std::string_view operation);

inline uint64_t elapsedMicros(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end) {
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  return micros > 0 ? static_cast<uint64_t>(micros) : 1;
}

inline void appendFixed32LE(std::vector<std::byte> &out, uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    out.push_back(static_cast<std::byte>((value >> (i * 8)) & 0xff));
  }
}

inline void appendFixed64LE(std::vector<std::byte> &out, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    out.push_back(static_cast<std::byte>((value >> (i * 8)) & 0xff));
  }
}

inline void writeFixed32LE(std::vector<std::byte> &out, size_t offset, uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    out[offset + static_cast<size_t>(i)] = static_cast<std::byte>((value >> (i * 8)) & 0xff);
  }
}

inline void writeFixed64LE(std::vector<std::byte> &out, size_t offset, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    out[offset + static_cast<size_t>(i)] = static_cast<std::byte>((value >> (i * 8)) & 0xff);
  }
}

inline bool readFixed32LE(std::span<const std::byte> bytes, size_t &offset, uint32_t &value) {
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

inline bool readFixed64LE(std::span<const std::byte> bytes, size_t &offset, uint64_t &value) {
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

}  // namespace storage_engine::internal
