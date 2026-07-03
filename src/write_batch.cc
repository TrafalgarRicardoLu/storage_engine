#include "storage_engine/db.h"

namespace storage_engine {

WriteBatch::WriteBatch()
    : arena_(inlineArena_.data(), inlineArena_.size()),
      entries_(&arena_) {}

WriteBatch::WriteBatch(const WriteBatch &other)
    : WriteBatch() {
  for (const auto &entry : other.entries_) {
    appendEntry(entry);
  }
}

WriteBatch &WriteBatch::operator=(const WriteBatch &other) {
  if (this != &other) {
    entries_.clear();
    for (const auto &entry : other.entries_) {
      appendEntry(entry);
    }
  }
  return *this;
}

WriteBatch::WriteBatch(WriteBatch &&other)
    : WriteBatch() {
  for (const auto &entry : other.entries_) {
    appendEntry(entry);
  }
  other.entries_.clear();
}

WriteBatch &WriteBatch::operator=(WriteBatch &&other) {
  if (this != &other) {
    entries_.clear();
    for (const auto &entry : other.entries_) {
      appendEntry(entry);
    }
    other.entries_.clear();
  }
  return *this;
}

void WriteBatch::Put(std::string_view key, std::string_view value) {
  entries_.push_back(Entry{
      .type = Type::kPut,
      .key = ArenaString(key, &arena_),
      .value = ArenaString(value, &arena_),
  });
}

void WriteBatch::Delete(std::string_view key) {
  entries_.push_back(Entry{
      .type = Type::kDelete,
      .key = ArenaString(key, &arena_),
      .value = ArenaString(&arena_),
  });
}

void WriteBatch::appendEntry(const Entry &entry) {
  entries_.push_back(Entry{
      .type = entry.type,
      .key = ArenaString(std::string_view(entry.key.data(), entry.key.size()), &arena_),
      .value = ArenaString(std::string_view(entry.value.data(), entry.value.size()), &arena_),
  });
}

}  // namespace storage_engine
