#include "common.h"

#include <cerrno>
#include <cstring>

namespace storage_engine::internal {

std::string errnoMessage(std::string_view operation) { return std::string(operation) + ": " + std::strerror(errno); }

}  // namespace storage_engine::internal
