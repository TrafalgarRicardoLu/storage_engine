#pragma once

#include <linux/io_uring.h>
#include <sys/uio.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

#include "storage_engine/status.h"
#include "storage_engine/task.h"

namespace storage_engine::io {

class UringExecutor {
 public:
  struct Options {
    uint32_t entries{8};
    bool sqPoll{false};
    uint32_t sqPollIdleMs{2000};
  };

  struct DebugStats {
    uint64_t completionLoopCompletions{0};
    bool sqPollEnabled{false};
  };

  static Result<UringExecutor> Create();
  static Result<UringExecutor> Create(Options options);

  UringExecutor() = default;
  UringExecutor(UringExecutor &&other) noexcept = default;
  UringExecutor &operator=(UringExecutor &&other) noexcept = default;
  UringExecutor(const UringExecutor &) = delete;
  UringExecutor &operator=(const UringExecutor &) = delete;
  ~UringExecutor();

  Task<Status> WritevAndFDataSync(int fd, std::span<const iovec> iovecs, uint64_t offset, size_t expectedBytes);
  Task<Result<size_t>> ReadAt(int fd, std::span<std::byte> buffer, uint64_t offset);
  Task<Status> FDataSync(int fd);
  DebugStats DebugStatsForTest() const;

 private:
  struct Operation;
  struct SingleOperationAwaiter;
  struct State;

  Status init(Options options);
  Status submit(Operation *operation);
  Status submitLinked(Operation *first, Operation *second);
  static Status submitRaw(State &state, struct io_uring_sqe source, uint64_t userData);
  static Status wakeSqPollIfNeeded(State &state);
  static void completionLoop(std::shared_ptr<State> state);
  void closeRing();

  std::shared_ptr<State> state_;
};

}  // namespace storage_engine::io
