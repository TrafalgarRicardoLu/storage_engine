#include "io/uring_executor.h"

#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

namespace storage_engine::io {
namespace {

std::string errnoMessage(std::string_view operation) { return std::string(operation) + ": " + std::strerror(errno); }

uint32_t loadAcquire(uint32_t &value) { return std::atomic_ref<uint32_t>(value).load(std::memory_order_acquire); }

void storeRelease(uint32_t &value, uint32_t next) {
  std::atomic_ref<uint32_t>(value).store(next, std::memory_order_release);
}

}  // namespace

struct UringExecutor::Operation {
  io_uring_sqe sqe{};
  std::coroutine_handle<> continuation;
  int32_t result{0};
};

struct UringExecutor::SingleOperationAwaiter {
  UringExecutor *executor;
  Operation operation;
  const char *submitError;
  const char *operationError;
  bool submitFailed{false};

  bool await_ready() const { return false; }
  bool await_suspend(std::coroutine_handle<> continuation) {
    operation.continuation = continuation;
    auto status = executor->submit(&operation);
    if (!status.ok()) {
      submitFailed = true;
      return false;
    }
    return true;
  }
  Result<int32_t> await_resume() {
    if (submitFailed) {
      return Status::IoError(submitError);
    }
    if (operation.result < 0) {
      return Status::IoError(std::string(operationError) + ": " + std::strerror(-operation.result));
    }
    return operation.result;
  }
};

struct UringExecutor::State {
  struct SubmissionRing {
    uint32_t *head{nullptr};
    uint32_t *tail{nullptr};
    uint32_t *ringMask{nullptr};
    uint32_t *ringEntries{nullptr};
    uint32_t *flags{nullptr};
    uint32_t *array{nullptr};
  };

  struct CompletionRing {
    uint32_t *head{nullptr};
    uint32_t *tail{nullptr};
    uint32_t *ringMask{nullptr};
    uint32_t *ringEntries{nullptr};
    struct io_uring_cqe *cqes{nullptr};
  };

  int ringFd{-1};
  SubmissionRing sq;
  CompletionRing cq;
  struct io_uring_sqe *sqes{nullptr};
  void *sqRingPtr{nullptr};
  void *cqRingPtr{nullptr};
  void *sqesPtr{nullptr};
  size_t sqRingSize{0};
  size_t cqRingSize{0};
  size_t sqesSize{0};
  bool singleMmap{false};
  std::mutex sqMutex;
  std::thread completionThread;
  std::atomic<bool> stopping{false};
  std::atomic<uint64_t> completionLoopCompletions{0};
  bool sqPollEnabled{false};
};

Status UringExecutor::wakeSqPollIfNeeded(State &state) {
  if (!state.sqPollEnabled) {
    return Status::Ok();
  }
  auto flags = loadAcquire(*state.sq.flags);
  if ((flags & IORING_SQ_NEED_WAKEUP) == 0) {
    return Status::Ok();
  }
  auto ret = syscall(__NR_io_uring_enter, state.ringFd, 0, 0, IORING_ENTER_SQ_WAKEUP, nullptr, 0);
  if (ret < 0) {
    return Status::IoError(errnoMessage("io_uring_enter sqpoll wakeup"));
  }
  return Status::Ok();
}

Status UringExecutor::submitRaw(State &state, io_uring_sqe source, uint64_t userData) {
  std::lock_guard lock(state.sqMutex);

  auto tail = loadAcquire(*state.sq.tail);
  auto head = loadAcquire(*state.sq.head);
  auto entries = *state.sq.ringEntries;
  if (tail - head >= entries) {
    return Status::IoError("io_uring submission queue is full");
  }

  auto index = tail & *state.sq.ringMask;
  auto *sqe = &state.sqes[index];
  *sqe = source;
  sqe->user_data = userData;
  state.sq.array[index] = index;
  storeRelease(*state.sq.tail, tail + 1);

  if (state.sqPollEnabled) {
    return wakeSqPollIfNeeded(state);
  }

  auto ret = syscall(__NR_io_uring_enter, state.ringFd, 1, 0, 0, nullptr, 0);
  if (ret < 0) {
    return Status::IoError(errnoMessage("io_uring_enter submit"));
  }
  return Status::Ok();
}

void UringExecutor::completionLoop(std::shared_ptr<State> state) {
  for (;;) {
    auto ret = syscall(__NR_io_uring_enter, state->ringFd, 0, 1, IORING_ENTER_GETEVENTS, nullptr, 0);
    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (state->stopping.load(std::memory_order_acquire)) {
        return;
      }
      continue;
    }

    for (;;) {
      auto cqHead = loadAcquire(*state->cq.head);
      auto cqTail = loadAcquire(*state->cq.tail);
      if (cqHead == cqTail) {
        break;
      }

      auto &cqe = state->cq.cqes[cqHead & *state->cq.ringMask];
      auto userData = cqe.user_data;
      auto result = cqe.res;
      storeRelease(*state->cq.head, cqHead + 1);

      if (userData == 0) {
        if (state->stopping.load(std::memory_order_acquire)) {
          return;
        }
        continue;
      }

      auto *operation = reinterpret_cast<Operation *>(userData);
      operation->result = result;
      state->completionLoopCompletions.fetch_add(1, std::memory_order_relaxed);
      if (operation->continuation) {
        operation->continuation.resume();
      }
    }
  }
}

Result<UringExecutor> UringExecutor::Create() { return Create(Options{}); }

Result<UringExecutor> UringExecutor::Create(Options options) {
  UringExecutor executor;
  auto status = executor.init(options);
  if (!status.ok()) {
    return status;
  }
  return executor;
}

UringExecutor::~UringExecutor() { closeRing(); }

Status UringExecutor::init(Options options) {
  state_ = std::make_shared<State>();

  io_uring_params params{};
  if (options.sqPoll) {
    params.flags |= IORING_SETUP_SQPOLL;
    params.sq_thread_idle = options.sqPollIdleMs;
  }
  state_->ringFd = static_cast<int>(syscall(__NR_io_uring_setup, options.entries, &params));
  if (state_->ringFd < 0) {
    auto status = Status::IoError(errnoMessage("io_uring_setup"));
    state_.reset();
    return status;
  }

  state_->sqRingSize = params.sq_off.array + params.sq_entries * sizeof(uint32_t);
  state_->cqRingSize = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);

  if (params.features & IORING_FEAT_SINGLE_MMAP) {
    state_->singleMmap = true;
    state_->sqRingSize = std::max(state_->sqRingSize, state_->cqRingSize);
    state_->cqRingSize = state_->sqRingSize;
  }

  state_->sqRingPtr = mmap(nullptr,
                           state_->sqRingSize,
                           PROT_READ | PROT_WRITE,
                           MAP_SHARED | MAP_POPULATE,
                           state_->ringFd,
                           IORING_OFF_SQ_RING);
  if (state_->sqRingPtr == MAP_FAILED) {
    state_->sqRingPtr = nullptr;
    auto status = Status::IoError(errnoMessage("mmap sq ring"));
    closeRing();
    return status;
  }

  if (state_->singleMmap) {
    state_->cqRingPtr = state_->sqRingPtr;
  } else {
    state_->cqRingPtr = mmap(nullptr,
                             state_->cqRingSize,
                             PROT_READ | PROT_WRITE,
                             MAP_SHARED | MAP_POPULATE,
                             state_->ringFd,
                             IORING_OFF_CQ_RING);
    if (state_->cqRingPtr == MAP_FAILED) {
      state_->cqRingPtr = nullptr;
      auto status = Status::IoError(errnoMessage("mmap cq ring"));
      closeRing();
      return status;
    }
  }

  state_->sqesSize = params.sq_entries * sizeof(io_uring_sqe);
  state_->sqesPtr = mmap(nullptr,
                         state_->sqesSize,
                         PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_POPULATE,
                         state_->ringFd,
                         IORING_OFF_SQES);
  if (state_->sqesPtr == MAP_FAILED) {
    state_->sqesPtr = nullptr;
    auto status = Status::IoError(errnoMessage("mmap sqes"));
    closeRing();
    return status;
  }

  auto *sqBase = static_cast<std::byte *>(state_->sqRingPtr);
  auto *cqBase = static_cast<std::byte *>(state_->cqRingPtr);

  state_->sq.head = reinterpret_cast<uint32_t *>(sqBase + params.sq_off.head);
  state_->sq.tail = reinterpret_cast<uint32_t *>(sqBase + params.sq_off.tail);
  state_->sq.ringMask = reinterpret_cast<uint32_t *>(sqBase + params.sq_off.ring_mask);
  state_->sq.ringEntries = reinterpret_cast<uint32_t *>(sqBase + params.sq_off.ring_entries);
  state_->sq.flags = reinterpret_cast<uint32_t *>(sqBase + params.sq_off.flags);
  state_->sq.array = reinterpret_cast<uint32_t *>(sqBase + params.sq_off.array);

  state_->cq.head = reinterpret_cast<uint32_t *>(cqBase + params.cq_off.head);
  state_->cq.tail = reinterpret_cast<uint32_t *>(cqBase + params.cq_off.tail);
  state_->cq.ringMask = reinterpret_cast<uint32_t *>(cqBase + params.cq_off.ring_mask);
  state_->cq.ringEntries = reinterpret_cast<uint32_t *>(cqBase + params.cq_off.ring_entries);
  state_->cq.cqes = reinterpret_cast<io_uring_cqe *>(cqBase + params.cq_off.cqes);
  state_->sqes = static_cast<io_uring_sqe *>(state_->sqesPtr);
  state_->sqPollEnabled = options.sqPoll;

  state_->completionThread = std::thread(&UringExecutor::completionLoop, state_);
  return Status::Ok();
}

Status UringExecutor::submit(Operation *operation) {
  if (!state_ || state_->ringFd < 0) {
    return Status::IoError("io_uring executor is closed");
  }
  return submitRaw(*state_, operation->sqe, reinterpret_cast<uint64_t>(operation));
}

Status UringExecutor::submitLinked(Operation *first, Operation *second) {
  if (!state_ || state_->ringFd < 0) {
    return Status::IoError("io_uring executor is closed");
  }

  std::lock_guard lock(state_->sqMutex);
  auto tail = loadAcquire(*state_->sq.tail);
  auto head = loadAcquire(*state_->sq.head);
  auto entries = *state_->sq.ringEntries;
  if (tail - head + 2 > entries) {
    return Status::IoError("io_uring submission queue is full");
  }

  auto firstIndex = tail & *state_->sq.ringMask;
  auto secondIndex = (tail + 1) & *state_->sq.ringMask;
  first->sqe.flags |= IOSQE_IO_LINK;
  first->sqe.user_data = reinterpret_cast<uint64_t>(first);
  second->sqe.user_data = reinterpret_cast<uint64_t>(second);
  state_->sqes[firstIndex] = first->sqe;
  state_->sqes[secondIndex] = second->sqe;
  state_->sq.array[firstIndex] = firstIndex;
  state_->sq.array[secondIndex] = secondIndex;
  storeRelease(*state_->sq.tail, tail + 2);

  if (state_->sqPollEnabled) {
    return wakeSqPollIfNeeded(*state_);
  }

  auto ret = syscall(__NR_io_uring_enter, state_->ringFd, 2, 0, 0, nullptr, 0);
  if (ret < 0) {
    return Status::IoError(errnoMessage("io_uring_enter submit linked"));
  }
  return Status::Ok();
}

void UringExecutor::closeRing() {
  if (!state_) {
    return;
  }

  if (state_->completionThread.joinable()) {
    state_->stopping.store(true, std::memory_order_release);
    io_uring_sqe sqe{};
    sqe.opcode = IORING_OP_NOP;
    auto status = submitRaw(*state_, sqe, 0);
    if (!status.ok()) {
      close(state_->ringFd);
      state_->ringFd = -1;
    }
    state_->completionThread.join();
  }

  if (state_->sqesPtr != nullptr) {
    munmap(state_->sqesPtr, state_->sqesSize);
  }
  if (state_->cqRingPtr != nullptr && state_->cqRingPtr != state_->sqRingPtr) {
    munmap(state_->cqRingPtr, state_->cqRingSize);
  }
  if (state_->sqRingPtr != nullptr) {
    munmap(state_->sqRingPtr, state_->sqRingSize);
  }
  if (state_->ringFd >= 0) {
    close(state_->ringFd);
  }

  state_.reset();
}

UringExecutor::DebugStats UringExecutor::DebugStatsForTest() const {
  if (!state_) {
    return {};
  }
  return DebugStats{
      .completionLoopCompletions = state_->completionLoopCompletions.load(std::memory_order_relaxed),
      .sqPollEnabled = state_->sqPollEnabled,
  };
}

Task<Status> UringExecutor::WritevAndFDataSync(int fd,
                                               std::span<const iovec> iovecs,
                                               uint64_t offset,
                                               size_t expectedBytes) {
  struct Awaiter {
    UringExecutor *executor;
    Operation writeOperation;
    Operation syncOperation;
    bool submitFailed{false};
    size_t expectedBytes;

    bool await_ready() const { return false; }
    bool await_suspend(std::coroutine_handle<> continuation) {
      syncOperation.continuation = continuation;
      auto status = executor->submitLinked(&writeOperation, &syncOperation);
      if (!status.ok()) {
        submitFailed = true;
        return false;
      }
      return true;
    }
    Status await_resume() {
      if (submitFailed) {
        return Status::IoError("io_uring linked writev+fdatasync submit failed");
      }
      if (writeOperation.result < 0) {
        return Status::IoError("io_uring writev failed: " + std::string(std::strerror(-writeOperation.result)));
      }
      if (static_cast<size_t>(writeOperation.result) != expectedBytes) {
        return Status::IoError("io_uring writev completed short");
      }
      if (syncOperation.result < 0) {
        return Status::IoError("io_uring fdatasync failed: " + std::string(std::strerror(-syncOperation.result)));
      }
      return Status::Ok();
    }
  };

  io_uring_sqe writeSqe{};
  writeSqe.opcode = IORING_OP_WRITEV;
  writeSqe.fd = fd;
  writeSqe.addr = reinterpret_cast<uint64_t>(iovecs.data());
  writeSqe.len = static_cast<uint32_t>(iovecs.size());
  writeSqe.off = offset;

  io_uring_sqe syncSqe{};
  syncSqe.opcode = IORING_OP_FSYNC;
  syncSqe.fd = fd;
  syncSqe.fsync_flags = IORING_FSYNC_DATASYNC;

  co_return co_await Awaiter{
      .executor = this,
      .writeOperation =
          Operation{
              .sqe = writeSqe,
              .continuation = {},
              .result = 0,
          },
      .syncOperation =
          Operation{
              .sqe = syncSqe,
              .continuation = {},
              .result = 0,
          },
      .submitFailed = false,
      .expectedBytes = expectedBytes,
  };
}

Task<Result<size_t>> UringExecutor::ReadAt(int fd, std::span<std::byte> buffer, uint64_t offset) {
  iovec iov{
      .iov_base = buffer.data(),
      .iov_len = buffer.size(),
  };
  io_uring_sqe sqe{};
  sqe.opcode = IORING_OP_READV;
  sqe.fd = fd;
  sqe.addr = reinterpret_cast<uint64_t>(&iov);
  sqe.len = 1;
  sqe.off = offset;

  auto result = co_await SingleOperationAwaiter{
      .executor = this,
      .operation =
          Operation{
              .sqe = sqe,
              .continuation = {},
              .result = 0,
          },
      .submitError = "io_uring readv submit failed",
      .operationError = "io_uring readv failed",
      .submitFailed = false,
  };
  if (!result) {
    co_return result.error();
  }
  co_return static_cast<size_t>(result.value());
}

Task<Status> UringExecutor::FDataSync(int fd) {
  io_uring_sqe sqe{};
  sqe.opcode = IORING_OP_FSYNC;
  sqe.fd = fd;
  sqe.fsync_flags = IORING_FSYNC_DATASYNC;

  auto result = co_await SingleOperationAwaiter{
      .executor = this,
      .operation =
          Operation{
              .sqe = sqe,
              .continuation = {},
              .result = 0,
          },
      .submitError = "io_uring fdatasync submit failed",
      .operationError = "io_uring fdatasync failed",
      .submitFailed = false,
  };
  if (!result) {
    co_return result.error();
  }
  co_return Status::Ok();
}

}  // namespace storage_engine::io
