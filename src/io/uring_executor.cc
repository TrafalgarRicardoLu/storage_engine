#include "io/uring_executor.h"

#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

namespace storage_engine::io {
namespace {

std::string errnoMessage(std::string_view operation) { return std::string(operation) + ": " + std::strerror(errno); }

uint32_t loadAcquire(uint32_t &value) { return std::atomic_ref<uint32_t>(value).load(std::memory_order_acquire); }

void storeRelease(uint32_t &value, uint32_t next) {
  std::atomic_ref<uint32_t>(value).store(next, std::memory_order_release);
}

}  // namespace

Result<UringExecutor> UringExecutor::Create(uint32_t entries) {
  UringExecutor executor;
  auto status = executor.init(entries);
  if (!status.ok()) {
    return status;
  }
  return executor;
}

UringExecutor::UringExecutor(int ringFd)
    : ringFd_(ringFd) {}

UringExecutor::UringExecutor(UringExecutor &&other) noexcept { *this = std::move(other); }

UringExecutor &UringExecutor::operator=(UringExecutor &&other) noexcept {
  if (this != &other) {
    closeRing();
    ringFd_ = std::exchange(other.ringFd_, -1);
    sq_ = other.sq_;
    cq_ = other.cq_;
    sqes_ = std::exchange(other.sqes_, nullptr);
    sqRingPtr_ = std::exchange(other.sqRingPtr_, nullptr);
    cqRingPtr_ = std::exchange(other.cqRingPtr_, nullptr);
    sqesPtr_ = std::exchange(other.sqesPtr_, nullptr);
    sqRingSize_ = std::exchange(other.sqRingSize_, 0);
    cqRingSize_ = std::exchange(other.cqRingSize_, 0);
    sqesSize_ = std::exchange(other.sqesSize_, 0);
    singleMmap_ = std::exchange(other.singleMmap_, false);
  }
  return *this;
}

UringExecutor::~UringExecutor() { closeRing(); }

Status UringExecutor::init(uint32_t entries) {
  io_uring_params params{};
  ringFd_ = static_cast<int>(syscall(__NR_io_uring_setup, entries, &params));
  if (ringFd_ < 0) {
    return Status::IoError(errnoMessage("io_uring_setup"));
  }

  sqRingSize_ = params.sq_off.array + params.sq_entries * sizeof(uint32_t);
  cqRingSize_ = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);

  if (params.features & IORING_FEAT_SINGLE_MMAP) {
    singleMmap_ = true;
    sqRingSize_ = std::max(sqRingSize_, cqRingSize_);
    cqRingSize_ = sqRingSize_;
  }

  sqRingPtr_ =
      mmap(nullptr, sqRingSize_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ringFd_, IORING_OFF_SQ_RING);
  if (sqRingPtr_ == MAP_FAILED) {
    sqRingPtr_ = nullptr;
    auto status = Status::IoError(errnoMessage("mmap sq ring"));
    closeRing();
    return status;
  }

  if (singleMmap_) {
    cqRingPtr_ = sqRingPtr_;
  } else {
    cqRingPtr_ =
        mmap(nullptr, cqRingSize_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ringFd_, IORING_OFF_CQ_RING);
    if (cqRingPtr_ == MAP_FAILED) {
      cqRingPtr_ = nullptr;
      auto status = Status::IoError(errnoMessage("mmap cq ring"));
      closeRing();
      return status;
    }
  }

  sqesSize_ = params.sq_entries * sizeof(io_uring_sqe);
  sqesPtr_ = mmap(nullptr, sqesSize_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ringFd_, IORING_OFF_SQES);
  if (sqesPtr_ == MAP_FAILED) {
    sqesPtr_ = nullptr;
    auto status = Status::IoError(errnoMessage("mmap sqes"));
    closeRing();
    return status;
  }

  auto *sqBase = static_cast<std::byte *>(sqRingPtr_);
  auto *cqBase = static_cast<std::byte *>(cqRingPtr_);

  sq_.head = reinterpret_cast<uint32_t *>(sqBase + params.sq_off.head);
  sq_.tail = reinterpret_cast<uint32_t *>(sqBase + params.sq_off.tail);
  sq_.ringMask = reinterpret_cast<uint32_t *>(sqBase + params.sq_off.ring_mask);
  sq_.ringEntries = reinterpret_cast<uint32_t *>(sqBase + params.sq_off.ring_entries);
  sq_.flags = reinterpret_cast<uint32_t *>(sqBase + params.sq_off.flags);
  sq_.array = reinterpret_cast<uint32_t *>(sqBase + params.sq_off.array);

  cq_.head = reinterpret_cast<uint32_t *>(cqBase + params.cq_off.head);
  cq_.tail = reinterpret_cast<uint32_t *>(cqBase + params.cq_off.tail);
  cq_.ringMask = reinterpret_cast<uint32_t *>(cqBase + params.cq_off.ring_mask);
  cq_.ringEntries = reinterpret_cast<uint32_t *>(cqBase + params.cq_off.ring_entries);
  cq_.cqes = reinterpret_cast<io_uring_cqe *>(cqBase + params.cq_off.cqes);
  sqes_ = static_cast<io_uring_sqe *>(sqesPtr_);

  return Status::Ok();
}

void UringExecutor::closeRing() {
  if (sqesPtr_ != nullptr) {
    munmap(sqesPtr_, sqesSize_);
  }
  if (cqRingPtr_ != nullptr && cqRingPtr_ != sqRingPtr_) {
    munmap(cqRingPtr_, cqRingSize_);
  }
  if (sqRingPtr_ != nullptr) {
    munmap(sqRingPtr_, sqRingSize_);
  }
  if (ringFd_ >= 0) {
    close(ringFd_);
  }

  ringFd_ = -1;
  sqes_ = nullptr;
  sqRingPtr_ = nullptr;
  cqRingPtr_ = nullptr;
  sqesPtr_ = nullptr;
  sqRingSize_ = 0;
  cqRingSize_ = 0;
  sqesSize_ = 0;
  singleMmap_ = false;
}

Result<int32_t> UringExecutor::submitAndWait(io_uring_sqe &source) {
  std::lock_guard lock(mutex_);

  auto tail = loadAcquire(*sq_.tail);
  auto head = loadAcquire(*sq_.head);
  auto entries = *sq_.ringEntries;
  if (tail - head >= entries) {
    return Status::IoError("io_uring submission queue is full");
  }

  auto index = tail & *sq_.ringMask;
  auto *sqe = &sqes_[index];
  *sqe = source;
  sq_.array[index] = index;
  storeRelease(*sq_.tail, tail + 1);

  auto ret = syscall(__NR_io_uring_enter, ringFd_, 1, 1, IORING_ENTER_GETEVENTS, nullptr, 0);
  if (ret < 0) {
    return Status::IoError(errnoMessage("io_uring_enter"));
  }

  for (;;) {
    auto cqHead = loadAcquire(*cq_.head);
    auto cqTail = loadAcquire(*cq_.tail);
    if (cqHead != cqTail) {
      auto &cqe = cq_.cqes[cqHead & *cq_.ringMask];
      auto result = cqe.res;
      storeRelease(*cq_.head, cqHead + 1);
      return result;
    }

    ret = syscall(__NR_io_uring_enter, ringFd_, 0, 1, IORING_ENTER_GETEVENTS, nullptr, 0);
    if (ret < 0) {
      return Status::IoError(errnoMessage("io_uring_enter wait"));
    }
  }
}

Task<Status> UringExecutor::WritevAt(int fd, std::span<const iovec> iovecs, uint64_t offset, size_t expectedBytes) {
  io_uring_sqe sqe{};
  sqe.opcode = IORING_OP_WRITEV;
  sqe.fd = fd;
  sqe.addr = reinterpret_cast<uint64_t>(iovecs.data());
  sqe.len = static_cast<uint32_t>(iovecs.size());
  sqe.off = offset;

  auto result = submitAndWait(sqe);
  if (!result) {
    co_return result.error();
  }
  if (result.value() < 0) {
    co_return Status::IoError("io_uring writev failed: " + std::string(std::strerror(-result.value())));
  }
  if (static_cast<size_t>(result.value()) != expectedBytes) {
    co_return Status::IoError("io_uring writev completed short");
  }
  co_return Status::Ok();
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

  auto result = submitAndWait(sqe);
  if (!result) {
    co_return result.error();
  }
  if (result.value() < 0) {
    co_return Status::IoError("io_uring readv failed: " + std::string(std::strerror(-result.value())));
  }
  co_return static_cast<size_t>(result.value());
}

Task<Status> UringExecutor::FDataSync(int fd) {
  io_uring_sqe sqe{};
  sqe.opcode = IORING_OP_FSYNC;
  sqe.fd = fd;
  sqe.fsync_flags = IORING_FSYNC_DATASYNC;

  auto result = submitAndWait(sqe);
  if (!result) {
    co_return result.error();
  }
  if (result.value() < 0) {
    co_return Status::IoError("io_uring fdatasync failed: " + std::string(std::strerror(-result.value())));
  }
  co_return Status::Ok();
}

}  // namespace storage_engine::io
