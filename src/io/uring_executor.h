#pragma once

#include <linux/io_uring.h>
#include <sys/uio.h>

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <span>

#include "storage_engine/status.h"
#include "storage_engine/task.h"

namespace storage_engine::io {

class UringExecutor {
 public:
  static Result<UringExecutor> Create(uint32_t entries = 8);

  UringExecutor() = default;
  UringExecutor(UringExecutor &&other) noexcept;
  UringExecutor &operator=(UringExecutor &&other) noexcept;
  UringExecutor(const UringExecutor &) = delete;
  UringExecutor &operator=(const UringExecutor &) = delete;
  ~UringExecutor();

  Task<Status> WritevAt(int fd, std::span<const iovec> iovecs, uint64_t offset, size_t expectedBytes);
  Task<Result<size_t>> ReadAt(int fd, std::span<std::byte> buffer, uint64_t offset);
  Task<Status> FDataSync(int fd);

 private:
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

  explicit UringExecutor(int ringFd);

  Status init(uint32_t entries);
  Result<int32_t> submitAndWait(struct io_uring_sqe &source);
  void closeRing();

  int ringFd_{-1};
  SubmissionRing sq_;
  CompletionRing cq_;
  struct io_uring_sqe *sqes_{nullptr};
  void *sqRingPtr_{nullptr};
  void *cqRingPtr_{nullptr};
  void *sqesPtr_{nullptr};
  size_t sqRingSize_{0};
  size_t cqRingSize_{0};
  size_t sqesSize_{0};
  bool singleMmap_{false};
  std::mutex mutex_;
};

}  // namespace storage_engine::io
