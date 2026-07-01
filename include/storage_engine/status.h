#pragma once

#include <cassert>
#include <string>
#include <utility>

namespace storage_engine {

enum class StatusCode {
  kOk = 0,
  kNotFound,
  kInvalidArgument,
  kIoError,
  kCorruption,
};

class Status {
 public:
  Status() = default;
  Status(StatusCode code, std::string message)
      : code_(code),
        message_(std::move(message)) {}

  static Status Ok() { return {}; }
  static Status NotFound(std::string message) { return {StatusCode::kNotFound, std::move(message)}; }
  static Status InvalidArgument(std::string message) { return {StatusCode::kInvalidArgument, std::move(message)}; }
  static Status IoError(std::string message) { return {StatusCode::kIoError, std::move(message)}; }
  static Status Corruption(std::string message) { return {StatusCode::kCorruption, std::move(message)}; }

  bool ok() const { return code_ == StatusCode::kOk; }
  StatusCode code() const { return code_; }
  const std::string &message() const { return message_; }

 private:
  StatusCode code_{StatusCode::kOk};
  std::string message_;
};

template <typename T>
class Result {
 public:
  Result(T value)
      : ok_(true),
        value_(std::move(value)) {}
  Result(Status status)
      : ok_(false),
        status_(std::move(status)) {
    assert(!status_.ok());
  }

  bool ok() const { return ok_; }
  explicit operator bool() const { return ok_; }

  T &value() & {
    assert(ok_);
    return value_;
  }
  const T &value() const & {
    assert(ok_);
    return value_;
  }
  T &&value() && {
    assert(ok_);
    return std::move(value_);
  }

  const Status &error() const {
    assert(!ok_);
    return status_;
  }

 private:
  bool ok_{false};
  T value_{};
  Status status_{Status::Ok()};
};

}  // namespace storage_engine
