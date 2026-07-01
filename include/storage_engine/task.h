#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <utility>

namespace storage_engine {

template <typename T>
class Task {
 public:
  struct promise_type {
    Task get_return_object() { return Task(std::coroutine_handle<promise_type>::from_promise(*this)); }
    std::suspend_always initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void unhandled_exception() { exception_ = std::current_exception(); }

    template <typename U>
    void return_value(U &&value) {
      value_.emplace(std::forward<U>(value));
    }

    std::optional<T> value_;
    std::exception_ptr exception_;
  };

  explicit Task(std::coroutine_handle<promise_type> handle)
      : handle_(handle) {}
  Task(Task &&other) noexcept
      : handle_(std::exchange(other.handle_, {})) {}
  Task &operator=(Task &&other) noexcept {
    if (this != &other) {
      if (handle_) {
        handle_.destroy();
      }
      handle_ = std::exchange(other.handle_, {});
    }
    return *this;
  }
  Task(const Task &) = delete;
  Task &operator=(const Task &) = delete;
  ~Task() {
    if (handle_) {
      handle_.destroy();
    }
  }

  T run() {
    drive();
    return takeResult();
  }

  void drive() {
    while (!handle_.done()) {
      handle_.resume();
    }
    if (handle_.promise().exception_) {
      std::rethrow_exception(handle_.promise().exception_);
    }
  }

  T takeResult() { return std::move(*handle_.promise().value_); }

  struct Awaiter {
    Task &task;

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<>) {
      task.drive();
      return false;
    }
    T await_resume() { return task.takeResult(); }
  };

  Awaiter operator co_await() { return Awaiter{*this}; }

 private:
  std::coroutine_handle<promise_type> handle_;
};

template <>
class Task<void> {
 public:
  struct promise_type {
    Task get_return_object() { return Task(std::coroutine_handle<promise_type>::from_promise(*this)); }
    std::suspend_always initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() { exception_ = std::current_exception(); }

    std::exception_ptr exception_;
  };

  explicit Task(std::coroutine_handle<promise_type> handle)
      : handle_(handle) {}
  Task(Task &&other) noexcept
      : handle_(std::exchange(other.handle_, {})) {}
  Task(const Task &) = delete;
  Task &operator=(const Task &) = delete;
  ~Task() {
    if (handle_) {
      handle_.destroy();
    }
  }

  void run() {
    while (!handle_.done()) {
      handle_.resume();
    }
    if (handle_.promise().exception_) {
      std::rethrow_exception(handle_.promise().exception_);
    }
  }

 private:
  std::coroutine_handle<promise_type> handle_;
};

}  // namespace storage_engine
