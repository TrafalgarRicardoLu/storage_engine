#pragma once

#include <condition_variable>
#include <coroutine>
#include <exception>
#include <mutex>
#include <optional>
#include <utility>

namespace storage_engine {

template <typename T>
class Task {
 public:
  struct promise_type {
    Task get_return_object() { return Task(std::coroutine_handle<promise_type>::from_promise(*this)); }
    std::suspend_always initial_suspend() noexcept { return {}; }

    struct FinalAwaiter {
      bool await_ready() noexcept { return false; }
      std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> handle) noexcept {
        auto &promise = handle.promise();
        {
          std::lock_guard lock(promise.mutex_);
          promise.completed_ = true;
        }
        promise.cv_.notify_all();
        if (promise.continuation_) {
          return promise.continuation_;
        }
        return std::noop_coroutine();
      }
      void await_resume() noexcept {}
    };

    FinalAwaiter final_suspend() noexcept { return {}; }
    void unhandled_exception() { exception_ = std::current_exception(); }

    template <typename U>
    void return_value(U &&value) {
      value_.emplace(std::forward<U>(value));
    }

    void wait() {
      std::unique_lock lock(mutex_);
      cv_.wait(lock, [this] { return completed_; });
    }

    bool completed() {
      std::lock_guard lock(mutex_);
      return completed_;
    }

    std::optional<T> value_;
    std::exception_ptr exception_;
    std::coroutine_handle<> continuation_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool completed_{false};
  };

  explicit Task(std::coroutine_handle<promise_type> handle)
      : handle_(handle) {}
  Task(Task &&other) noexcept
      : handle_(std::exchange(other.handle_, {})),
        started_(std::exchange(other.started_, false)) {}
  Task &operator=(Task &&other) noexcept {
    if (this != &other) {
      if (handle_) {
        handle_.destroy();
      }
      handle_ = std::exchange(other.handle_, {});
      started_ = std::exchange(other.started_, false);
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
    start();
    handle_.promise().wait();
    return takeResult();
  }

  T takeResult() {
    if (handle_.promise().exception_) {
      std::rethrow_exception(handle_.promise().exception_);
    }
    return std::move(*handle_.promise().value_);
  }

  struct Awaiter {
    Task &task;

    bool await_ready() { return task.handle_.promise().completed(); }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) {
      task.handle_.promise().continuation_ = continuation;
      if (!task.started_) {
        task.started_ = true;
        return task.handle_;
      }
      return std::noop_coroutine();
    }
    T await_resume() { return task.takeResult(); }
  };

  Awaiter operator co_await() { return Awaiter{*this}; }

 private:
  void start() {
    if (!started_) {
      started_ = true;
      handle_.resume();
    }
  }

  std::coroutine_handle<promise_type> handle_;
  bool started_{false};
};

template <>
class Task<void> {
 public:
  struct promise_type {
    Task get_return_object() { return Task(std::coroutine_handle<promise_type>::from_promise(*this)); }
    std::suspend_always initial_suspend() noexcept { return {}; }

    struct FinalAwaiter {
      bool await_ready() noexcept { return false; }
      std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> handle) noexcept {
        auto &promise = handle.promise();
        {
          std::lock_guard lock(promise.mutex_);
          promise.completed_ = true;
        }
        promise.cv_.notify_all();
        if (promise.continuation_) {
          return promise.continuation_;
        }
        return std::noop_coroutine();
      }
      void await_resume() noexcept {}
    };

    FinalAwaiter final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() { exception_ = std::current_exception(); }

    void wait() {
      std::unique_lock lock(mutex_);
      cv_.wait(lock, [this] { return completed_; });
    }

    bool completed() {
      std::lock_guard lock(mutex_);
      return completed_;
    }

    std::exception_ptr exception_;
    std::coroutine_handle<> continuation_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool completed_{false};
  };

  explicit Task(std::coroutine_handle<promise_type> handle)
      : handle_(handle) {}
  Task(Task &&other) noexcept
      : handle_(std::exchange(other.handle_, {})),
        started_(std::exchange(other.started_, false)) {}
  Task(const Task &) = delete;
  Task &operator=(const Task &) = delete;
  ~Task() {
    if (handle_) {
      handle_.destroy();
    }
  }

  void run() {
    start();
    handle_.promise().wait();
    if (handle_.promise().exception_) {
      std::rethrow_exception(handle_.promise().exception_);
    }
  }

 private:
  void start() {
    if (!started_) {
      started_ = true;
      handle_.resume();
    }
  }

  std::coroutine_handle<promise_type> handle_;
  bool started_{false};
};

}  // namespace storage_engine
