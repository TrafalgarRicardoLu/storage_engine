#include "bench_common.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <limits>

namespace storage_engine::bench {
namespace {

bool parseUint64(std::string_view value, uint64_t &out) {
  uint64_t result = 0;
  if (value.empty()) {
    return false;
  }
  for (auto ch : value) {
    if (ch < '0' || ch > '9') {
      return false;
    }
    result = result * 10 + static_cast<uint64_t>(ch - '0');
  }
  out = result;
  return true;
}

bool parseInt(std::string_view value, int &out) {
  uint64_t parsed = 0;
  if (!parseUint64(value, parsed) || parsed > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
    return false;
  }
  out = static_cast<int>(parsed);
  return true;
}

bool parseBool(std::string_view value, bool &out) {
  if (value == "1" || value == "true") {
    out = true;
    return true;
  }
  if (value == "0" || value == "false") {
    out = false;
    return true;
  }
  return false;
}

void usage() {
  std::cerr << "usage: storage_engine_bench "
            << "--benchmarks=fillseq,fillrandom,readrandom,overwrite,concurrent_fillseq,concurrent_readrandom "
            << "--db=/tmp/storage_engine_bench --num=100000 --reads=100000 "
            << "--threads=1 --key_size=16 --value_size=100 --uring_sqpoll=0 "
            << "--group_commit_window_us=100 --group_commit_target_size=8 --adaptive_group_commit=1\n";
}

double percentileFromSorted(const std::vector<uint64_t> &values, double pct) {
  if (values.empty()) {
    return 0;
  }
  auto index = static_cast<size_t>((pct / 100.0) * static_cast<double>(values.size() - 1));
  return static_cast<double>(values[index]);
}

}  // namespace

std::vector<std::string> split(std::string_view input, char delimiter) {
  std::vector<std::string> parts;
  size_t start = 0;
  while (start <= input.size()) {
    auto end = input.find(delimiter, start);
    if (end == std::string_view::npos) {
      end = input.size();
    }
    parts.emplace_back(input.substr(start, end - start));
    start = end + 1;
    if (end == input.size()) {
      break;
    }
  }
  return parts;
}

bool parseArgs(int argc, char **argv, Options &options) {
  for (int i = 1; i < argc; ++i) {
    std::string_view arg(argv[i]);
    if (arg == "--help") {
      usage();
      return false;
    }

    auto pos = arg.find('=');
    if (pos == std::string_view::npos || !arg.starts_with("--")) {
      std::cerr << "invalid argument: " << arg << "\n";
      usage();
      return false;
    }

    auto key = arg.substr(2, pos - 2);
    auto value = arg.substr(pos + 1);
    uint64_t parsed = 0;

    if (key == "benchmarks") {
      options.benchmarks = std::string(value);
    } else if (key == "db") {
      options.db = std::string(value);
    } else if (key == "num") {
      if (!parseUint64(value, options.num)) {
        return false;
      }
    } else if (key == "reads") {
      if (!parseUint64(value, options.reads)) {
        return false;
      }
    } else if (key == "threads") {
      if (!parseInt(value, options.threads) || options.threads <= 0) {
        return false;
      }
    } else if (key == "key_size") {
      if (!parseInt(value, options.keySize) || options.keySize <= 0) {
        return false;
      }
    } else if (key == "value_size") {
      if (!parseInt(value, options.valueSize) || options.valueSize < 0) {
        return false;
      }
    } else if (key == "seed") {
      if (!parseUint64(value, parsed) || parsed > std::numeric_limits<uint32_t>::max()) {
        return false;
      }
      options.seed = static_cast<uint32_t>(parsed);
    } else if (key == "uring_sqpoll") {
      if (!parseBool(value, options.uringSqPoll)) {
        return false;
      }
    } else if (key == "uring_sqpoll_idle_ms") {
      if (!parseUint64(value, parsed) || parsed > std::numeric_limits<uint32_t>::max()) {
        return false;
      }
      options.uringSqPollIdleMs = static_cast<uint32_t>(parsed);
    } else if (key == "group_commit_window_us") {
      if (!parseUint64(value, options.groupCommitWindowMicros)) {
        return false;
      }
    } else if (key == "group_commit_target_size") {
      if (!parseUint64(value, parsed) || parsed == 0 || parsed > std::numeric_limits<size_t>::max()) {
        return false;
      }
      options.groupCommitTargetSize = static_cast<size_t>(parsed);
    } else if (key == "adaptive_group_commit") {
      if (!parseBool(value, options.adaptiveGroupCommit)) {
        return false;
      }
    } else if (key == "high_pressure_group_commit_window_us") {
      if (!parseUint64(value, options.highPressureGroupCommitWindowMicros)) {
        return false;
      }
    } else if (key == "high_pressure_group_commit_target_size") {
      if (!parseUint64(value, parsed) || parsed == 0 || parsed > std::numeric_limits<size_t>::max()) {
        return false;
      }
      options.highPressureGroupCommitTargetSize = static_cast<size_t>(parsed);
    } else if (key == "high_pressure_group_commit_queue_threshold") {
      if (!parseUint64(value, parsed) || parsed == 0 || parsed > std::numeric_limits<size_t>::max()) {
        return false;
      }
      options.highPressureGroupCommitQueueThreshold = static_cast<size_t>(parsed);
    } else {
      std::cerr << "unknown argument: " << arg << "\n";
      usage();
      return false;
    }
  }

  if (options.reads == 0) {
    options.reads = options.num;
  }
  return true;
}

std::string makeKey(uint64_t value, int keySize) {
  std::string suffix = std::to_string(value);
  if (static_cast<int>(suffix.size()) >= keySize) {
    return suffix;
  }
  return std::string(static_cast<size_t>(keySize) - suffix.size(), '0') + suffix;
}

std::string makeValue(uint64_t value, int valueSize) {
  std::string base = "value_" + std::to_string(value) + "_";
  std::string result;
  result.reserve(static_cast<size_t>(valueSize));
  while (static_cast<int>(result.size()) < valueSize) {
    result += base;
  }
  result.resize(static_cast<size_t>(valueSize));
  return result;
}

void recordLatency(std::vector<uint64_t> &latencyUs, Clock::time_point start) {
  auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count();
  latencyUs.push_back(static_cast<uint64_t>(elapsed));
}

void printMetrics(std::string_view name, const Options &options, const Metrics &metrics) {
  auto opsPerSec = metrics.seconds > 0 ? static_cast<double>(metrics.ops) / metrics.seconds : 0;
  auto mbPerSec = metrics.seconds > 0 ? static_cast<double>(metrics.bytes) / (1024.0 * 1024.0) / metrics.seconds : 0;
  auto avgUs = metrics.ops > 0 ? metrics.seconds * 1000000.0 / static_cast<double>(metrics.ops) : 0;
  auto latencyUs = metrics.latencyUs;
  std::sort(latencyUs.begin(), latencyUs.end());

  std::cout << name << ":\n";
  std::cout << "  ops: " << metrics.ops << "\n";
  std::cout << "  seconds: " << std::fixed << std::setprecision(6) << metrics.seconds << "\n";
  std::cout << "  ops/sec: " << std::fixed << std::setprecision(2) << opsPerSec << "\n";
  std::cout << "  mb/sec: " << std::fixed << std::setprecision(2) << mbPerSec << "\n";
  std::cout << "  avg_us/op: " << std::fixed << std::setprecision(2) << avgUs << "\n";
  std::cout << "  p50_us: " << percentileFromSorted(latencyUs, 50) << "\n";
  std::cout << "  p95_us: " << percentileFromSorted(latencyUs, 95) << "\n";
  std::cout << "  p99_us: " << percentileFromSorted(latencyUs, 99) << "\n";
  std::cout << "  key_size: " << options.keySize << "\n";
  std::cout << "  value_size: " << options.valueSize << "\n";
}

}  // namespace storage_engine::bench
