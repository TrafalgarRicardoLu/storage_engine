#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "storage_engine/db.h"

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
  std::string benchmarks{"fillseq"};
  std::string db{"/tmp/storage_engine_bench"};
  uint64_t num{100000};
  uint64_t reads{0};
  int threads{1};
  int keySize{16};
  int valueSize{100};
  uint32_t seed{301};
};

struct Metrics {
  uint64_t ops{0};
  uint64_t bytes{0};
  double seconds{0};
  std::vector<uint64_t> latencyUs;
};

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

void usage() {
  std::cerr << "usage: storage_engine_bench "
            << "--benchmarks=fillseq,fillrandom,readrandom,overwrite,concurrent_fillseq "
            << "--db=/tmp/storage_engine_bench --num=100000 --reads=100000 "
            << "--threads=1 --key_size=16 --value_size=100\n";
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

double percentile(std::vector<uint64_t> values, double pct) {
  if (values.empty()) {
    return 0;
  }
  std::sort(values.begin(), values.end());
  auto index = static_cast<size_t>((pct / 100.0) * static_cast<double>(values.size() - 1));
  return static_cast<double>(values[index]);
}

void printMetrics(std::string_view name, const Options &options, const Metrics &metrics) {
  auto opsPerSec = metrics.seconds > 0 ? static_cast<double>(metrics.ops) / metrics.seconds : 0;
  auto mbPerSec = metrics.seconds > 0 ? static_cast<double>(metrics.bytes) / (1024.0 * 1024.0) / metrics.seconds : 0;
  auto avgUs = metrics.ops > 0 ? metrics.seconds * 1000000.0 / static_cast<double>(metrics.ops) : 0;

  std::cout << name << ":\n";
  std::cout << "  ops: " << metrics.ops << "\n";
  std::cout << "  seconds: " << std::fixed << std::setprecision(6) << metrics.seconds << "\n";
  std::cout << "  ops/sec: " << std::fixed << std::setprecision(2) << opsPerSec << "\n";
  std::cout << "  mb/sec: " << std::fixed << std::setprecision(2) << mbPerSec << "\n";
  std::cout << "  avg_us/op: " << std::fixed << std::setprecision(2) << avgUs << "\n";
  std::cout << "  p50_us: " << percentile(metrics.latencyUs, 50) << "\n";
  std::cout << "  p95_us: " << percentile(metrics.latencyUs, 95) << "\n";
  std::cout << "  p99_us: " << percentile(metrics.latencyUs, 99) << "\n";
  std::cout << "  key_size: " << options.keySize << "\n";
  std::cout << "  value_size: " << options.valueSize << "\n";
}

std::unique_ptr<storage_engine::DB> openFreshDb(const Options &options) {
  if (options.db.empty() || options.db == "/") {
    std::cerr << "refuse to use unsafe db path: " << options.db << "\n";
    return nullptr;
  }
  std::filesystem::remove_all(options.db);
  std::filesystem::create_directories(options.db);
  auto db = storage_engine::DB::Open(options.db);
  if (!db) {
    std::cerr << "open db failed: " << db.error().message() << "\n";
    return nullptr;
  }
  return std::move(db).value();
}

bool preload(storage_engine::DB &db, const Options &options) {
  for (uint64_t i = 0; i < options.num; ++i) {
    auto status = db.Put(makeKey(i, options.keySize), makeValue(i, options.valueSize));
    if (!status.ok()) {
      std::cerr << "preload failed: " << status.message() << "\n";
      return false;
    }
  }
  return true;
}

bool benchFillSeq(const Options &options) {
  auto db = openFreshDb(options);
  if (!db) {
    return false;
  }

  Metrics metrics;
  metrics.latencyUs.reserve(static_cast<size_t>(options.num));
  auto start = Clock::now();
  for (uint64_t i = 0; i < options.num; ++i) {
    auto opStart = Clock::now();
    auto key = makeKey(i, options.keySize);
    auto value = makeValue(i, options.valueSize);
    auto status = db->Put(key, value);
    if (!status.ok()) {
      std::cerr << "fillseq failed: " << status.message() << "\n";
      return false;
    }
    recordLatency(metrics.latencyUs, opStart);
    metrics.bytes += key.size() + value.size();
  }
  metrics.seconds = std::chrono::duration<double>(Clock::now() - start).count();
  metrics.ops = options.num;
  printMetrics("fillseq", options, metrics);
  return true;
}

bool benchFillRandom(const Options &options) {
  auto db = openFreshDb(options);
  if (!db) {
    return false;
  }

  Metrics metrics;
  metrics.latencyUs.reserve(static_cast<size_t>(options.num));
  std::mt19937_64 rng(options.seed);
  auto start = Clock::now();
  for (uint64_t i = 0; i < options.num; ++i) {
    auto keyIndex = rng() % options.num;
    auto opStart = Clock::now();
    auto key = makeKey(keyIndex, options.keySize);
    auto value = makeValue(i, options.valueSize);
    auto status = db->Put(key, value);
    if (!status.ok()) {
      std::cerr << "fillrandom failed: " << status.message() << "\n";
      return false;
    }
    recordLatency(metrics.latencyUs, opStart);
    metrics.bytes += key.size() + value.size();
  }
  metrics.seconds = std::chrono::duration<double>(Clock::now() - start).count();
  metrics.ops = options.num;
  printMetrics("fillrandom", options, metrics);
  return true;
}

bool benchReadRandom(const Options &options) {
  auto db = openFreshDb(options);
  if (!db || !preload(*db, options)) {
    return false;
  }

  Metrics metrics;
  metrics.latencyUs.reserve(static_cast<size_t>(options.reads));
  std::mt19937_64 rng(options.seed);
  uint64_t hits = 0;
  auto start = Clock::now();
  for (uint64_t i = 0; i < options.reads; ++i) {
    auto key = makeKey(rng() % options.num, options.keySize);
    auto opStart = Clock::now();
    auto value = db->Get(key);
    if (value) {
      ++hits;
      metrics.bytes += key.size() + value.value().size();
    }
    recordLatency(metrics.latencyUs, opStart);
  }
  metrics.seconds = std::chrono::duration<double>(Clock::now() - start).count();
  metrics.ops = options.reads;
  printMetrics("readrandom", options, metrics);
  std::cout << "  hits: " << hits << "\n";
  std::cout << "  misses: " << (options.reads - hits) << "\n";
  return true;
}

bool benchOverwrite(const Options &options) {
  auto db = openFreshDb(options);
  if (!db || !preload(*db, options)) {
    return false;
  }

  Metrics metrics;
  metrics.latencyUs.reserve(static_cast<size_t>(options.num));
  std::mt19937_64 rng(options.seed);
  auto start = Clock::now();
  for (uint64_t i = 0; i < options.num; ++i) {
    auto opStart = Clock::now();
    auto key = makeKey(rng() % options.num, options.keySize);
    auto value = makeValue(i + options.num, options.valueSize);
    auto status = db->Put(key, value);
    if (!status.ok()) {
      std::cerr << "overwrite failed: " << status.message() << "\n";
      return false;
    }
    recordLatency(metrics.latencyUs, opStart);
    metrics.bytes += key.size() + value.size();
  }
  metrics.seconds = std::chrono::duration<double>(Clock::now() - start).count();
  metrics.ops = options.num;
  printMetrics("overwrite", options, metrics);
  return true;
}

bool benchConcurrentFillSeq(const Options &options) {
  auto db = openFreshDb(options);
  if (!db) {
    return false;
  }

  Metrics metrics;
  metrics.latencyUs.resize(static_cast<size_t>(options.num));
  std::vector<std::thread> threads;
  threads.reserve(static_cast<size_t>(options.threads));
  std::atomic<bool> failed{false};
  std::atomic<uint64_t> bytes{0};

  auto start = Clock::now();
  for (int thread = 0; thread < options.threads; ++thread) {
    threads.emplace_back([&, thread] {
      for (auto i = static_cast<uint64_t>(thread); i < options.num; i += static_cast<uint64_t>(options.threads)) {
        auto opStart = Clock::now();
        auto key = makeKey(i, options.keySize);
        auto value = makeValue(i, options.valueSize);
        auto status = db->Put(key, value);
        if (!status.ok()) {
          failed.store(true, std::memory_order_relaxed);
          return;
        }
        metrics.latencyUs[static_cast<size_t>(i)] = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - opStart).count());
        bytes.fetch_add(key.size() + value.size(), std::memory_order_relaxed);
      }
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }
  if (failed.load(std::memory_order_relaxed)) {
    std::cerr << "concurrent_fillseq failed\n";
    return false;
  }
  metrics.seconds = std::chrono::duration<double>(Clock::now() - start).count();
  metrics.ops = options.num;
  metrics.bytes = bytes.load(std::memory_order_relaxed);
  printMetrics("concurrent_fillseq", options, metrics);
  std::cout << "  threads: " << options.threads << "\n";
  return true;
}

bool runBenchmark(std::string_view name, const Options &options) {
  if (name == "fillseq") {
    return benchFillSeq(options);
  }
  if (name == "fillrandom") {
    return benchFillRandom(options);
  }
  if (name == "readrandom") {
    return benchReadRandom(options);
  }
  if (name == "overwrite") {
    return benchOverwrite(options);
  }
  if (name == "concurrent_fillseq") {
    return benchConcurrentFillSeq(options);
  }

  std::cerr << "unknown benchmark: " << name << "\n";
  return false;
}

}  // namespace

int main(int argc, char **argv) {
  Options options;
  if (!parseArgs(argc, argv, options)) {
    return 1;
  }

  std::cout << "storage_engine_bench\n";
  std::cout << "db=" << options.db << "\n";
  std::cout << "benchmarks=" << options.benchmarks << "\n";
  std::cout << "num=" << options.num << "\n";
  std::cout << "reads=" << options.reads << "\n";
  std::cout << "threads=" << options.threads << "\n";
  std::cout << "sync=true\n\n";

  for (const auto &benchmark : split(options.benchmarks, ',')) {
    if (!runBenchmark(benchmark, options)) {
      return 1;
    }
  }
  return 0;
}
