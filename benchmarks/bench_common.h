#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace storage_engine::bench {

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
  bool uringSqPoll{false};
  uint32_t uringSqPollIdleMs{2000};
  uint64_t groupCommitWindowMicros{100};
  size_t groupCommitTargetSize{8};
  bool adaptiveGroupCommit{true};
  uint64_t highPressureGroupCommitWindowMicros{200};
  size_t highPressureGroupCommitTargetSize{16};
  size_t highPressureGroupCommitQueueThreshold{10};
};

struct Metrics {
  uint64_t ops{0};
  uint64_t bytes{0};
  double seconds{0};
  std::vector<uint64_t> latencyUs;
};

std::vector<std::string> split(std::string_view input, char delimiter);
bool parseArgs(int argc, char **argv, Options &options);
std::string makeKey(uint64_t value, int keySize);
std::string makeValue(uint64_t value, int valueSize);
void recordLatency(std::vector<uint64_t> &latencyUs, Clock::time_point start);
void printMetrics(std::string_view name, const Options &options, const Metrics &metrics);

}  // namespace storage_engine::bench
