#include <sys/uio.h>

#include <atomic>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "storage_engine/db.h"
#include "storage_engine/task.h"
#include "wal.h"

namespace {

std::filesystem::path freshTestDir(const std::string &name) {
  auto dir = std::filesystem::temp_directory_path() / ("storage_engine_" + name);
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  return dir;
}

void testPutGetAndRecover() {
  auto dir = freshTestDir("put_get_recover");

  {
    auto db = storage_engine::DB::Open(dir.string()).value();
    assert(db->Put("alpha", "one").ok());
    assert(db->Put("beta", "two").ok());

    auto alpha = db->Get("alpha").value();
    auto beta = db->Get("beta").value();
    assert(alpha == "one");
    assert(beta == "two");
  }

  {
    auto db = storage_engine::DB::Open(dir.string()).value();
    auto alpha = db->Get("alpha").value();
    auto beta = db->Get("beta").value();
    assert(alpha == "one");
    assert(beta == "two");
  }
}

void testDeleteSurvivesRecovery() {
  auto dir = freshTestDir("delete_recover");

  {
    auto db = storage_engine::DB::Open(dir.string()).value();
    assert(db->Put("key", "value").ok());
    assert(db->Delete("key").ok());
    assert(db->Get("key").error().code() == storage_engine::StatusCode::kNotFound);
  }

  {
    auto db = storage_engine::DB::Open(dir.string()).value();
    assert(db->Get("key").error().code() == storage_engine::StatusCode::kNotFound);
  }
}

void testWriteBatchIsAtomic() {
  auto dir = freshTestDir("write_batch");
  auto db = storage_engine::DB::Open(dir.string()).value();

  storage_engine::WriteBatch batch;
  batch.Put("a", "1");
  batch.Put("b", "2");
  batch.Delete("a");

  assert(db->Write(batch).ok());
  assert(db->Get("a").error().code() == storage_engine::StatusCode::kNotFound);
  assert(db->Get("b").value() == "2");
}

void testRecoveryTruncatesTornWalTailBeforeAppending() {
  auto dir = freshTestDir("torn_tail_append");

  {
    auto db = storage_engine::DB::Open(dir.string()).value();
    assert(db->Put("stable", "value").ok());
  }

  {
    std::ofstream wal(dir / "wal.log", std::ios::binary | std::ios::app);
    wal.write("torn", 4);
  }

  {
    auto db = storage_engine::DB::Open(dir.string()).value();
    assert(db->Get("stable").value() == "value");
    assert(db->Put("after_torn", "new_value").ok());
  }

  {
    auto db = storage_engine::DB::Open(dir.string()).value();
    assert(db->Get("stable").value() == "value");
    assert(db->Get("after_torn").value() == "new_value");
  }
}

void testConcurrentWritesRecover() {
  auto dir = freshTestDir("concurrent_writes");

  {
    auto db = storage_engine::DB::Open(dir.string()).value();
    std::vector<std::thread> threads;
    threads.reserve(8);
    for (int i = 0; i < 8; ++i) {
      threads.emplace_back([&db, i] {
        auto key = "key_" + std::to_string(i);
        auto value = "value_" + std::to_string(i);
        assert(db->Put(key, value).ok());
      });
    }
    for (auto &thread : threads) {
      thread.join();
    }
  }

  {
    auto db = storage_engine::DB::Open(dir.string()).value();
    for (int i = 0; i < 8; ++i) {
      auto key = "key_" + std::to_string(i);
      auto value = "value_" + std::to_string(i);
      assert(db->Get(key).value() == value);
    }
  }
}

void testAsyncWriteUsesCoroutineCompletionAndPersistentUring() {
  auto dir = freshTestDir("async_persistent_uring");
  auto db = storage_engine::DB::Open(dir.string()).value();

  storage_engine::WriteBatch batch;
  batch.Put("async_key", "async_value");

  auto status = db->WriteAsync(std::move(batch)).run();
  assert(status.ok());
  assert(db->Get("async_key").value() == "async_value");

  auto stats = db->DebugStatsForTest();
  assert(stats.uringExecutorCreations == 1);
  assert(stats.asyncWriterSuspensions == 1);
  assert(stats.uringCompletionLoopCompletions >= 2);
  assert(stats.memtableReservedBuckets >= 4096);
  assert(stats.inlineWriterDrains > 0);
  assert(stats.writerThreadDrains == 0);
  assert(!stats.uringSqPollEnabled);
}

void testWalEncodeBuffersAreReusedAcrossWrites() {
  auto dir = freshTestDir("wal_encode_buffer_reuse");
  auto db = storage_engine::DB::Open(dir.string()).value();

  assert(db->Put("first", "value").ok());
  assert(db->Put("second", "value").ok());

  auto stats = db->DebugStatsForTest();
  assert(stats.walEncodeFixedCapacity > 0);
  assert(stats.walEncodeIovecCapacity > 0);
  assert(stats.walEncodeBufferReuses > 0);
}

void testConcurrentWritesUseGroupCommitWindow() {
  auto dir = freshTestDir("group_commit_window");
  auto db = storage_engine::DB::Open(dir.string()).value();
  constexpr int kThreads = 16;
  std::atomic<int> ready{0};
  std::atomic<bool> start{false};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&db, &ready, &start, i] {
      ready.fetch_add(1, std::memory_order_release);
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      assert(db->Put("group_key_" + std::to_string(i), "value").ok());
    });
  }
  while (ready.load(std::memory_order_acquire) != kThreads) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);
  for (auto &thread : threads) {
    thread.join();
  }

  auto stats = db->DebugStatsForTest();
  assert(stats.groupCommitWaits > 0);
  assert(stats.maxWriteGroupSize > 1);
  assert(stats.memtableApplyLocks > 0);
  assert(stats.memtableApplyLocks < kThreads);
  assert(stats.groupCommitTargetSize == 8);
  assert(stats.groupCommitWindowMicros == 100);
}

void testWalEncodedBatchSizeMatchesRecord() {
  storage_engine::WriteBatch first;
  first.Put("alpha", "one");
  first.Delete("beta");
  storage_engine::WriteBatch second;
  second.Put("gamma", "three");

  std::vector<const storage_engine::WriteBatch *> batches{&first, &second};
  auto encoded = storage_engine::wal::EncodeBatch(42, batches);

  assert(storage_engine::wal::EncodedBatchSize(batches) == encoded.size());
  auto decoded = storage_engine::wal::DecodeLog(std::span<const std::byte>(encoded));
  assert(decoded);
  assert(decoded.value().validBytes == encoded.size());
  assert(decoded.value().batches.size() == 1);
  assert(decoded.value().batches[0].baseSequence == 42);
  assert(decoded.value().batches[0].batch.entries().size() == 3);
}

void testWalCrc32MatchesStandardVector() {
  constexpr std::string_view input = "123456789";
  auto bytes = std::span<const std::byte>(reinterpret_cast<const std::byte *>(input.data()), input.size());
  assert(storage_engine::wal::Crc32(bytes) == 0xcbf43926u);
}

std::vector<std::byte> materializeIovecs(std::span<const iovec> iovecs) {
  std::vector<std::byte> bytes;
  for (const auto &iov : iovecs) {
    auto *data = static_cast<const std::byte *>(iov.iov_base);
    bytes.insert(bytes.end(), data, data + iov.iov_len);
  }
  return bytes;
}

void testWalFragmentEncodingMatchesContiguousRecord() {
  storage_engine::WriteBatch first;
  first.Put("alpha", "one");
  first.Delete("beta");
  storage_engine::WriteBatch second;
  second.Put("gamma", "three");

  std::vector<const storage_engine::WriteBatch *> batches{&first, &second};
  auto contiguous = storage_engine::wal::EncodeBatch(7, batches);
  auto fragmented = storage_engine::wal::EncodeBatchFragments(7, batches);

  assert(fragmented.size == contiguous.size());
  assert(fragmented.iovecs.size() > 1);
  assert(materializeIovecs(fragmented.iovecs) == contiguous);
}

storage_engine::Task<int> immediateValue() { co_return 7; }

storage_engine::Task<int> nestedImmediateValue() {
  auto child = immediateValue();
  co_return co_await child;
}

void testNestedInlineTaskCompletesOnce() {
  auto task = nestedImmediateValue();
  assert(task.run() == 7);
}

}  // namespace

int main() {
  testPutGetAndRecover();
  testDeleteSurvivesRecovery();
  testWriteBatchIsAtomic();
  testRecoveryTruncatesTornWalTailBeforeAppending();
  testConcurrentWritesRecover();
  testAsyncWriteUsesCoroutineCompletionAndPersistentUring();
  testWalEncodeBuffersAreReusedAcrossWrites();
  testConcurrentWritesUseGroupCommitWindow();
  testWalEncodedBatchSizeMatchesRecord();
  testWalCrc32MatchesStandardVector();
  testWalFragmentEncodingMatchesContiguousRecord();
  testNestedInlineTaskCompletesOnce();
  return 0;
}
