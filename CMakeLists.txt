cmake_minimum_required(VERSION 3.19)
project(storage_engine VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(-Wall -Wextra -Werror -Wpedantic)

find_package(Threads REQUIRED)

enable_testing()

add_library(storage_engine
  src/common.cc
  src/db.cc
  src/io/uring_executor.cc
  src/memtable.cc
  src/wal.cc
  src/write_batch.cc
)

target_include_directories(storage_engine
  PUBLIC
    ${PROJECT_SOURCE_DIR}/include
  PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(storage_engine
  PUBLIC
    Threads::Threads
)

add_executable(storage_engine_tests
  tests/test_db.cc
)

target_link_libraries(storage_engine_tests
  PRIVATE
    storage_engine
)

target_include_directories(storage_engine_tests
  PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

add_test(NAME storage_engine_tests COMMAND storage_engine_tests)

add_executable(storage_engine_bench
  benchmarks/bench_common.cc
  benchmarks/storage_engine_bench.cc
)

target_link_libraries(storage_engine_bench
  PRIVATE
    storage_engine
)

add_test(
  NAME storage_engine_bench_fillseq_smoke
  COMMAND storage_engine_bench --benchmarks=fillseq --num=8 --value_size=16 --db=/tmp/storage_engine_bench_fillseq_smoke
)

add_test(
  NAME storage_engine_bench_readrandom_smoke
  COMMAND storage_engine_bench --benchmarks=fillseq,readrandom --num=8 --reads=8 --value_size=16 --db=/tmp/storage_engine_bench_readrandom_smoke
)

add_test(
  NAME storage_engine_bench_concurrent_readrandom_smoke
  COMMAND storage_engine_bench --benchmarks=concurrent_readrandom --num=8 --reads=8 --threads=4 --value_size=16 --db=/tmp/storage_engine_bench_concurrent_readrandom_smoke
)
