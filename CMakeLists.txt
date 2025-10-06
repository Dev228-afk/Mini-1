cmake_minimum_required(VERSION 3.16)
project(Mini1Project LANGUAGES CXX)

# C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Warnings
if (MSVC)
  add_compile_options(/W4)
else()
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# OpenMP configuration
set(_openmp_enabled OFF)
set(_using_appleclang OFF)
if (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
  set(_using_appleclang ON)
endif()

find_package(OpenMP)
if (OpenMP_CXX_FOUND)
  message(STATUS "OpenMP found via built-in detection")
  set(_openmp_enabled ON)
endif()

if (NOT _openmp_enabled AND _using_appleclang)
  # AppleClang needs manual wiring to Homebrew's libomp
  message(STATUS "Attempting manual OpenMP setup for AppleClang")

  set(_libomp_search_roots
      /opt/homebrew/opt/libomp
      /usr/local/opt/libomp)

  execute_process(
    COMMAND brew --prefix libomp
    OUTPUT_VARIABLE _brew_libomp_prefix
    ERROR_QUIET
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  if (_brew_libomp_prefix)
    list(APPEND _libomp_search_roots "${_brew_libomp_prefix}")
  endif()

  list(REMOVE_DUPLICATES _libomp_search_roots)

  find_path(LIBOMP_INCLUDE_DIR
    NAMES omp.h
    PATHS ${_libomp_search_roots}
    PATH_SUFFIXES include)

  find_library(LIBOMP_LIBRARY
    NAMES omp libomp
    PATHS ${_libomp_search_roots}
    PATH_SUFFIXES lib)

  if (LIBOMP_INCLUDE_DIR AND LIBOMP_LIBRARY)
    message(STATUS "Configured OpenMP using libomp: ${LIBOMP_LIBRARY}")
    set(_openmp_enabled ON)
  endif()
endif()

add_executable(benchmark
  src/main.cpp
  src/factory/DataSourceFactory.cpp
  src/implementations/VectorDataSource.cpp
  src/implementations/MapDataSource.cpp
  src/utility/CSVParser.cpp
)

target_include_directories(benchmark PRIVATE
  src
  src/interfaces
  src/implementations
  src/utility
  src/factory
)

if (_openmp_enabled)
  if (OpenMP_CXX_FOUND)
    target_link_libraries(benchmark PRIVATE OpenMP::OpenMP_CXX)
  else()
    target_compile_options(benchmark PRIVATE -Xpreprocessor -fopenmp)
    target_link_libraries(benchmark PRIVATE ${LIBOMP_LIBRARY})
    target_include_directories(benchmark PRIVATE ${LIBOMP_INCLUDE_DIR})
  endif()
  target_compile_definitions(benchmark PRIVATE HAS_OPENMP=1)
else()
  message(WARNING "OpenMP support was not detected; benchmarks will execute in serial mode. Install libomp via Homebrew or GCC with OpenMP to enable parallelism.")
endif()

set_target_properties(benchmark PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
)

message(STATUS "Configured Mini1Project successfully")
