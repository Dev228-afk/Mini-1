# mini_1 Benchmark Harness

This project benchmarks different data-source implementations while exercising OpenMP-based parallelism. The build now auto-detects OpenMP support on macOS and Linux; if OpenMP cannot be configured, the code still builds but runs in serial mode and prints a warning.

## Prerequisites

- CMake 3.16+
- A C++17 compiler with OpenMP support
- OpenMP runtime (`libomp`) installed

### macOS (Apple Silicon or Intel)

1. Install Homebrew if you have not already.
2. Install the OpenMP runtime:

   ```sh
   brew install libomp
   ```

3. Configure normally; the project searches common Homebrew locations and automatically enables OpenMP when `libomp` is present:

   ```sh
   cmake -S . -B build
   ```

### Linux

Ensure your distribution provides `libomp` (LLVM) or the GNU OpenMP runtime. Typical packages include `libomp-dev`, `libgomp1`, or GCC toolchains built with OpenMP enabled. Then configure with:

```sh
cmake -S . -B build
```

## Build

Once prerequisites are satisfied:

```sh
cmake --build build
```

## Running

Execute the benchmark binary from the build directory, providing the desired dataset and options:

```sh
/usr/bin/time -l ./build/benchmark Data/worldbank/worldbank.csv map --year 2019 --min 1e5 --max 1e7 --threads 8
```

If OpenMP is available, the `mode` column in the CSV output will switch to `parallel` whenever more than one thread is active. If you see a message about OpenMP not being available, revisit the steps above to install and select an OpenMP-capable compiler.
