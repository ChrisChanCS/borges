# Borges: A Low-Latency Distributed Shared Log on a CXL Memory/SSD Hybrid

Borges is a research distributed shared log over a shared CXL memory/SSD hybrid. The codebase borrows [Tigon](https://github.com/ut-datasys/tigon)'s and [Cxlalloc](https://github.com/nwtnni/cxlalloc)'s underlying infrastructures.

## Folder structure

| Directory | Contents |
| --- | --- |
| `src/common` | Shared layouts, cache operations, leases, global cuts, and I/O helpers |
| `src/sequencer` | Global ordering and index publication |
| `src/shard_server` | Request handling, batching, replication, and reads |
| `src/benchmark`, `src/workload` | Workload definitions and executable entry points |
| `src/rdma` | Optional RDMA backend and diagnostic tools |
| `dependencies` | Cxlalloc dependencies and runtime libraries |
| [emulation/](https://github.com/ut-datasys/tigon/tree/master/emulation) | Symlink to Tigon's CXL pod emulation tools |
| `third_party/tigon` | Tigon submodule |
| `scripts` | Build, deployment, and experiment helpers |

Before each experiment, update `src/common/config.json` and upload it with
`./scripts/run.sh sync 8`. The backend is selected at build time by `use_rdma`;
rebuild when changing it.

## Hardware requirement

1. A CXL device, CXL memory and CMM-H are both OK
2. A machine with at least 40 cores in one socket
3. Ubuntu 22.04

## Build environment for Borges

Borges uses [Tigon's emulation tools](https://github.com/ut-datasys/tigon/tree/master/emulation)
to emulate a CXL pod with eight VMs. The `emulation/` symlink points to
`third_party/tigon/emulation`. Initialize the pinned Tigon submodule after cloning:

```bash
git submodule update --init --depth 1 third_party/tigon
```

Follow
[Tigon's VM setup guide](https://github.com/ut-datasys/tigon#setup-vm-based-cxl-pod-emulation-from-scratch)
from `third_party/tigon/` to prepare the host, build the VM image, and launch the VMs.

The Borges experiment scripts expect a shared 64 GiB CXL region and root SSH
access through `127.0.0.1` ports `10022` through `10029`. Once the VMs are running,
run the following from the Borges repository to install its CXL driver and
runtime dependencies:

```bash
./scripts/setup.sh VMS 8
```

## Compile Borges

The evaluated build uses Linux/x86-64, Clang 17, LLD 17, CMake 3.18 or newer,
and C++20. Install development headers/libraries for Boost, jemalloc, glog,
gflags, spdlog/fmt, nlohmann JSON, and CRoaring (validated with 1.3.0). RDMA additionally needs
libibverbs. CMake fetches the pinned xxHash and Abseil versions on its first run;
the Cxlalloc library is provided under `dependencies/cxlalloc`.

Build locally without accessing any VM:

```bash
cmake -S . -B build
cmake --build build -j 4
```

The default executables are `sequencer`, `server`, and `client`. The CXL backend
requires an x86-64 CPU supporting CLFLUSHOPT and CLWB. To upload binaries and
configuration to the running VMs, use `./scripts/run.sh sync 8`.

The default backend comes from `src/common/config.json`. To build RDMA without
changing that file, copy it to a separate file, set `use_rdma` to `true`, and run:

```bash
cmake -S . -B build-rdma -DRHODES_CONFIG_FILE=/absolute/path/to/rdma-config.json
cmake --build build-rdma -j 4
```

Use a separate build directory per backend. Applications read `config.json`
from their working directory at runtime; deploy a configuration that matches
the compiled backend. An RDMA build also provides `rdma_mem_server` and
`validate_append`, with `rdma_test` and `dump_roots` available as explicit
diagnostic targets.

## Minimal working example

Run the command below to start log-append microbenchmark.

```bash
./test-append.sh 1
```

You will see `Test complete; all processes cleaned up` once the example completes successfully.

## Reproduction with one command

The entire reproduction requires ~3 hours by using the command below.

```bash
./run_all.sh
```

Experiment scripts write output to `result/`, which is kept locally and ignored by Git.

## Per-workload

Each workload has an individual script for evaluation. The name is `test-xxx.sh` where `xxx` is the workload name. Before running a different workload, please update `src/common/config.json` according to the setup to test.

## Baselines

Borges compares with two advanced shared-log systems, [Scalog](https://github.com/scalog/scalog) and [Boki](https://github.com/ut-osa/boki) , both of which are replication-first. 
