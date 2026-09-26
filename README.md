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
| `dependencies`, `emulation` | Cxlalloc dependencies and CXL pod emulation |
| `scripts` | Build, deployment, and experiment helpers |

Before each experiment, update `src/common/config.json` and upload it with
`./scripts/run.sh sync 8`. The backend is selected at build time by `use_rdma`;
rebuild when changing it.

The `emulation/` directory is from [Tigon](https://github.com/ut-datasys/tigon), which is used for emulating CXL memory with a NUMA node. Please refer to Tigon's repo to learn how to use it.


## Hardware requirement

1. A CXL device, CXL memory and CMM-H are both OK
2. A machine with at least 40 cores in one socket
3. Ubuntu 22.04

## Build environment for Borges

Borges follows Tigon's exact method to build the environment, where 8 VMs are emulated as a CXL pod.

1. Host setup
```bash
./scripts/setup.sh HOST
```
2. Build VM image
```bash
./emulation/image/make_vm_img.sh
```
3. Launch VMs
```bash
sudo daxctl reconfigure-device --mode=system-ram dax0.0 --force # manage CXL memory as a CPU-less NUMA node
sudo ./emulation/start_vms.sh --using-old-img --cxl 0 5 8 0 2 # replace the last argument with the NUMA node number of CXL memory (e.g., 2)
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
