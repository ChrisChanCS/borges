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
| `dependencies` | Borges's CXL kernel module, initialization/recovery tools, Cxlalloc, and runtime libraries |
| [emulation/](https://github.com/ut-datasys/tigon/tree/master/emulation) | Symlink to Tigon's CXL pod emulation tools |
| `third_party/tigon` | Tigon submodule |
| `scripts` | Build, deployment, and experiment helpers |

Before each experiment, update `src/common/config.json` and upload it with
`./scripts/run.sh sync 8`. The backend is selected at build time by `use_rdma`;
rebuild when changing it.

## Hardware requirements

Both configurations below use Ubuntu 22.04. Tigon documents two ways to provide
the shared memory used by the CXL pod:

| Configuration | Host requirements | Shared-memory backing |
| --- | --- | --- |
| Real CXL memory | At least 40 cores in one socket, with CXL memory connected to that socket | A CXL memory device exposed as a NUMA node |
| CXL emulation with NUMA memory | A two-socket machine with at least 40 cores per socket | DRAM on the remote NUMA node; a CXL device is optional |

The default setup launches eight VMs with five vCPUs each and a shared 64 GiB
memory region. Provision enough memory for that region and the VMs themselves.

## Build environment for Borges

Borges uses [Tigon's emulation tools](https://github.com/ut-datasys/tigon/tree/master/emulation)
to represent the hosts of a CXL pod as eight VMs on one physical machine. All
VMs access the same shared-memory region. That region can be backed by either
real CXL memory or remote NUMA DRAM. With a CXL device, the memory accesses use
physical CXL memory while the VMs provide the pod's host environment. With NUMA
DRAM, the setup emulates shared CXL memory; latency and bandwidth depend on the
machine's NUMA topology.

The `emulation/` symlink points to `third_party/tigon/emulation`. Tigon provides
the VM image builder and shared-memory emulation tools. The VM's CXL kernel
module and initialization/recovery tools come from Borges's own
`dependencies/kernel_module/` and are installed in step 3.

Run all commands below from the Borges repository root. The VM image and
launch commands follow the pinned version of
[Tigon's setup guide](https://github.com/ut-datasys/tigon/blob/ccd567a50116b7bada06df71a3bf0a07c424572e/README.md#setup-vm-based-cxl-pod-emulation-from-scratch).

### 1. Prepare the host and VM image

Initialize the Tigon submodule, run Borges's host setup, then build the VM
image through the `emulation/` symlink:

```bash
git submodule update --init --depth 1 third_party/tigon
./scripts/setup.sh HOST
./emulation/image/make_vm_img.sh
```

### 2. Launch VMs with either memory configuration

Run one of the following alternatives from the Borges repository root. Use
`numactl --hardware` to inspect the host's NUMA nodes.

#### Option A: real CXL memory

Expose the CXL device's memory as system RAM, then allocate the shared region
on its NUMA node. This is Tigon's example with device `dax0.0` and CXL NUMA
node `2`; replace both values with those for your host:

```bash
sudo daxctl reconfigure-device --mode=system-ram dax0.0 --force
sudo ./emulation/start_vms.sh --using-old-img --cxl 0 5 8 0 2
```

#### Option B: emulate CXL memory using remote NUMA DRAM

Use the other socket's DRAM to back the shared region. Tigon's example uses
NUMA node `1` and enables its uncore frequency setup:

```bash
sudo ./emulation/start_vms.sh --using-old-img --cxl 0 5 8 1 1
```

The five positional values after `--cxl` are:

| Argument | Meaning in these examples |
| --- | --- |
| `host_id` | `0`: identifier of the physical host running the VMs |
| `num_cpus` | `5`: vCPUs per VM |
| `num_vms` | `8`: number of VMs |
| `configure_uncore_freq` | `0`: skip Tigon's uncore frequency setup; `1`: enable it |
| `shmem_dir_numa` | NUMA node backing the shared region: `2` for the CXL example, `1` for the remote DRAM example |

Both commands use `--cxl` to select Tigon's shared-memory VM configuration.
The final NUMA-node argument determines where the shared memory is allocated.
The launch script configures 64 GiB of shared memory and 10 GiB of private
memory per VM. `--using-old-img` reuses the prepared VM image and existing VM
directories.

### 3. Install the Borges VM dependencies

The Borges experiment scripts expect a shared 64 GiB CXL region and root SSH
access through `127.0.0.1` ports `10022` through `10029`. Once the VMs are running,
use Borges's setup script to install its CXL driver and runtime dependencies
before running an experiment:

```bash
./scripts/setup.sh VMS 8
```

This command copies the following files from Borges to every VM and loads
`/root/cxl_ivpci.ko`:

| Borges file | Destination in each VM |
| --- | --- |
| `dependencies/kernel_module/cxl_ivpci.ko` | `/root/cxl_ivpci.ko` |
| `dependencies/kernel_module/cxl_init` | `/root/cxl_init` |
| `dependencies/kernel_module/cxl_recover_meta` | `/root/cxl_recover_meta` |

The experiment scripts use these deployed tools to initialize the shared
64 GiB region and recover allocator metadata.

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

The appropriate `request_worker_num` depends on the workload. As a starting
point, we recommend `1` for low-load experiments and `2` for high-load
experiments. This setting controls the number of request workers per shard
server.

After every change to `src/common/config.json`, run the following command from
the Borges repository root to synchronize the updated configuration to all
eight VMs before starting the workload:

```bash
./scripts/run.sh sync 8
```

## Baselines

Borges compares with two advanced shared-log systems, [Scalog](https://github.com/scalog/scalog) and [Boki](https://github.com/ut-osa/boki) , both of which are replication-first. 
