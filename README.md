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
| Real CXL memory | At least 40 cores in one socket, with CXL memory connected to that socket, and an SR-IOV-capable Ethernet NIC (Intel X550 in our experiments) | A CXL memory device exposed as a NUMA node |
| CXL emulation with NUMA memory | A two-socket machine with at least 40 cores per socket | DRAM on the remote NUMA node; a CXL device is optional |

The default setup launches eight VMs with five vCPUs each and a shared 64 GiB/16 GiB 
memory region for emulated/real CXL device. Provision enough memory for that region and the VMs themselves.

## Build environment for Borges

Borges uses [Tigon's emulation tools](https://github.com/ut-datasys/tigon/tree/master/emulation)
to represent the hosts of a CXL pod as eight VMs on one physical machine. All
VMs access the same shared-memory region. That region can be backed by either
real CXL memory or remote NUMA DRAM. With a CXL device, the memory accesses use
physical CXL memory while the VMs provide the pod's host environment. Borges's
real-CXL setup uses SR-IOV Ethernet virtual functions (VFs) for communication
between VMs. With NUMA DRAM, the setup emulates shared CXL memory; latency and
bandwidth depend on the machine's NUMA topology.

The `emulation/` symlink points to `third_party/tigon/emulation`. Tigon provides
the VM image builder and shared-memory emulation tools. The VM's CXL kernel
module and initialization/recovery tools come from Borges's own
`dependencies/kernel_module/` and are installed in step 3.

Run all commands below from the Borges repository root. The setup uses the
pinned version of [Tigon's setup guide](https://github.com/ut-datasys/tigon/blob/ccd567a50116b7bada06df71a3bf0a07c424572e/README.md#setup-vm-based-cxl-pod-emulation-from-scratch)
and the `--sriov` option in its
[VM launcher](https://github.com/ut-datasys/tigon/blob/ccd567a50116b7bada06df71a3bf0a07c424572e/emulation/start_vms.sh#L41).

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
`numactl --hardware` to inspect the host's NUMA nodes. The launcher requires
existing QEMU VMs to be stopped before starting a new group.

#### Option A: real CXL memory with SR-IOV networking

Borges's real-CXL experiments used an **Intel X550** NIC with SR-IOV for
VM-to-VM networking.

**Launcher compatibility:** The pinned Tigon launcher's `--sriov` path currently
supports Mellanox/NVIDIA ConnectX NICs. Its NIC/VF discovery and host driver
handling are specific to ConnectX and `mlx5_core`; using it with Intel X550
requires adapting those parts. The unmodified launcher does not support the
Intel X550 setup used in our experiments.

The following instructions cover the launcher's ConnectX path. It assigns one
Ethernet VF to each VM through `vfio-pci`, while continuing to expose the
shared CXL region through IVSHMEM.

Before launching on a ConnectX host, prepare the SR-IOV environment:

- Enable SR-IOV in the NIC firmware and enable the IOMMU in the host firmware
  and kernel (for example, `intel_iommu=on` on Intel hosts). See
  [NVIDIA's SR-IOV setup guide](https://networking-docs.nvidia.com/mlnxofedswum/590590/single-root-io-virtualization-sr-iov)
  and the [Linux VFIO documentation](https://docs.kernel.org/driver-api/vfio.html).
- Use a ConnectX NIC in Ethernet mode with `mlx5_core` available on the host
  and in the VM image. The launcher selects the first ConnectX Ethernet
  physical function (PF) reported by `lshw`. It requests 16 VFs when none are
  enabled; if VFs already exist, at least eight must be available for this
  eight-VM setup.
- Install the host tools used for device discovery, VFIO binding, and VM
  preparation in addition to the host setup in step 1:

```bash
sudo apt-get install -y driverctl lshw pciutils iproute2 iptables qemu-system-x86 libguestfs-tools daxctl
```

Expose the CXL device's memory as system RAM, then allocate the shared region
on its NUMA node. The example below uses device `dax0.0` and CXL NUMA node `2`;
replace both values with those for your host:

```bash
sudo daxctl reconfigure-device --mode=system-ram dax0.0 --force
sudo ./emulation/start_vms.sh --using-old-img --sriov 0 5 8 0 2
```

For `host_id=0`, the launcher configures the VF interfaces with addresses
`192.168.100.2` through `192.168.100.9` for VM-to-VM traffic. SSH management
remains available through `127.0.0.1:10022` through `127.0.0.1:10029` on a
separate virtual network. After boot, check `lspci -k` and `ip -br address`
inside the VMs and verify connectivity between their VF addresses.

#### Option B: emulate CXL memory using remote NUMA DRAM with TAP networking

Use the other socket's DRAM to back the shared region. Tigon's example uses
NUMA node `1` and enables its uncore frequency setup. This command uses a
virtio/TAP bridge for VM-to-VM traffic:

```bash
sudo ./emulation/start_vms.sh --using-old-img --cxl 0 5 8 1 1
```

The five positional values after `--sriov` or `--cxl` are:

| Argument | Meaning in these examples |
| --- | --- |
| `host_id` | `0`: identifier of the physical host running the VMs |
| `num_cpus` | `5`: vCPUs per VM |
| `num_vms` | `8`: number of VMs |
| `configure_uncore_freq` | `0`: skip Tigon's uncore frequency setup; `1`: enable it |
| `shmem_dir_numa` | NUMA node backing the shared region: `2` for the CXL example, `1` for the remote DRAM example |

Both modes provide the same shared-memory VM configuration: `--sriov` selects
SR-IOV networking, and `--cxl` selects virtio/TAP networking. The final
NUMA-node argument determines where the shared memory is allocated.
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

## Global-cut buffer

This implementation replaces the global-cut ring buffer described in the paper
with a linear buffer. This preserves the complete history of global cuts so
newly joined shard servers can access earlier entries. Our tests showed no
measurable performance impact from this change.

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
