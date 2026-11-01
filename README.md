<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="logo/logo.svg">
    <source media="(prefers-color-scheme: dark)" srcset="logo/logo-dark.svg">
    <img alt="epsilonDB logo" src="logo/logo.svg" height="80">
  </picture>
</div>
<br>

# *epsilonDB*

*epsilonDB* is an OLTP engine tailored for the charachteristics of flash-based SSDs. It acheives low SSD write amplification without relying on large over-provisioning. *epsilonDB* relies on discarding dirty pages upon eviction while keeping the necessary log records to reconstrut discarded pages. This allows *epsilonDB* to gain flexibility in imposing a flash-friendly write pattern that significantly reduces SSD write amplification.

This is the artifact for the paper:
> **epsilonDB: Towards Zero Write Amplification for Modern OLTP**

> Mohamed Farouk Drira, Tianzheng Wang and Jonghyeok Park.
> *Under submission*.

## Building

Required dependencies:

```sh
apt install cmake build-essential nvme-cli liburing-dev libnvme-dev libtbb2-dev
```

Build:

```sh
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=[Release|RelWithDebInfo] ..
make
```

## Infer Superblock Size

*epsilonDB*'s flash-friendly write pattern relies on approximation of the reclaim unit (RU), which can be assumed to match the device's superblock size. The RU size is passed with `--ru_size`, in 4 KiB pages. `scripts/run.sh` already knows the RU size for the SSDs used in the paper (`RU_SIZE_MAP`); for any other device, pass it explicitly or add the model to that map.
To estimate the superblock size of a new device, use `scripts/superblock_probe.sh`. It splits the drive into aligned chunks of the candidate size, fills it, then overwrites it chunk by chunk in random order. If the candidate matches the real superblock size, each overwrite invalidates whole superblocks, so device GC has nothing to relocate and the WAF stays close to 1.
On FDP compliant devices, the RU size can be directly read from the controller (using `nvme cli` for example).  

```bash
cd scripts
make            # builds get_waf, which reads the host/media write counters (requires the OCP SMART log page C0h)
sudo ./superblock_probe.sh -d /dev/nvme2n1 -r 500000 -y
```

Run it with several candidate values of `-r` (in 4 KiB pages). Use the smallest value that gives a WAF of about 1 as `--ru_size`. Run `./superblock_probe.sh` without arguments to see all options. The script requires `fio`.

Rule of thumb: for 1TB device start with 4GB. Scale linearly, i.e, 2TB => 8GB. 

> **Warning:** the probe sanitizes, trims and overwrites the whole device, destroying all of its data.

## Benchmarks Examples 

After building, from inside the `scripts/` directory, run the following command by passing the build directory as environment variable.
note : the script will ask for sudo password once to `chmod` the devices, and to calculate WAF from the device.  The binary itself does not require root privelage.

### YCSB
```bash
BUILD_DIR="../build" bash run.sh --device /dev/nvme2n1 --log_device /dev/nvme0n1 --benchmark ycsb --database_size 410 --distribution uniform --trim --run_for_hours 5 --worker_threads 64  --buffer_pool_gib 40
```

### TPC-C

```bash
 BUILD_DIR="../build" bash run.sh --device /dev/nvme2n1 --log_device /dev/nvme0n1 --benchmark tpcc --tpcc_warehouse_count 3800 --trim --run_for_hours 5 --bg_page_fixer_threads 4 --threshold 0.7
 ```

For running the Baseline add `--vanilla` to the above commands (you can remove *epsilonDB* specific flags).
More examples can be found under the `scripts/`

#### Mandatory flags

`--device` SSD device to store database pages.

`--log_device` SSD device to store WAL.

#### *epsilonDB* parameters to configure:

`--bg_page_fixer_threads` The number of background threads that will fix pages will determine the contention between read and writes on the SSD bandwidth. Set up this number depending on your device supported bandwidth and its sensitivity to read/write interference (typical range between [1,4]).

`--threshold` The threshold of pages that needs to be discarded inside a reclaim unit (RU) to kick in background page fixing for this RU. The default is 80%. This threshold depends on the workload access pattern. For stable perforance, run TPC-C with 70% threshold and microbenchmarks you can set higher (80%-90%).

`--noppl` You can use this flag to turn off the PPL optimisation.

`--max_log_records_to_discard` If you turn off PPL, this flag enables you to set the maximum number of log records to discard. Valid numbers are between (0-7), where 0 falls back to the baseline (no discarding).

#### FDP Support

*epsilonDB* implements Flexible Data Placement (FDP) support through [libfdp](https://github.com/Driramohamedfarouk/libfdp), a lightweight library we implemented. It is fetched and built automatically by CMake (`-DWITH_FDP=ON`, the default).

`--enable_fdp` This requires to run the binary with root. This is because I/O is issued using NVMe Passthrough which requires root privilage from kernel versiom 6.2.

`--fdp_placement` Different placement policies are defined under `backend/leanstore/storage/fdp/placement-policies`. The one used in the paper is `FrozenRecords`.

`--fdp_reformat` Besides `--trim` this reformats the FDP device. Together with  `--fdp_nruh` to specify the number of RUH. For example, `FrozenRecords` requires formatting the namespace with 3 RUHs (or 4 if the WAL is colocated on the same namespace).

`--fdp_waf` Uses `nvme fdp stats` to calculate the WAF instead of OCP.

> **Note:** FDP is not fully supported with `--bg_page_fixer_variant=2` (the default), which only tracks the pages written to the default RUH. With any placement policy other than `Default`, use variant 1 by adding the following flags to `scripts/template.gflag`:
> ```
> --bg_page_fixer_variant=1
> --nobg_page_fixer_tree_lookup
> --nobg_page_fixer_log_io_uring
> ```


#### WAL on the same device

To store the WAL on the same namespace as the database pages, pass the same device to `--device` and `--log_device`. This places the WAL in a disjoint LBA range at the end of the namespace, and turns on `--enable_fdp` so that the WAL is written to its own RUH (remember to account for it in `--fdp_nruh`).

`--log_space_pct` Percentage of the namespace reserved for the WAL (default 10%). Add it to `scripts/template.gflag` to change it.

### Compression Support (Experimental)

`--compress_pages` compresses pages with zstd on write-back and packs them out-of-place into 4 KiB blocks.

note : With discarding, it requires `--bg_page_fixer_variant=2` (the default).

YCSB's default payload is random and does not compress. Use `--ycsb_payload zero` or `--ycsb_payload repeat` to generate compressible pages, for example:

```bash
BUILD_DIR="../build" bash run.sh --device /dev/nvme2n1 --log_device /dev/nvme0n1 --benchmark ycsb --database_size 410 --distribution uniform --trim --run_for_hours 5 --threshold 0.8 --compress_pages --ycsb_payload repeat
```

## License
This project is licensed under the [MIT License](LICENSE).