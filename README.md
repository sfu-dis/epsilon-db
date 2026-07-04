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
apt install cmake build-essential nvme-cli libaio-dev liburing-dev libnvme-dev libtbb2-dev
```

Build:

```sh
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=[Release|RelWithDebInfo] ..
make
```

## Benchmarks Examples 
### YCSB
```bash
sudo BUILD_DIR="../build" bash run.sh --device /dev/nvme2n1 --log_device /dev/nvme0n1 --benchmark ycsb --database_size 410 --distribution uniform --trim --run_for_hours 5 --worker_threads 64  --buffer_pool_gib 40
```

### TPC-C

```bash
 sudo BUILD_DIR="../build" bash run.sh --device /dev/nvme2n1 --log_device /dev/nvme0n1 --benchmark tpcc --tpcc_warehouse_count 3800 --trim --run_for_hours 5 --bg_page_fixer_threads 4 --threshold 0.7
 ```

For running the Baseline add `--vanilla` to the above commands (you can remove *epsilonDB* specific flags).

#### Mandatory flags

`--device` SSD device to store database pages.

`--log_device` SSD device to store WAL.

#### *epsilonDB* parameters to configure:

`--bg_page_fixer_threads` The number of background threads that will fix pages will determine the contention between read and writes on the SSD bandwidth. Set up this number depending on your device supported bandwidth and its sensitivity to read/write interference (typical range between [1,4]).

`--threshold` The threshold of pages that needs to be discarded inside a reclaim unit (RU) to kick in background page fixing for this RU. The default is 80%. This threshold depends on the workload access pattern. For stable perforance, run TPC-C with 70% threshold and microbenchmarks you can set higher (80%-90%).

`--noppl` You can use this flag to turn off the PPL optimisation.

`--max_log_records_to_discard` If you turn off PPL, this flag enables you to set the maximum number of log records to discard. Valid numbers are between (0-7), where 0 falls back to the baseline (no discarding).

## License
This project is licensed under the [MIT License](LICENSE).