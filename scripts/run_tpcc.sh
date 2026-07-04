
# Baseline
sudo BUILD_DIR="../RWDIbuild" bash run.sh --device /dev/nvme0n1 --log_device /dev/nvme2n1 --benchmark tpcc --tpcc_warehouse_count 15000 --trim --vanilla  --run_for_hours 5

# epsilonDB
sudo BUILD_DIR="../RWDIbuild" bash run.sh --device /dev/nvme0n1 --log_device /dev/nvme2n1 --benchmark tpcc --tpcc_warehouse_count 15000 --trim --run_for_hours 5 --bg_page_fixer_threads 4 --threshold 0.7
