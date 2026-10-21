export BUILD_DIR="../build"
export STATS_DIR="/home/mfd4/fdp/paper"


# Baseline
bash run.sh --device /dev/nvme1n1 --log_device /dev/nvme2n1 --benchmark tpcc --tpcc_warehouse_count 1900 --trim --vanilla  --run_for_hours 5

# epsilonDB
bash run.sh --device /dev/nvme1n1 --log_device /dev/nvme2n1 --benchmark tpcc --tpcc_warehouse_count 1900 --trim --run_for_hours 5 --bg_page_fixer_threads 4 --threshold 0.7
