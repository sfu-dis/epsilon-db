
WORKERS=(32 64 96)

set -x

for worker in "${WORKERS[@]}"
do
   # Baseline
   sudo BUILD_DIR="../RWDIbuild" bash run.sh --device /dev/nvme3n1 --log_device /dev/nvme0n1 --benchmark ycsb --database_size 760 --distribution zipfian --trim  --vanilla --override_stats_dir --run_for_hours 2 --worker_threads ${worker} --subdir scalability
   sleep 60
   # epsilonDB
   sudo BUILD_DIR="../RWDIbuild" bash run.sh --device /dev/nvme3n1 --log_device /dev/nvme0n1 --benchmark ycsb --database_size 760 --distribution zipfian --trim --override_stats_dir --run_for_hours 2 --worker_threads ${worker} --subdir scalability
   sleep 60
done


