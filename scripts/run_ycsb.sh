
# Baseline
# sudo bash BUILD_DIR="../RWDIbuild" run.sh --device /dev/nvme2n1 --log_device /dev/nvme0n1 --benchmark ycsb --database_size 800 --distribution zipfian --trim  --vanilla --zipfian_skew 0.8

# epsilonDB
# sudo bash BUILD_DIR="../RWDIbuild" run.sh --device /dev/nvme2n1 --log_device /dev/nvme0n1 --benchmark ycsb --database_size 800 --distribution zipfian --trim --zipfian_skew 0.8


sudo BUILD_DIR="../build" bash run.sh --device /dev/nvme3n1 --log_device /dev/nvme2n1 --benchmark ycsb --database_size 300 --distribution zipfian --trim --zipfian_skew 0.8 --ycsb_read_ratio 50 --run_for_hours 5


sudo BUILD_DIR="../build" bash run.sh --device /dev/nvme3n1 --log_device /dev/nvme2n1 --benchmark ycsb --database_size 300 --distribution zipfian --trim --zipfian_skew 0.8 --ycsb_read_ratio 50 --run_for_hours 5 --vanilla
