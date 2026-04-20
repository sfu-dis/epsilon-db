#!/bin/bash

set -euo pipefail
# set -x

trap 'echo "Error: command failed: $BASH_COMMAND"' ERR


STATS_DIR=/home/mfd4/fdp/paper
SRC_DIR=".." # assume running from build dir
BUILD_DIR="."
FLAGS_FILE="${SRC_DIR}/template.gflag"

device=
vanilla=true
benchmark=
distribution=
zipfian_skew=
database_size_gib=
threshold=
trim=false

tpcc_warehouse_count=

original_args=("$@")

while [[ $# -gt 0 ]]; do
    case "$1" in
        --device)
            device="$2"
            shift 2
            ;;
        --trim_device)
            trim="$2"
            shift 2
            ;;
        --vanilla)
            vanilla="$2"
            shift 2
            ;;
        --benchmark)
            benchmark="$2"
            shift 2
            ;;
        --distribution)
            distribution="$2"
            shift 2
            ;;
        --zipfian_skew)
            zipfian_skew="$2"
            shift 2
            ;;
        --database_size)
            database_size_gib="$2"
            shift 2
            ;;
        --tpcc_warehouse_count)
            tpcc_warehouse_count="$2"
            shift 2
            ;;
        --threshold)
            threshold="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

missing=0

if [[ -z "${device:-}" ]]; then
    echo "Error: --device is required"
    missing=1
fi

if [[ -z "${trim:-}" ]]; then
    echo "Error: --trim_device is required"
    missing=1
fi


if [[ -z "${benchmark:-}" ]]; then
    echo "Error: --benchmark is required"
    missing=1
elif [[ "$benchmark" != "ycsb" && "$benchmark" != "tpcc" ]]; then
    echo "Error: benchmark should be either ycsb or tpcc"
    exit 1
fi

if [[ "$benchmark"  == "ycsb" ]]; then
   if [[ -z "${database_size_gib:-}" ]]; then
       echo "Error: --database_size is required"
       missing=1
   fi
else
   # TODO(mfd) : Probably consider estimating the wh count from target gib
   if [[ -z "${tpcc_warehouse_count:-}" ]]; then
       echo "Error: --tpcc_warehouse_count is required"
       missing=1
   fi
fi

if [[ -z "${vanilla:-}" ]]; then
    echo "Error: --vanilla is required"
    missing=1
fi

if [[ "$vanilla" == "false" ]]; then
   if [[ -z "${threshold:-}" ]]; then
       echo "Error: --threshold is required"
       missing=1
   fi
fi

if [[ "$distribution" == "zipfian" ]]; then
   if [[ -z "${zipfian_skew:-}" ]]; then
       echo "Error: --zipfian_skew is required"
       missing=1
   fi
fi

if (( missing )); then
    echo "Aborting due to missing required arguments."
    exit 1
fi

skew="${distribution}"

if [[ "$distribution" == "zipfian" ]]; then
   skew="zipf${zipfian_skew}"
fi

if [[ "$benchmark" == "ycsb" ]]; then
   tib_int=$(( database_size_gib / 1024 ))
   tib_dec=$(( (database_size_gib * 10 / 1024) % 10 ))
   database_size_tib="${tib_int}.${tib_dec}"
   util="${database_size_tib}TiB"
   dir_name="${STATS_DIR}/${benchmark}_${skew}_${util}"
else
   util="${tpcc_warehouse_count}whs"
   dir_name="${STATS_DIR}/${benchmark}_${util}"
fi


if [[ "$vanilla" == true ]]; then
   dir_name="${dir_name}_vanilla"
else 
   dir_name="${dir_name}_${threshold}"
fi


if [[ "$benchmark" == "ycsb" ]]; then
    BENCHMARK_BINARY="${BUILD_DIR}/frontend/ycsb"
elif [[ "$benchmark" == "tpcc" ]]; then
    BENCHMARK_BINARY="${BUILD_DIR}/frontend/tpcc"
else
    echo "Error: benchmark should be either ycsb or tpcc"
    exit 1
fi

#TODO(mfd) : Either run for a period of time or until a fixed number of media writes

if [[ -d "$dir_name" ]]; then
    echo "Directory Already Exists. Are you sure, you want to override those results ?"
    read -p "Override previous results [y/N]: " answer

    case "$answer" in
        [Yy]* )
            echo "Overriding existing results..."
            ;;
        * )
            echo "Aborting."
            exit 1
            ;;
    esac
fi

# echo "$dir_name"
mkdir -p "$dir_name"

passwd=""
waf() {
   echo "Background WAF sampling process: starting"
   while true; do
      wafstr="$(echo ${passwd} | sudo -S nvme fdp stats ${device} -e 1)"
      echo "${wafstr}"
      echo "${wafstr}" | awk '/(HBMW)/ {hbmw = $7} /(MBMW)/ {mbmw = $7} END {if (hbmw == 0) print 0; else print mbmw / hbmw}'
      echo "${wafstr}" | awk '/(HBMW)/ {hbmw= $7} /(MBMW)/ {mbmw = $7} END {print mbmw-hbmw}'
      sleep 600s
   done;
}

load_flags="load.gflag"
run_flags="run.gflag"

cp "$FLAGS_FILE" "$load_flags"
cp "$FLAGS_FILE" "$run_flags"

{
   echo "--ssd_path=${device}"
   echo "--persist"
   echo "--run_for_seconds=0"
   echo "--noenable_discarding"
   echo "--nowal_pwrite"
   echo "--wal_partition_by=page"
   
   if [[ "$benchmark" == "ycsb" ]]; then
      echo "--target_gib=${database_size_gib}"
   else
      echo "--tpcc_warehouse_count=${tpcc_warehouse_count}"
   fi
} >> "$load_flags"


{
   echo "--ssd_path=${device}"
   echo "--recover"
   echo "--run_for_seconds=14400" # 8 hours
   echo "--noycsb_warmup"
   echo "--wal_pwrite"
   if [[ "$vanilla" == "true" ]]; then
      echo "--noenable_discarding"
      echo "--wal_partition_by=page"
   else
      echo "--enable_discarding"
      echo "--ru_gc_threads=4"
      echo "--ru_gc_threshold=${threshold}"
      echo "--wal_partition_by=ru_epoch"
   fi

   if [[ "$benchmark" == "tpcc" ]]; then
      echo "--steady_tpcc"
   else
      # ycsb benchmark
      if [[ "$distribution" == "zipfian" ]]; then
         echo "--zipf_factor=${zipfian_skew}"
      else
         echo "--zipf_factor=0"
      fi
      echo "--target_gib=${database_size_gib}"
   fi

} >> "$run_flags"

# exit 0

# Make sure we're running as root
# The use of root here is for 3 purpouses, first 2 can be avoided
# 1. direct access to the block device
# 2. IO passthrough requires root privelages in kernel version < 6.12
# 3. To sample waf from device
if [[ $EUID -ne 0 ]]; then
    echo "This script must be run as root. Re-running with sudo..."
    exec sudo bash "$0" "$original_args"
fi

# TODO(mfd) : Take the controller name from the device name
if [[ "$trim" == "true" ]]; then
   echo "Trimmimg the device"
   bash /home/mfd4/fdp/reset-single-ns.sh --dev /dev/nvme0
fi

make -j 10

# Use a flags file
# Generate from it the load and run flag files

# sudo gdb --ex run --args ./frontend/ycsb --flagfile="$load_flags"


sudo ${BENCHMARK_BINARY} --flagfile="$load_flags"

# lauch the WAF calculator in the background
waf > "${dir_name}/waf" &
waf_pid=$!

shutdown() {
    echo "stopping background waf calculator job..."

    cp log_bm.csv ${dir_name}
    cp log_cr.csv ${dir_name}
    kill "$waf_pid" 2>/dev/null
    wait "$waf_pid"
    exit 0
}

trap shutdown INT
trap shutdown EXIT

sudo ${BENCHMARK_BINARY} --flagfile="$run_flags"
# while true; do
#   sleep 10
#    echo "G"
# done;


