#!/bin/bash

set -euo pipefail
# set -x

trap 'echo "Error: command failed: $BASH_COMMAND"' ERR


STATS_DIR=/home/mfd4/fdp/paper
SRC_DIR=".." # assume running from build dir
BUILD_DIR="."
DEVICE_RESET_SCRIPT="${SRC_DIR}/scripts/reset-single-ns.sh"
FLAGS_FILE="${SRC_DIR}/template.gflag"
DRY_RUN_FLAGS_FILE="${SRC_DIR}/template.dry_run.gflag"
YCSB_FLAGS_FILE="${SRC_DIR}/ycsb.gflag"
TPCC_FLAGS_FILE="${SRC_DIR}/tpcc.gflag"

device=
vanilla=0
benchmark=
distribution=
zipfian_skew=0.8
database_size_gib=
threshold=0.8
trim=0
prefix=""
dry_run=0
stats_dir=1
gdb=0

tpcc_warehouse_count=
max_log_records_to_discard=7

force_stats_dir=0
force_no_stats_dir=0
load_only=0
run_only=0
description=0
override_stats_dir=0

original_args=("$@")

# TODO(mfd) : Add a usage() method and --help
while [[ $# -gt 0 ]]; do
    case "$1" in
        --device)
            device="$2"
            shift 2
            ;;
        --trim)
            trim=1
            shift 1
            ;;
        --vanilla)
            vanilla=1
            shift 1
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
        --max_log_records_to_discard)
            max_log_records_to_discard="$2"
            shift 2
            ;;
        --prefix)
            prefix="$2"
            shift 2
            ;;
        --dry_run)
            FLAGS_FILE="$DRY_RUN_FLAGS_FILE"
            dry_run=1
            shift 1
            ;;
        --debug)
            gdb=1
            shift 1
            ;;
        --no_stats_dir)
           stats_dir=0
           force_no_stats_dir=1
           shift 1
           ;;
        --stats_dir)
           stats_dir=1
           force_stats_dir=1
           shift 1
           ;;
        --load_only)
           load_only=1
           shift 1
           ;;
        --run_only)
           run_only=1
           shift 1
           ;;
        --description)
           description=1
           shift 1
           ;;
        --override_stats_dir)
           override_stats_dir=1
           shift 1
           ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

if (( dry_run )); then
   trim=0
   if (( force_stats_dir == 1)); then
      stats_dir=1
   else
      stats_dir=0
   fi
fi

missing=0

if [[ -z "${device:-}" ]]; then
    echo "Error: --device is required"
    missing=1
fi

# if [[ -z "${trim:-}" ]]; then
#    echo "Error: --trim is required"
#    missing=1
# fi


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

# if [[ -z "${vanilla:-}" ]]; then
#    echo "Error: --vanilla is required"
#    missing=1
# fi

if (( vanilla == 0 )); then
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

if (( dry_run == 0 && trim == 0 && run_only == 0)); then
   echo "You are not doing a dry run but trim is disabled, have you forgotten the option --trim"
   read -p "Continue? [Y/y], Trim device? [t], type anything else to abort: " answer
   case "$answer" in
        [Yy]* )
            echo "Continuing..."
            ;;
        [t]* )
            trim=1
            echo "Will trim device, Continuing..."
            ;;
        * )
            echo "Aborting."
            exit 1
            ;;
   esac
fi

if (( run_only == 1 && trim == 1)); then
   echo "You are requesting to trim the device and to run only without loading"
   echo "You're probably wrong. Aborting..."
   exit 1
fi

skew="${distribution}"

if [[ "$distribution" == "zipfian" ]]; then
   skew="zipf${zipfian_skew}"
fi

dir_name="${STATS_DIR}/${prefix}${benchmark}"

if [[ "$benchmark" == "ycsb" ]]; then
   tib_int=$(( database_size_gib / 1024 ))
   tib_dec=$(( (database_size_gib * 10 / 1024) % 10 ))
   database_size_tib="${tib_int}.${tib_dec}"
   util="${database_size_tib}TiB"
   dir_name="${dir_name}_${skew}_${util}"
else
   util="${tpcc_warehouse_count}whs"
   dir_name="${dir_name}_${util}"
fi


if (( vanilla == 1 )); then
   dir_name="${dir_name}_vanilla"
else
   dir_name="${dir_name}_${threshold}_discard${max_log_records_to_discard}_epsilondb"
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


# if (( 0 )); then
if [[ -d "$dir_name" && "$stats_dir" -eq 1 && "$override_stats_dir" -eq 0 ]]; then
    echo "Directory Already Exists. Are you sure, you want to override those results ?"
    read -p "Override previous results [y/N]: " answer

    case "$answer" in
        [Yy]* )
            echo "Overriding existing results..."
            rm -rf "${dir_name}"
            ;;
        * )
            echo "Aborting."
            exit 1
            ;;
    esac
fi

# echo "$dir_name"
if (( stats_dir == 1)); then
    mkdir -p "$dir_name"
    echo "Creating Directory ${dir_name}"
    if (( description == 1)); then
       echo "Please write a description that will go into the stats folder (additional remarks)"
       read -p "> " text_desc
       echo "${text_desc}" > "${dir_name}/description.txt"
    fi
fi

passwd=""
fdp=0
waf() {
   # echo "Background WAF sampling process: starting"
   if (( fdp == 1 )); then
      while true; do
         wafstr="$(echo ${passwd} | sudo -S nvme fdp stats ${device} -e 1)"
         echo "${wafstr}"
         echo "${wafstr}" | awk '/(HBMW)/ {hbmw = $7} /(MBMW)/ {mbmw = $7} END {if (hbmw == 0) print 0; else print mbmw / hbmw}'
         echo "${wafstr}" | awk '/(HBMW)/ {hbmw= $7} /(MBMW)/ {mbmw = $7} END {print mbmw-hbmw}'
         sleep 600s
      done;
   else
     sudo bash ${SRC_DIR}/scripts/calcssdwaf.sh ${dir_name}/detailed_waf "${device}"
   fi
}

load_flags="load.gflag"
run_flags="run.gflag"

cp "$FLAGS_FILE" "$load_flags"
cp "$FLAGS_FILE" "$run_flags"

if [[ "$benchmark" == "ycsb" ]]; then
   cat "$YCSB_FLAGS_FILE" >> "$load_flags"
   cat "$YCSB_FLAGS_FILE" >> "$run_flags"
else
   cat "$TPCC_FLAGS_FILE" >> "$load_flags"
   cat "$TPCC_FLAGS_FILE" >> "$run_flags"
fi

{
   echo "--ssd_path=${device}"
   echo "--persist"
   echo "--run_for_seconds=0"
   echo "--noenable_discarding"
   echo "--nowal"
   echo "--nowal_pwrite"
   # echo "--wal_partition_by=page"

   if [[ "$benchmark" == "ycsb" ]]; then
      echo "--target_gib=${database_size_gib}"
   else
      echo "--tpcc_warehouse_count=${tpcc_warehouse_count}"
   fi
} >> "$load_flags"


{
   echo "--ssd_path=${device}"
   echo "--recover"
   echo "--clean_recover"
   # echo "--run_for_seconds=36000" # 8 hours
   echo "--run_for_seconds=57600" # 16 hours
   echo "--wal_pwrite"
   if (( vanilla == 1 )); then
      echo "--noenable_discarding"
      echo "--wal_partition_by=ru_epoch"
   else
      echo "--enable_discarding"
      echo "--ru_gc_threads=4"
      echo "--ru_gc_threshold=${threshold}"
      echo "--wal_partition_by=ru_epoch"
      echo "--max_log_records_to_discard=${max_log_records_to_discard}"
   fi

   if [[ "$benchmark" == "tpcc" ]]; then
      echo "--tpcc_warehouse_count=${tpcc_warehouse_count}"
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

# turn the flags file to a single line of arguments
flagfile_to_line() {
    local file="$1"
    local out=()

    while IFS= read -r line; do
        line="${line%%#*}"

        line="$(echo "$line" | xargs)"

        [[ -z "$line" ]] && continue

        out+=( "$line" )
    done < "$file"

    printf "%s " "${out[@]}"
}


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

# Get controller device by removing 'n' and digits after it
controller=$(echo "${device}" | sed -E 's/n[0-9]+$//')

sanitize_nvme() {
    echo "sanitize"
    sudo nvme sanitize --sanact=2 "${device}"
    sleep 1m
    sudo nvme sanitize-log "${controller}"
    sleep 1m
    sudo blkdiscard -v "${device}"
    sleep 1m
}

if (( trim == 1)); then
   echo "Trimmimg the device"
   sanitize_nvme
fi

make -j 10

# Validate flags before running long experiments.
sudo ${BENCHMARK_BINARY} --validate_flags_and_exit $(flagfile_to_line "$load_flags")
sudo ${BENCHMARK_BINARY} --validate_flags_and_exit $(flagfile_to_line "$run_flags")


# sudo ${BENCHMARK_BINARY} --flagfile="$load_flags"
if (( run_only == 0 )); then
   sudo ${BENCHMARK_BINARY} $(flagfile_to_line "$load_flags") | tee load_log.txt
fi

if (( load_only == 1 )); then
   exit 0
fi

# lauch the WAF calculator in the background
if (( stats_dir == 1 )); then
   waf > "${dir_name}/waf" &
   waf_pid=$!
fi

shutdown() {
    echo "stopping background waf calculator job..."
    set +x

    if (( stats_dir == 1)); then
       cp log_bm.csv ${dir_name}
       cp log_cr.csv ${dir_name}
       cp absorbed_writes_histogram.txt ${dir_name}
       pkill -TERM -P "$waf_pid" 2>/dev/null
       kill $waf_pid 2>/dev/null
       wait "$waf_pid"
    fi
    exit 0
}

trap shutdown INT
trap shutdown EXIT
trap shutdown TERM

PREFIX=
if (( gdb == 1 )); then
    PREFIX="gdb --ex run --args "
fi

sudo ${PREFIX}${BENCHMARK_BINARY} $(flagfile_to_line "$run_flags")
# while true; do
#   sleep 10
#    echo "G"
# done;


