#!/bin/bash


if [ -z "${BUILD_DIR}" ]; then
   echo "Error: please set up BUILD_DIR environment variable."
   exit 1
else
   echo "${BUILD_DIR}"
fi

set -euo pipefail
# set -x

trap 'echo "Error: command failed: $BASH_COMMAND"' ERR

STATS_DIR=/home/mfd4/fdp/paper
# BUILD_DIR="."
# Put all necessary flags into the scripts dir
# SRC_DIR=".." # assume running from build dir
DEVICE_RESET_SCRIPT="$PWD/reset-single-ns.sh"
FLAGS_FILE="$PWD/template.gflag"
DRY_RUN_FLAGS_FILE="$PWD/template.dry_run.gflag"
YCSB_FLAGS_FILE="$PWD/ycsb.gflag"
TPCC_FLAGS_FILE="$PWD/tpcc.gflag"
WAF_SCRIPT="$PWD/calcssdwaf.sh"
PARSE_PY="$PWD/parse_block.py"

device=
# currently un-used
log_device=/dev/nvme3n1
vanilla=0
benchmark=
threshold=0.8
trim=0
sanitize=0
run_for_seconds=7200 # default to 5 hours

worker_threads=96
ru_size=

prefix=""
dry_run=0
stats_dir=1
gdb=0

# TPC-C only flags
tpcc_warehouse_count=
max_log_records_to_discard=7

# YCSB only flags
ycsb_read_ratio=0
ycsb_dead_tuple_ratio=0
distribution=
zipfian_skew=0.8
database_size_gib=
dram_gib=80

subdir=

force_stats_dir=0
force_no_stats_dir=0
load_only=0
run_only=0
description=0
override_stats_dir=0

bg_page_fixer_threads=2

ppl=1

blktrace=0

# whether the device has FDP support or not, this is for samling the WAF.
fdp=0

original_args=("$@")

# TODO(mfd) : Add a usage() method and --help
while [[ $# -gt 0 ]]; do
    case "$1" in
        --device)
            device="$2"
            shift 2
            ;;
        --log_device)
           log_device="$2"
           shift 2
           ;;
        --trim)
            trim=1
            shift 1
            ;;
        --sanitize)
            sanitize=1
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
        --buffer_pool_gib)
            dram_gib="$2"
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
        --bg_page_fixer_threads)
           bg_page_fixer_threads="$2"
           shift 2
           ;;
        --run_for_seconds)
           run_for_seconds="$2"
           shift 2
           ;;
        --worker_threads)
           worker_threads="$2"
           shift 2
           ;;
        --run_for_minutes)
           run_for_seconds=$(( $2 * 60 ))
           shift 2
           ;;
        --run_for_hours)
           run_for_seconds=$(( $2 * 3600 ))
           shift 2
           ;;
        --ycsb_read_ratio)
           ycsb_read_ratio="$2"
           shift 2
           ;;
        --ycsb_dead_tuple_ratio)
           ycsb_dead_tuple_ratio="$2"
           shift 2
           ;;
        --noppl)
           ppl=0
           shift 1
           ;;
        --subdir)
           subdir="$2"
           shift 2
           ;;
        --blktrace)
           blktrace=1
           shift 1
           ;;
        --fdp)
           fdp=1
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

if [ -z "${bg_page_fixer_threads}" ]; then
   if [ "${benchmark}" == "tpcc" ]; then
      bg_page_fixer_threads=4
   else
      bg_page_fixer_threads=2
   fi
fi

if (( missing )); then
    echo "Aborting due to missing required arguments."
    exit 1
fi

if (( dry_run == 0 && trim == 0 && run_only == 0)); then
   echo "You are not doing a dry run but trim is disabled, have you forgotten the option --trim"
   #TODO(mfd) : Add a timeout.
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

if (( ycsb_dead_tuple_ratio > 0 )); then
   skew="${skew}_0.25dead"
fi

# Get controller device by removing 'n' and digits after it
controller=$(echo "${device}" | sed -E 's/n[0-9]+$//')
model=$(lsblk -ndo MODEL ${device} | tr ' ' '_')
echo "$model"

declare -A MODEL_MAP=(
    ["SAMSUNG_MZ1L2960HCJR-00A07"]="PM39A"
    ["Samsung_SSD_980_PRO_1TB"]="980PRO"
    ["MZOL63T8HDLT-00AFB"]="PM9D3a"
    ["Micron_7450_MTFDKBA480TFR"]="M7450Pro"
)

# This is just used by me to avoid forgetting to change the ru size when I change device.
# Therefore pay attention, you may have the same model but different capacity, hence
# different RU SIZE.
declare -A RU_SIZE_MAP=(
    ["Micron_7450_MTFDKBA480TFR"]="500000"
    ["SAMSUNG_MZ1L2960HCJR-00A07"]="1000000"
    ["MZOL63T8HDLT-00AFB"]="3193344"
)

if [[ -n "$subdir" ]]; then
   STATS_DIR="${STATS_DIR}/${subdir}"
fi

short="${MODEL_MAP[$model]}"
echo $short
if [[ -z "$short" ]]; then
   # TODO(mfd) : Prompt me for a mnemonic for the model.
   exit 0
   dir_name="${STATS_DIR}/${prefix}${benchmark}"
else
   dir_name="${STATS_DIR}/${short}/${prefix}${benchmark}"
fi

if [[ -z "${ru_size}" ]]; then
   ru_size="${RU_SIZE_MAP[$model]}"
   echo "Unspecified RU Size, will use ${ru_size}"
fi

if [[ -z "$ru_size" ]]; then
   echo "Please specify an RU size; --ru_size"
   exit 1
fi

if [[ "$benchmark" == "ycsb" ]]; then
   # tib_int=$(( database_size_gib / 1024 ))
   # tib_dec=$(( (database_size_gib * 10 / 1024) % 10 ))
   # database_size_tib="${tib_int}.${tib_dec}"
   util="${database_size_gib}G"
   dir_name="${dir_name}_${skew}_${util}"
else
   util="${tpcc_warehouse_count}whs"
   dir_name="${dir_name}_${util}"
fi

dir_name="${dir_name}_${worker_threads}W"

if (( vanilla == 1 )); then
   dir_name="${dir_name}_vanilla"
else
   dir_name="${dir_name}_${threshold}_discard${max_log_records_to_discard}_epsilondb"
fi


if [[ "$benchmark" == "ycsb" ]]; then
    # BENCHMARK_BINARY="${BUILD_DIR}/frontend/ycsb"
    BENCHMARK_BINARY="./frontend/ycsb"
elif [[ "$benchmark" == "tpcc" ]]; then
    # BENCHMARK_BINARY="${BUILD_DIR}/frontend/tpcc"
    BENCHMARK_BINARY="./frontend/tpcc"
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
    echo "Creating Directory ${dir_name}"
    mkdir -p ${dir_name}
    if (( description == 1)); then
       echo "Please write a description that will go into the stats folder (additional remarks)"
       read -p "> " text_desc
       echo "${text_desc}" > "${dir_name}/description.txt"
       echo "${model}" > "${dir_name}/ssd_model.txt"
    fi
fi

passwd=""
# fdp=0
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
     sudo bash ${WAF_SCRIPT} ${dir_name}/detailed_waf "${device}"
   fi
}

pushd ${BUILD_DIR}

blktrace_ssd_read() {
   echo "$PWD in blktrace"
   if [ -d blktrace_out ]; then
      rm -rf blktrace_out
   fi
   mkdir -p blktrace_out
   # wake up in the last 30 minutes and run for 15 minutes
   sleep $((${run_for_seconds} - 300))
   echo "Woke up to Collect traces"
   # put raw data inside the build dir
   sudo blktrace -d ${device} -a read -o blktrace_out/steady -w 300
}

#TODO(mfd): A temporary flag for common flags.
# which are ssd device, log deice, OP region, RU size


make -j 10

load_flags="load.gflag"
run_flags="run.gflag"
common_flags="common.gflag"

cp "$FLAGS_FILE" "$load_flags"
cp "$FLAGS_FILE" "$run_flags"

{
   echo "--ssd_path=${device}"
   echo "--worker_threads=${worker_threads}"
   echo "--ru_size=${ru_size}"

   echo "--dram_gib=${dram_gib}"

   if [[ "$benchmark" == "ycsb" ]]; then
      echo "--target_gib=${database_size_gib}"
   else
      echo "--tpcc_warehouse_count=${tpcc_warehouse_count}"
   fi
} > "${common_flags}"

cat "${common_flags}" >> "${load_flags}"
cat "${common_flags}" >> "${run_flags}"

if [[ "$benchmark" == "ycsb" ]]; then
   cat "$YCSB_FLAGS_FILE" >> "$load_flags"
   cat "$YCSB_FLAGS_FILE" >> "$run_flags"
   echo "--ycsb_read_ratio=${ycsb_read_ratio}" >> "$run_flags"
   echo "--ycsb_dead_tuple_ratio=${ycsb_dead_tuple_ratio}" >> "$run_flags"
else
   cat "$TPCC_FLAGS_FILE" >> "$load_flags"
   cat "$TPCC_FLAGS_FILE" >> "$run_flags"
fi

{
   echo "--persist"
   echo "--run_for_seconds=0"
   echo "--noenable_discarding"
   echo "--nowal"
   echo "--nowal_pwrite"
   echo "--nobulk_insert"

   # to avoid generating and shuffling the key array
   echo "--zipf_factor=0"

} >> "$load_flags"


{
   echo "--recover"
   echo "--clean_recover"
   echo "--run_for_seconds=${run_for_seconds}"
   echo "--wal_pwrite"
   echo "--bulk_insert"

   echo "--redo_log_file=${log_device}"

   if (( vanilla == 1 )); then
      echo "--noenable_discarding"
      echo "--wal_partition_by=ru_epoch"
   else
      echo "--enable_discarding"
      echo "--ru_gc_threads=${bg_page_fixer_threads}"
      echo "--ru_gc_threshold=${threshold}"
      echo "--wal_partition_by=ru_epoch"
      if (( ppl == 1 )); then
         echo "--per_page_logging"
         echo "--ppl_merge_threshold=4"
         echo "--opportunistic_log_compaction"
         echo "--max_log_records_to_discard=7"
      else
         echo "--noper_page_logging"
         echo "--max_log_records_to_discard=${max_log_records_to_discard}"
      fi
   fi

   if [[ "$benchmark" == "ycsb" ]]; then
      # ycsb benchmark
      if [[ "$distribution" == "zipfian" ]]; then
         echo "--zipf_factor=${zipfian_skew}"
      else
         echo "--zipf_factor=0"
      fi
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


# Make sure we're running as root
# The use of root here is for 2 purpouses, first one can be avoided
# 1. direct access to the block device
# 2. To sample waf from device
if [[ $EUID -ne 0 ]]; then
    echo "This script must be run as root. Re-running with sudo..."
    exec sudo bash "$0" "$original_args"
fi


sanitize_nvme() {
    if (( sanitize )); then
       echo "sanitize"
       sudo nvme sanitize --sanact=2 "${device}"
       sleep 1m
       sudo nvme sanitize-log "${controller}"
       sleep 1m
    fi
    sudo blkdiscard -v "${device}"
    sleep 1m
}

if (( trim == 1)); then
   echo "Trimmimg the device"
   if (( fdp == 1 )); then
      bash ${DEVICE_RESET_SCRIPT} --dev "${controller}"
      NUM_NS=$(sudo nvme list-ns ${controller} -a | wc -l)
      echo $NUM_NS
      for ((i=1; i <= NUM_NS; i++)); do
	 sudo nvme delete-ns ${controller} -n ${i}
      done
      sleep 10
      sudo nvme set-feature ${controller} -f 0x1D -c 0 -s
      sudo nvme set-feature ${controller} -f 0x1D -c 1 -s
      sudo nvme get-feature ${controller} -f 0x1D -H
      sleep 10
      # size_bytes=$(blockdev --getsize64 )
      total_cap=$(nvme id-ctrl "${controller}" | awk -F: '/Total NVM Capacity/ {gsub(/ /,"",$2); print $2}')
      DEV_SIZE=$(( total_cap / 4096 ))
      # This command may change from one device to another depending on the supported formatting
      sudo nvme create-ns ${controller} -b 4096 --nsze=${DEV_SIZE}  --ncap=${DEV_SIZE}
      sudo nvme attach-ns ${controller} --namespace-id=1 --controllers=0x7
      sleep 10
   fi
   sanitize_nvme
fi


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

   if (( blktrace )); then
      blktrace_ssd_read > "${dir_name}/blktrace_ssd_read.out" &
      blktrace_pid=$!
   fi
fi

shutdown() {
    echo "stopping background waf calculator job..."
    set +e

    if (( stats_dir == 1)); then
       cp log_bm.csv ${dir_name}
       cp log_cr.csv ${dir_name}
       cp absorbed_writes_histogram.txt ${dir_name}
       cp "${load_flags}" ${dir_name}
       cp "${run_flags}" ${dir_name}
       pkill -TERM -P "$waf_pid" 2>/dev/null
       kill $waf_pid 2>/dev/null
       wait "$waf_pid"
       if (( blktrace )); then
          wait "$blktrace_pid" || true
          rm blkparse_out
          blkparse -i blktrace_out/steady.blktrace.0 -f "%T.%t %a %S\n" -o blkparse_out
          python3 ${PARSE_PY} blkparse_out > "${dir_name}/blktrace_ssd_latency"
          # mv blkparse_out ${dir_name}
       fi
       echo "results are saved to ${dir_name}"
       popd
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

