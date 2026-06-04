#!/bin/bash

DEVICES=("/dev/nvme2n1" )  # List of SSD devices
TEST_SIZE=$((700 * 1024 * 1024 * 1024))  # 128GB test size
NUMJOBS=1  # Number of parallel threads
RUNTIME=60  # Test runtime in seconds
# BLOCK_SIZES=(4096 8192)  # 0.5 KiB to 8 KiB
BLOCK_SIZES=(4096)  # 0.5 KiB to 8 KiB



# Ensure the script runs as root
if [ "$(id -u)" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

run_only=0

while [[ $# -gt 0 ]]; do
   case "$1" in
      --run_only) run_only=1; shift 1;;
      *) echo "Unknown argument: $1"; exit 1;;
    esac
done

# Loop through each device
for DEVICE in "${DEVICES[@]}"; do
    DEVICE_NAME=$(basename "$DEVICE")  # Extract device name for output file naming
    OUTPUT_FILE="latency_throughput_iops_results_${DEVICE_NAME}.txt"

    # Remove previous results
     rm -f *_latency.json $OUTPUT_FILE

     if (( run_only == 0 )); then
        echo "Discarding existing data on $DEVICE..."
        sudo blkdiscard $DEVICE

        echo "Filling $DEVICE with sequential writes before testing random reads..."

       # Prefill the device with sequential writes using fio
       sudo fio --name=write_test \
           --filename=$DEVICE \
           --direct=1 \
           --ioengine=libaio \
           --rw=write \
           --bs=1M \
           --size=$TEST_SIZE \
           --iodepth=64 \
           --numjobs=32 \
           --output-format=json \
           --offset_increment=$((TEST_SIZE / NUMJOBS)) > "${DEVICE_NAME}_write.json"

       echo "Sequential write complete for $DEVICE. Proceeding with random read tests..."

    fi
    # Measure Read Latency, Throughput, and IOPS for each block size
    for BS in "${BLOCK_SIZES[@]}"; do
        echo "Measuring ${BS}-byte read latency, throughput & IOPS on $DEVICE with $NUMJOBS threads..."

if (( 1 )); then
        numactl --cpunodebind=1 --membind=1 fio --name=read_test_${BS} \
            --filename=$DEVICE \
            --direct=1 \
            --ioengine=libaio \
            --rw=randrw \
            --rwmixread=100 \
            --bs=${BS} \
            --size=$TEST_SIZE \
            --iodepth=1 \
            --numjobs=$NUMJOBS \
            --time_based \
            --runtime=$RUNTIME \
            --output-format=json \
            --offset_align=4096 > "${DEVICE_NAME}_${BS}_read.json"

        echo "${BS}-byte read results on $DEVICE:" | tee -a $OUTPUT_FILE
fi

        # Extract and log mean latency (in nanoseconds)
        RLATENCY=$(jq '[.jobs[].read.lat_ns.mean // 0] | add / (length | if . == 0 then 1 else . end)' "${DEVICE_NAME}_${BS}_read.json")
        WLATENCY=$(jq '[.jobs[].write.lat_ns.mean // 0] | add / (length | if . == 0 then 1 else . end)' "${DEVICE_NAME}_${BS}_read.json")
        echo "Mean Latency: R: $RLATENCY ns, W: $WLATENCY" | tee -a $OUTPUT_FILE

        # Extract and log throughput (in MB/s)
        # RTHROUGHPUT=$(jq '[.jobs[].read.bw_bytes // 0] | add / (length | if . == 0 then 1 else . end) / (1024 * 1024)' "${DEVICE_NAME}_${BS}_read.json")
        # WTHROUGHPUT=$(jq '[.jobs[].write.bw_bytes // 0] | add / (length | if . == 0 then 1 else . end) / (1024 * 1024)' "${DEVICE_NAME}_${BS}_read.json")
        RTHROUGHPUT=$(jq '[.jobs[].read.bw_bytes // 0] | add / (1024 * 1024)' "${DEVICE_NAME}_${BS}_read.json")
        WTHROUGHPUT=$(jq '[.jobs[].write.bw_bytes // 0] | add / (1024 * 1024)' "${DEVICE_NAME}_${BS}_read.json")
        echo "Throughput: $RTHROUGHPUT MB/s, $WTHROUGHPUT MB/s" | tee -a $OUTPUT_FILE

        # Extract and log IOPS
        RIOPS=$(jq '[.jobs[].read.iops // 0] | add' "${DEVICE_NAME}_${BS}_read.json")
        WIOPS=$(jq '[.jobs[].write.iops // 0] | add' "${DEVICE_NAME}_${BS}_read.json")
        echo "RIOPS: $RIOPS, WIOPS : $WIOPS" | tee -a $OUTPUT_FILE

        echo "" | tee -a $OUTPUT_FILE
    done

    echo "Results for $DEVICE saved to $OUTPUT_FILE"
done

echo "All tests completed!"
