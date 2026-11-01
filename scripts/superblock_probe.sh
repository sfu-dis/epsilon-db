#!/usr/bin/env bash
#
# Estimate the superblock (reclaim unit) size of an NVMe SSD.
#
# Split the drive into aligned chunks of RU_SIZE pages. Fill it once, then
# overwrite it chunk by chunk in random order, flushing after each chunk.
# If RU_SIZE == the real superblock size, each overwrite invalidates one
# whole superblock, GC has nothing to relocate, and WAF ~= 1.
#
# Run with several RU_SIZE values. The value that gives WAF ~= 1 is the answer.
#
# Counters come from get_waf, which reads them out of the NVMe log pages as
# binary. Build it first with `make`.
#
# WARNING: destroys all data on the device.

set -euo pipefail

DEV=""; RU=0; PS=4096; ROUNDS=4; PCT=100; QD=32; SEED=1
ENGINE=io_uring; TRIM=1; YES=0; GETWAF=./get_waf

usage() {
  cat <<EOF
usage: $0 -d DEV -r RU_SIZE [options]
  -d DEV      block device, e.g. /dev/nvme0n1
  -r RU_SIZE  candidate superblock size, in pages
  -p SIZE     page size in bytes (default $PS)
  -n N        overwrite rounds (default $ROUNDS)
  -x PCT      percent of each superblock rewritten per round (default $PCT)
  -q QD       queue depth inside one superblock (default $QD)
  -s SEED     random seed (default $SEED)
  -e ENGINE   fio ioengine (default $ENGINE)
  -g PATH     path to the get_waf binary (default $GETWAF)
  -T          skip the trim
  -y          confirm data loss
EOF
  exit 1
}

while getopts "d:r:p:n:x:q:s:e:g:Ty" o; do
  case $o in
    d) DEV=$OPTARG ;; r) RU=$OPTARG ;; p) PS=$OPTARG ;; n) ROUNDS=$OPTARG ;;
    x) PCT=$OPTARG ;; q) QD=$OPTARG ;; s) SEED=$OPTARG ;; e) ENGINE=$OPTARG ;;
    g) GETWAF=$OPTARG ;; T) TRIM=0 ;; y) YES=1 ;; *) usage ;;
  esac
done
[[ -b "$DEV" && $RU -gt 0 ]] || usage
[[ -x "$GETWAF" ]] || { echo "cannot run $GETWAF, build it with make or pass -g"; exit 1; }
grep -q "^$DEV" /proc/mounts && { echo "$DEV is mounted, refusing"; exit 1; }
[[ $YES -eq 1 ]] || { echo "this erases $DEV -- add -y"; exit 1; }

# ------------------------------------------------------------ geometry

SIZE=$(blockdev --getsize64 "$DEV")
SB_BYTES=$(( RU * PS ))
NSB=$(( SIZE / SB_BYTES ))
TOTAL=$(( NSB * SB_BYTES ))
IO_SIZE=$(( SB_BYTES * PCT / 100 / PS * PS ))
(( NSB > 0 )) || { echo "ru-size larger than the device"; exit 1; }

# largest power-of-two block size <= 1M that divides a superblock
# TODO(mfd) : SHould I align up the superblock size given to a be
# a multiple of 1M ?
FILL_BS=1048576
while (( SB_BYTES % FILL_BS != 0 )); do FILL_BS=$(( FILL_BS / 2 )); done

printf 'device      %s (%s GB)\n' "$DEV" "$(( SIZE / 1000000000 ))"
printf 'ru-size     %d pages = %d MiB\n' "$RU" "$(( SB_BYTES / 1048576 ))"
printf 'superblocks %d, unused tail %d MiB\n' "$NSB" "$(( (SIZE-TOTAL)/1048576 ))"
printf 'per round   %d MiB per superblock, %d%%\n' "$(( IO_SIZE/1048576 ))" "$PCT"
printf 'FILL_BS %d\n' "$FILL_BS"

# ------------------------------------------------------------ counters

# get_waf prints three lines: host bytes, media bytes, WAF.
# Take the first two into C_HOST and C_MEDIA, ignore the third.
read_counters() {
  { read -r C_HOST; read -r C_MEDIA; read -r _; } < <("$GETWAF" -d "$DEV")
  [[ -n ${C_HOST:-} && -n ${C_MEDIA:-} ]] || { echo "$GETWAF gave no output"; exit 1; }
}

# WAF between two snapshots, to three decimals, in integer arithmetic.
# m*1000/h is the WAF scaled by 1000, so 1268 means 1.268.
report() {  # report <label> <host0> <media0> <host1> <media1>
  local h=$(( $4 - $2 )) m=$(( $5 - $3 )) w
  if (( h > 0 && m > 0 )); then
    w=$(( m * 1000 / h ))
    printf '%s: host %d MiB, media %d MiB, WAF %d.%03d\n' \
      "$1" "$(( h / 1048576 ))" "$(( m / 1048576 ))" "$(( w / 1000 ))" "$(( w % 1000 ))"
  else
    printf '%s: WAF n/a (counters did not move)\n' "$1"
  fi
}

# ------------------------------------------------------------ workload

echo "counters before:"
"$GETWAF" -d "$DEV" -H

if (( TRIM )); then
  echo "sanitize"
  sudo nvme sanitize --sanact=2 "$DEV"
  sleep 1m
  # sudo nvme sanitize-log "${controller}"
  # sleep 1m
  echo "trimming..."
  blkdiscard -v "$DEV"
fi



echo "filling..."
# fio --name=fill --filename="$DEV" --ioengine="$ENGINE" --direct=1 --thread \
#     --rw=write --bs="$FILL_BS" --iodepth="$QD" --offset=0 --size="$TOTAL" \
#     --fsync=$(( SB_BYTES / FILL_BS )) --minimal > /dev/null
for (( sb = 0; sb < NSB; sb++ )); do
  fio --name=fill --filename="$DEV" --ioengine="$ENGINE" --direct=1 --thread \
      --rw=write --bs="$FILL_BS" --iodepth="$QD" \
      --offset=$(( sb * SB_BYTES )) --size="$SB_BYTES" \
      --end_fsync=1 --minimal > /dev/null
  (( (sb + 1) % 100 == 0 )) && echo "  $(( sb + 1 ))/$NSB"
done

read_counters
H0=$C_HOST; M0=$C_MEDIA
PH=$H0;     PM=$M0

for (( r = 1; r <= ROUNDS; r++ )); do
  echo "round $r/$ROUNDS"
  i=0
  # deterministic shuffle of the superblock order, different each round
  for sb in $(seq 0 $(( NSB - 1 )) | shuf --random-source=<(yes "$(( SEED + r ))")); do
    fio --name=sb --filename="$DEV" --ioengine="$ENGINE" --direct=1 --thread \
        --rw=randwrite --bs="$PS" --iodepth="$QD" \
        --offset=$(( sb * SB_BYTES )) --size="$SB_BYTES" --io_size="$IO_SIZE" \
        --randseed=$(( SEED * 1000000 + r * 10000 + sb )) \
        --end_fsync=1 --minimal > /dev/null
    i=$(( i + 1 ))
    (( i % 100 == 0 )) && echo "  $i/$NSB"
  done
  read_counters
  report "  round $r" "$PH" "$PM" "$C_HOST" "$C_MEDIA"
  PH=$C_HOST; PM=$C_MEDIA
done

echo
report "ru-size $RU pages, overall" "$H0" "$M0" "$PH" "$PM"
