import sys
import numpy as np

dispatched = {}
latencies  = []

with open(sys.argv[1]) as f:
    for line in f:
        parts = line.split()
        if len(parts) != 3:
            continue
        try:
            ts = float(parts[0])
            action = parts[1]
            sector = parts[2]
        except ValueError:
            continue

        if action == "D":
            dispatched[sector] = ts
        elif action == "C" and sector in dispatched:
            d2c_us = (ts - dispatched.pop(sector)) * 1e6
            latencies.append((sector, d2c_us))

if not latencies:
    print("No Dispatch Complete (D->C) pairs found.")
    sys.exit(1)


# debug: print the 10 slowest IOs with their sector
print(f"\n--- 10 slowest IOs ---")
slowest = sorted(latencies, key=lambda x: x[1], reverse=True)[:10]
for sector, us in slowest:
    print(f"  sector {sector:>12}  {us:.2f} µs")

times = np.array([l[1] for l in latencies])
print(f"Samples : {len(times):,}")
print(f"Min     : {np.min(times):.2f} µs")
print(f"Avg     : {np.mean(times):.2f} µs")
print(f"Max     : {np.max(times):.2f} µs")
print(f"p10     : {np.percentile(times, 10):.2f} µs")
print(f"p20     : {np.percentile(times, 20):.2f} µs")
print(f"p50     : {np.percentile(times, 50):.2f} µs")
print(f"p90     : {np.percentile(times, 90):.2f} µs")
print(f"p95     : {np.percentile(times, 95):.2f} µs")
print(f"p99     : {np.percentile(times, 99):.2f} µs")
print(f"p99.9   : {np.percentile(times, 99.9):.2f} µs")
print(f"p99.99   : {np.percentile(times, 99.99):.2f} µs")

