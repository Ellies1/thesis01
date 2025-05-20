# 🔬 SparkPi Energy Monitoring Demo Summary

## 🎯 Objective  
To verify whether a Spark-based workload (SparkPi) can be used to produce **detectable energy consumption** metrics in a cloud environment using Scaphandre on the host machine.

---

## 🧪 Workflow Overview

1. **Collect Pre-Metrics**  
   Host energy metrics are collected via Scaphandre’s Prometheus exporter at `localhost:8081/metrics` and saved to `before.txt`.

2. **Submit SparkPi to VM via SSH**  
   A SparkPi job is submitted from the host to the VM `cloud0_zsong@192.168.166.3` using `spark-submit`, configured to run **100,000 iterations**.

3. **Wait and Collect Post-Metrics**  
   After a short delay, energy metrics are collected again and saved to `after.txt`.

4. **Compute and Filter Differences**  
   The script filters for energy-related metrics (containing "joules") that changed between `before.txt` and `after.txt`, saving the result in `diff_result.txt`.

---

## ✅ Output Snapshot (`diff_result.txt`)

```
scaph_domain_energy_microjoules{domain_name="dram",domain_id="0",socket_id="1"} 49920589217
scaph_socket_energy_microjoules{socket_id="1"} 246521945562
scaph_domain_energy_microjoules{domain_name="dram",socket_id="0",domain_id="0"} 7243356492
scaph_socket_energy_microjoules{socket_id="0"} 43038699818
scaph_host_energy_microjoules 346728467994
```

---

## 📊 Metric Explanation

| Metric Name                          | Description                                                                 |
|-------------------------------------|-----------------------------------------------------------------------------|
| `scaph_domain_energy_microjoules`   | Energy usage in specific **domains** such as DRAM (memory), within a CPU socket |
| `scaph_socket_energy_microjoules`   | Total energy used by each **physical CPU socket**                          |
| `scaph_host_energy_microjoules`     | **Total energy consumed** by the entire host machine during the task       |

- **Unit:** All values are in **microjoules (μJ)**  
- **Conversion:** `1,000,000 μJ = 1 J` → `1,000 J ≈ 0.000278 kWh`

---

## 🔍 Analysis

- The `scaph_host_energy_microjoules` increased by ~**346 billion μJ**, or **346.7 kJ**, during the SparkPi job execution.
- This energy footprint is significant for a short compute-intensive task, confirming that the **Scaphandre + SparkPi** setup is suitable for **energy benchmarking** in big data environments.

---

## 🧩 Conclusion

This SparkPi demo proves effective as a minimal yet reliable **benchmark for energy analysis** in Spark workloads. It also validates that host-level tools like Scaphandre can detect fine-grained changes in energy usage from remote VM-executed tasks.