
# import subprocess
# import time
# import threading
# import os
# import matplotlib.pyplot as plt
# import pandas as pd

# RESULT_DIR = "result_3rd"
# FIG_DIR = os.path.join(RESULT_DIR, "finalpic")
# os.makedirs(RESULT_DIR, exist_ok=True)
# os.makedirs(FIG_DIR, exist_ok=True)

# def get_vm_cpu_utilization():
#     result = {}
#     try:
#         vm_list = subprocess.check_output("virsh list --name", shell=True).decode().splitlines()
#         for vm in vm_list:
#             if not vm.strip():
#                 continue
#             stats = subprocess.check_output(f"virsh domstats {vm} --vcpu", shell=True).decode()
#             cpu_time = 0
#             for line in stats.splitlines():
#                 if "vcpu." in line and ".time=" in line:
#                     try:
#                         val_str = line.split('=')[1].strip()
#                         val = int(float(val_str)) 
#                         cpu_time += val
#                     except Exception as e:
#                         print(f"[ERROR] Failed to parse line '{line}': {e}")
#             result[vm] = cpu_time
#     except Exception as e:
#         print(f"[⚠️] Failed to get VM CPU stats: {e}")
#     return result

# def plot_vm_power_per_config(phase_boundaries, current_config):
#     phase_colors = {
#     "datagen": "blue",
#     "metagen": "orange",
#     "query": "green"
#     }
#     vm_file = os.path.join(RESULT_DIR, "vm_power_dynamic.txt")
#     if not os.path.exists(vm_file):
#         print("⚠️ No VM power data found.")
#         return

#     df = pd.read_csv(vm_file, names=["timestamp", "vm", "power", "config"])
#     df = df[df["config"] == current_config]
#     grouped = df.groupby(["config", "vm"])

#     # Plot by phase segment
#     for (conf, vm), group in grouped:
#         group = group.copy()
#         t0 = group["timestamp"].min()
#         group["timestamp"] = group["timestamp"] - t0

#         plt.figure(figsize=(8, 4))

#         if conf in phase_boundaries:
#             # Divide into three parts by time
#             start_times = phase_boundaries[conf]
#             ts0 = start_times.get("datagen", t0) - t0
#             ts1 = start_times.get("metagen", t0) - t0
#             ts2 = start_times.get("query", t0) - t0
#             if ts0 == ts1 or ts1 == ts2:
#                 print(f"[⚠️] Phase boundary for {conf} might be invalid: ts0={ts0}, ts1={ts1}, ts2={ts2}")
#             df0 = group[group["timestamp"] < ts1]
#             df1 = group[(group["timestamp"] >= ts1) & (group["timestamp"] < ts2)]
#             df2 = group[group["timestamp"] >= ts2]

#             plt.plot(df0["timestamp"], df0["power"], color=phase_colors["datagen"], label="Data Generation")
#             plt.plot(df1["timestamp"], df1["power"], color=phase_colors["metagen"], label="Table Creation")
#             plt.plot(df2["timestamp"], df2["power"], color=phase_colors["query"], label="Query Execution")
            
#             duration_text = [
#                 f"Data Generation ({ts1 - ts0:.1f}s)",
#                 f"Table Creation ({ts2 - ts1:.1f}s)",
#                 f"Query Execution ({group['timestamp'].max() - ts2:.1f}s)"
#             ]
#             for i, line in enumerate(duration_text):
#                 plt.text(0.01, 0.02 + i * 0.06, line,
#                         fontsize=6,
#                         color='red',
#                         transform=plt.gca().transAxes,
#                         bbox=dict(facecolor='none', edgecolor='none', boxstyle='round,pad=0.3', alpha=0.8))
#         else:
#             plt.plot(group["timestamp"], group["power"])
#         plt.xlim(0, group["timestamp"].max() + 10)
#         plt.xlabel("Time (s)")
#         plt.ylabel("Estimated VM Power (W)")
#         plt.title(f"Power Over Time - {vm} during {conf}")
#         plt.grid(True)
#         plt.legend(loc="lower center", bbox_to_anchor=(0.5, -0.3), ncol=3, fontsize=9)
#         plt.tight_layout()
#         plt.savefig(os.path.join(FIG_DIR, f"vm_power_over_time_{conf}_{vm}.png"))
#         print(f"✅ Saved: vm_power_over_time_{conf}_{vm}.png")



# def power_collector_with_vm(stop_signal, config_name):
#     power_data = []
#     prev_vm_cpu = get_vm_cpu_utilization()
#     prev_ts = time.time()

#     while not stop_signal.is_set():
#         try:
#             metrics = subprocess.check_output("curl -s http://localhost:8081/metrics", shell=True).decode()
#             for line in metrics.splitlines():
#                 if "scaph_host_power_microwatts" in line:
#                     try:
#                         now = time.time()
#                         power = int(line.strip().split()[-1])
#                         print(f"[DEBUG] Host power: {power} µW at {now}")

#                         curr_vm_cpu = get_vm_cpu_utilization()
#                         delta_t = now - prev_ts
#                         vm_power_share = {}
#                         total_delta_cpu = 0

#                         for vm in curr_vm_cpu:
#                             if vm in prev_vm_cpu:
#                                 delta = curr_vm_cpu[vm] - prev_vm_cpu[vm]
#                                 print(f"[DEBUG] VM {vm} delta CPU time: {delta}")
#                                 total_delta_cpu += delta

#                         if total_delta_cpu == 0:
#                             print("[DEBUG] Skipping write: total delta CPU == 0")
#                             prev_vm_cpu = curr_vm_cpu
#                             prev_ts = now
#                             break

#                         for vm in curr_vm_cpu:
#                             if vm in prev_vm_cpu:
#                                 delta = curr_vm_cpu[vm] - prev_vm_cpu[vm]
#                                 est_power = power * delta / total_delta_cpu
#                                 vm_power_share[vm] = est_power / 1e6  # µW to W

#                         with open(os.path.join(RESULT_DIR, "vm_power_dynamic.txt"), "a") as f:
#                             for vm, pwr in vm_power_share.items():
#                                 f.write(f"{now},{vm},{pwr:.6f},{config_name}\n")
#                                 print(f"[DEBUG] Written VM {vm}: {pwr:.6f} W")

#                         prev_vm_cpu = curr_vm_cpu
#                         prev_ts = now
#                         break
#                     except ValueError as e:
#                         print(f"[DEBUG] ValueError parsing power: {e}")
#                         continue
#         except subprocess.CalledProcessError as e:
#             print(f"[DEBUG] curl metrics error: {e}")
#         time.sleep(2)
import subprocess
import time
import threading
import os
import matplotlib.pyplot as plt
import pandas as pd

RESULT_DIR = "result_3"
FIG_DIR = os.path.join(RESULT_DIR, "finalpic")
os.makedirs(RESULT_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)

def get_vm_cpu_utilization():
    result = {}
    try:
        vm_list = subprocess.check_output("virsh list --name", shell=True).decode().splitlines()
        for vm in vm_list:
            if not vm.strip():
                continue
            stats = subprocess.check_output(f"virsh domstats {vm} --vcpu", shell=True).decode()
            cpu_time = 0
            for line in stats.splitlines():
                if "vcpu." in line and ".time=" in line:
                    try:
                        val_str = line.split('=')[1].strip()
                        val = int(float(val_str)) 
                        cpu_time += val
                    except Exception as e:
                        print(f"[ERROR] Failed to parse line '{line}': {e}")
            result[vm] = cpu_time
    except Exception as e:
        print(f"[⚠️] Failed to get VM CPU stats: {e}")
    return result

def plot_vm_power_per_config(phase_boundaries, current_config):
    phase_colors = {
        "query": "green"
    }
    vm_file = os.path.join(RESULT_DIR, "vm_power_dynamic.txt")
    if not os.path.exists(vm_file):
        print("⚠️ No VM power data found.")
        return

    df = pd.read_csv(vm_file, names=["timestamp", "vm", "power", "config"])
    df = df[df["config"] == current_config]
    grouped = df.groupby(["config", "vm"])

    # Plot by query phase only
    for (conf, vm), group in grouped:
        group = group.copy()
        t0 = group["timestamp"].min()
        group["timestamp"] = group["timestamp"] - t0

        plt.figure(figsize=(8, 4))
        plt.plot(group["timestamp"], group["power"], color=phase_colors["query"], label="Query Execution")
        duration_text = [
            f"Query Execution ({group['timestamp'].max():.1f}s)"
        ]
        for i, line in enumerate(duration_text):
            plt.text(
                0.01, 0.02 + i * 0.06, line,
                fontsize=6,
                color='red',
                transform=plt.gca().transAxes,
                bbox=dict(facecolor='none', edgecolor='none', boxstyle='round,pad=0.3', alpha=0.8)
            )
        plt.xlim(0, group["timestamp"].max() + 10)
        plt.xlabel("Time (s)")
        plt.ylabel("Estimated VM Power (W)")
        plt.title(f"Power Over Time - {vm} during {conf}")
        plt.grid(True)
        plt.legend(loc="lower center", bbox_to_anchor=(0.5, -0.3), ncol=1, fontsize=9)
        plt.tight_layout()
        plt.savefig(os.path.join(FIG_DIR, f"vm_power_over_time_{conf}_{vm}.png"))
        print(f"✅ Saved: vm_power_over_time_{conf}_{vm}.png")

def power_collector_with_vm(stop_signal, config_name):
    power_data = []
    prev_vm_cpu = get_vm_cpu_utilization()
    prev_ts = time.time()

    while not stop_signal.is_set():
        try:
            metrics = subprocess.check_output("curl -s http://localhost:8081/metrics", shell=True).decode()
            for line in metrics.splitlines():
                if "scaph_host_power_microwatts" in line:
                    try:
                        now = time.time()
                        power = int(line.strip().split()[-1])
                        print(f"[DEBUG] Host power: {power} µW at {now}")

                        curr_vm_cpu = get_vm_cpu_utilization()
                        delta_t = now - prev_ts
                        vm_power_share = {}
                        total_delta_cpu = 0

                        for vm in curr_vm_cpu:
                            if vm in prev_vm_cpu:
                                delta = curr_vm_cpu[vm] - prev_vm_cpu[vm]
                                print(f"[DEBUG] VM {vm} delta CPU time: {delta}")
                                total_delta_cpu += delta

                        if total_delta_cpu == 0:
                            print("[DEBUG] Skipping write: total delta CPU == 0")
                            prev_vm_cpu = curr_vm_cpu
                            prev_ts = now
                            break

                        for vm in curr_vm_cpu:
                            if vm in prev_vm_cpu:
                                delta = curr_vm_cpu[vm] - prev_vm_cpu[vm]
                                est_power = power * delta / total_delta_cpu
                                vm_power_share[vm] = est_power / 1e6  # µW to W

                        with open(os.path.join(RESULT_DIR, "vm_power_dynamic.txt"), "a") as f:
                            for vm, pwr in vm_power_share.items():
                                f.write(f"{now},{vm},{pwr:.6f},{config_name}\n")
                                print(f"[DEBUG] Written VM {vm}: {pwr:.6f} W")

                        prev_vm_cpu = curr_vm_cpu
                        prev_ts = now
                        break
                    except ValueError as e:
                        print(f"[DEBUG] ValueError parsing power: {e}")
                        continue
        except subprocess.CalledProcessError as e:
            print(f"[DEBUG] curl metrics error: {e}")
        time.sleep(2)
