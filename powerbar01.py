import os
import json
import numpy as np
import matplotlib.pyplot as plt

# === 配置 ===
log_base_dir = "/home/zsong/continuum/eventloglocal/logs01/eventlog"
base_power_dir = "/home/zsong/continuum/TPCDSEC616"
target_vm = "cloud0_zsong"
target_exp = "T1-3"

# === 阶段颜色映射 ===
color_map = {
    "Scan": "plum",
    "Join": "salmon",
    "Write": "lightgreen",
    "Aggregate": "violet",
    "Init(Map & Parallelize)": "gray",
    "TakeOrdered": "yellow",
    "Exchange(shuffle/aggregate)": "orange",
    "Other": "lightgray"
}

def classify_stage(name):
    name_lower = name.lower()
    if "store_sales" in name_lower and "exchange" in name_lower:
        return "Join"
    if "createtable" in name_lower or "insert" in name_lower or "save" in name_lower or "output" in name_lower:
        return "Write"
    if "scan" in name_lower:
        return "Scan"
    if any(keyword in name_lower for keyword in [
        "aggregate", "hashaggregate", "sortaggregate",
        "objecthashaggregate", "partialaggregate", "finalaggregate"
    ]):
        return "Aggregate"
    if "parallelize" in name_lower or "map;" in name_lower:
        return "Init(Map & Parallelize)"
    if "takeordered" in name_lower:
        return "TakeOrdered"
    if "union" in name_lower:
        return "Union"
    if "exchange" in name_lower:
        return "Exchange(shuffle/aggregate)"
    return "Other"

def find_eventlog_for_experiment(log_base_dir, target_exp):
    target_key = target_exp.lower()
    for fname in os.listdir(log_base_dir):
        fpath = os.path.join(log_base_dir, fname)
        if not os.path.isfile(fpath):
            continue
        try:
            with open(fpath, 'r') as f:
                for line in f:
                    if '"spark.driver.host"' in line and target_key in line.lower():
                        print(f"[🔍] Matched log file: {fname}")
                        return fpath
        except Exception as e:
            print(f"[⚠️] Error reading {fname}: {e}")
    raise FileNotFoundError(f"[❌] No log file found in {log_base_dir} for experiment {target_exp}")

def extract_semantic_stage_name(stage_info):
    rdd_list = stage_info.get("RDD Info", [])
    semantic_names = set()
    for rdd in rdd_list:
        scope_str = rdd.get("Scope", "")
        try:
            scope = json.loads(scope_str)
            name = scope.get("name", "")
            if name:
                semantic_names.add(name)
        except json.JSONDecodeError:
            continue
    if semantic_names:
        return "; ".join(sorted(semantic_names))
    else:
        return stage_info.get("Stage Name", "unknown")

def extract_stage_phases(log_path):
    stage_start = {}
    stage_end = {}

    with open(log_path, 'r') as f:
        for line in f:
            try:
                event = json.loads(line)
                if event.get("Event") == "SparkListenerStageSubmitted":
                    stage_info = event["Stage Info"]
                    stage_id = stage_info["Stage ID"]
                    submit_time = stage_info.get("Submission Time")
                    semantic_name = extract_semantic_stage_name(stage_info)
                    if submit_time:
                        stage_start[stage_id] = (submit_time, semantic_name)

                elif event.get("Event") == "SparkListenerStageCompleted":
                    stage_info = event["Stage Info"]
                    stage_id = stage_info["Stage ID"]
                    complete_time = stage_info.get("Completion Time")
                    if complete_time:
                        stage_end[stage_id] = complete_time
            except json.JSONDecodeError:
                continue

    phase_ranges = []
    for sid in stage_start:
        if sid in stage_end:
            start, name = stage_start[sid]
            end = stage_end[sid]
            phase = classify_stage(name)
            phase_ranges.append((start / 1000, end / 1000, phase))
    return phase_ranges

def read_avg_power_series(base_dir, target_vm, target_exp):
    all_ts = []
    all_pw = []
    for i in range(1, 4):
        fpath = os.path.join(base_dir, f"result_{i}", "vm_power_dynamic.txt")
        ts_list = []
        pw_list = []
        with open(fpath) as f:
            for line in f:
                ts, vm, power, exp = line.strip().split(",")
                if vm == target_vm and exp == target_exp:
                    ts_list.append(float(ts))
                    pw_list.append(float(power))
        all_ts.append(ts_list)
        all_pw.append(pw_list)

    # 对齐时间戳（假设都一样）
    timestamps = all_ts[0]
    avg_pw = [np.mean([all_pw[j][i] for j in range(3)]) for i in range(len(timestamps))]
    return timestamps, avg_pw

def plot_power_with_phases(timestamps, powers, phase_ranges, out_path="power_phased.png"):
    start_time = timestamps[0]
    rel_timestamps = [ts - start_time for ts in timestamps]

    # === 阶段对齐与额外标记 ===
    adjusted_phases = []

    # # Post-Query（功耗存在但阶段结束）
    # if adjusted_phases:
    #     last_stage_end = max(end for _, end, _ in adjusted_phases)
    # else:
    #     last_stage_end = 0
    # last_power_time = rel_timestamps[-1]
    # if last_power_time > last_stage_end:
    #     adjusted_phases.append((last_stage_end, last_power_time, "Post-Query"))
    #     color_map["Post-Query"] = "None"

    # Startup（功耗早于阶段）
    first_stage_start = min(start for start, end, phase in phase_ranges)
    if first_stage_start > start_time:
        adjusted_phases.append((0, first_stage_start - start_time, "Startup"))
        color_map["Startup"] = "black"

    for (start, end, phase) in phase_ranges:
        if end < start_time:
            continue
        rel_start = max(0, start - start_time)
        rel_end = end - start_time
        adjusted_phases.append((rel_start, rel_end, phase))

    # === 绘图 ===
    plt.figure(figsize=(12, 5))
    plt.plot(rel_timestamps, powers, label="Power (W)", color="green")

    for (rel_start, rel_end, phase) in adjusted_phases:
        color = color_map.get(phase, "lightgray")
        plt.axvspan(rel_start, rel_end, color=color, alpha=0.3, label=phase)

    plt.xlabel("Time Since Query Start (s)")
    plt.ylabel("Power (W)")
    plt.title(f"Power Over Time with Phase Highlighting ({target_exp})")

    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys(), loc="upper right")

    # 平均执行时间注释
    duration = timestamps[-1] - timestamps[0]
    plt.annotate(f"Avg Duration ≈ {duration:.1f}s", xy=(0.99, 0.01), xycoords='axes fraction',
                 ha='right', va='bottom', fontsize=10, color='gray')

    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
    print(f"[✅] 功耗阶段图已保存：{out_path}")

# === 主流程 ===
if __name__ == "__main__":
    log_path = find_eventlog_for_experiment(log_base_dir, target_exp)
    phase_ranges = extract_stage_phases(log_path)
    timestamps, power_values = read_avg_power_series(base_power_dir, target_vm, target_exp)
    plot_power_with_phases(timestamps, power_values, phase_ranges)
