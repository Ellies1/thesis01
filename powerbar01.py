import os
import json
import numpy as np
import matplotlib.pyplot as plt

# === 配置 ===
log_base_dir = "/home/zsong/continuum/eventloglocal/logs01/eventlog"
base_power_dir = "/home/zsong/continuum/TPCDSEC616"
target_vm = "cloud0_zsong"
# target_exp = "T2-3"

# === 阶段颜色映射 ===
color_map = {
    "Scan": "plum",
    "Join": "salmon",
    "Write": "lightgreen",
    "Aggregate": "violet",
    "Init(Map & Parallelize)": "black",
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

    # === 修正部分：按最短长度统一对齐 ===
    min_len = min(len(p) for p in all_pw)
    all_ts = [ts[:min_len] for ts in all_ts]
    all_pw = [pw[:min_len] for pw in all_pw]

    timestamps = all_ts[0]
    avg_pw = [np.mean([all_pw[j][i] for j in range(3)]) for i in range(min_len)]
    return timestamps, avg_pw

def plot_power_with_phases(timestamps, powers, phase_ranges, out_path):
    import matplotlib.patches as mpatches

    start_time = timestamps[0]
    rel_timestamps = [ts - start_time for ts in timestamps]

    # === 阶段对齐 ===
    adjusted_phases = []
    phase_names_seen = []

    first_stage_start = min(start for start, end, phase in phase_ranges)
    if first_stage_start > start_time:
        adjusted_phases.append((0, first_stage_start - start_time, "Startup"))
        color_map["Startup"] = "grey"
        phase_names_seen.insert(0, "Startup")  # ✅ 保证 Startup 是 P1


    for (start, end, phase) in phase_ranges:
        if end < start_time:
            continue
        rel_start = max(0, start - start_time)
        rel_end = end - start_time
        adjusted_phases.append((rel_start, rel_end, phase))
        if phase not in phase_names_seen:
            phase_names_seen.append(phase)

    # === 编号映射 e.g. "P1: Init"
    phase_to_id = {name: f"P{i+1}" for i, name in enumerate(phase_names_seen)}

    # === 绘图 ===
    plt.figure(figsize=(12, 4.8))
    plt.plot(rel_timestamps, powers, label="Power (W)", color="green", linewidth=2.2)
    plt.xlim(min(rel_timestamps), max(rel_timestamps))
    plt.ylim(min(powers) * 0.95, max(powers) * 1.05)
    plt.xlim(left=0)

    # === 绘制底部阶段横条 ===
    y_min, y_max = plt.ylim()
    bar_height = y_min + 0.05 * (y_max - y_min)
    bar_bottom = y_min - 0.05 * (y_max - y_min)

    for (rel_start, rel_end, phase) in adjusted_phases:
        color = color_map.get(phase, "lightgray")
        plt.fill_betweenx([bar_bottom, bar_height], rel_start, rel_end, color=color, alpha=0.7)
        label = phase_to_id.get(phase, "")  # e.g., "P2"
  

    # === 添加阶段编号图例 ===
    custom_legend = [mpatches.Patch(color=color_map.get(p, "gray"), label=f"{phase_to_id[p]}: {p}")
                     for p in phase_names_seen]
    plt.legend(handles=custom_legend, loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=4, fontsize=9)

    # === 坐标轴 ===
    plt.xlabel("Time Since Query Start (s)")
    plt.ylabel("Power (W)")

    # === Duration 注释
    duration = timestamps[-1] - timestamps[0]
    plt.annotate(f"Avg Duration ≈ {duration:.1f}s", xy=(0.01, 0.99), xycoords='axes fraction',
             ha='left', va='top', fontsize=10, color='black')


    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
    print(f"[✅] 功耗阶段图已保存：{out_path}")

# === 主流程 ===
if __name__ == "__main__":
    # log_path = find_eventlog_for_experiment(log_base_dir, target_exp)
    # phase_ranges = extract_stage_phases(log_path)
    # timestamps, power_values = read_avg_power_series(base_power_dir, target_vm, target_exp)
    # out_path = f"power_phased_{target_exp}.png"
    # plot_power_with_phases(timestamps, power_values, phase_ranges, out_path=out_path)
    # === 实验配置列表（直接贴上你的那一大段）===
    experiment_configs = [
        {"name": "T1-1", "scale": 10, "query": "q3-v2.4", "instances": 1, "cores": 1, "mem": "4g", "repeat": 1},
        {"name": "T1-2", "scale": 10, "query": "q3-v2.4", "instances": 2, "cores": 2, "mem": "6g", "repeat": 1},
        {"name": "T1-3", "scale": 10, "query": "q3-v2.4", "instances": 1, "cores": 4, "mem": "8g", "repeat": 1},
        {"name": "T1-4", "scale": 10, "query": "q3-v2.4", "instances": 2, "cores": 3, "mem": "6g", "repeat": 1},
        {"name": "T2-1", "scale": 10, "query": "q3-v2.4", "instances": 2, "cores": 2, "mem": "6g", "repeat": 6},
        {"name": "T2-2", "scale": 10, "query": "q3-v2.4", "instances": 2, "cores": 2, "mem": "6g", "repeat": 12},
        {"name": "T2-3", "scale": 10, "query": "q3-v2.4", "instances": 2, "cores": 2, "mem": "6g", "repeat": 18},
        {"name": "T3-1", "scale": 10, "query": "q3-v2.4", "instances": 1, "cores": 2, "mem": "4g", "repeat": 3},
        {"name": "T3-2", "scale": 10, "query": "q3-v2.4", "instances": 2, "cores": 2, "mem": "4g", "repeat": 6},
        {"name": "T3-3", "scale": 10, "query": "q3-v2.4", "instances": 4, "cores": 2, "mem": "4g", "repeat": 12},
        {"name": "T4-1", "scale": 10, "query": "q3-v2.4", "instances": 1, "cores": 6, "mem": "12g", "repeat": 1},
        {"name": "T4-2", "scale": 10, "query": "q3-v2.4", "instances": 2, "cores": 3, "mem": "6g", "repeat": 1},
        {"name": "T4-3", "scale": 10, "query": "q3-v2.4", "instances": 3, "cores": 2, "mem": "4g", "repeat": 1},
        {"name": "T5-q5",  "scale": 10, "query": "q5-v2.4",  "instances": 2, "cores": 2, "mem": "6g", "repeat": 1},
        {"name": "T5-q18", "scale": 10, "query": "q18-v2.4", "instances": 2, "cores": 2, "mem": "6g", "repeat": 1},
        {"name": "T5-q64", "scale": 10, "query": "q64-v2.4", "instances": 2, "cores": 2, "mem": "6g", "repeat": 1}
    ]

    # === 自动批量绘图 ===
    for cfg in experiment_configs:
        target_exp = cfg["name"]
        try:
            log_path = find_eventlog_for_experiment(log_base_dir, target_exp)
            phase_ranges = extract_stage_phases(log_path)
            timestamps, power_values = read_avg_power_series(base_power_dir, target_vm, target_exp)
            out_path = f"power_phased_{target_exp}.png"
            plot_power_with_phases(timestamps, power_values, phase_ranges, out_path=out_path)
        except Exception as e:
            print(f"[❌] Failed to generate plot for {target_exp}: {e}")


