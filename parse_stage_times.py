
import json
import re
import os
import matplotlib.pyplot as plt

# 分类函数
def classify_stage(name):
    name_lower = name.lower()
    if "join" in name_lower:
        return "Join"
    elif "exchange" in name_lower and "scan" in name_lower:
        if "store_sales" in name_lower:
            return "Join"
        else:
            return "Scan"
    elif "scan" in name_lower:
        return "Scan"
    elif any(keyword in name_lower for keyword in [
        "aggregate", "hashaggregate", "sortaggregate",
        "objecthashaggregate", "partialaggregate", "finalaggregate"
    ]):
        return "Aggregate"
    elif any(keyword in name_lower for keyword in [
        "write", "save", "insert", "output", "datasink", "createtable"
    ]):
        return "Write"
    else:
        return "Other"

# 提取语义名
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

# 主解析函数
def parse_and_group_stages(log_path, min_duration_ms=1):
    stage_start = {}
    stage_end = {}
    experiment_name = None

    with open(log_path, 'r') as f:
        for line in f:
            try:
                event = json.loads(line)

                if event.get("Event") == "SparkListenerEnvironmentUpdate":
                    spark_props = event.get("Spark Properties", {})
                    driver_host = spark_props.get("spark.driver.host", "")
                    match = re.search(r"(t\d+(?:[-_][a-z]*\d+)?)", driver_host)
                    if match:
                        experiment_name = match.group(1)

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

    grouped = {}

    for sid in stage_start:
        if sid in stage_end:
            start, name = stage_start[sid]
            end = stage_end[sid]
            duration = end - start
            if duration < min_duration_ms:
                continue
            phase = classify_stage(name)
            if phase not in grouped:
                grouped[phase] = []
            grouped[phase].append((start, end, duration, sid, name))

    return experiment_name, grouped

# 能耗读取
def read_total_energy(exp_name):
    if not exp_name.lower().startswith("t"):
        print(f"[⚠️] 无法识别实验名格式: {exp_name}")
        return None

    # 构造符合 energy_Tx-xxx.txt 的格式
    energy_name = f"T{exp_name[1:]}"
    energy_path = f"/home/zsong/continuum/TPCDSEC616/result_3/energy_{energy_name}.txt"

    if not os.path.exists(energy_path):
        print(f"[⚠️] 能耗文件不存在: {energy_path}")
        return None

    try:
        with open(energy_path) as f:
            return float(f.readline().strip())
    except:
        print(f"[⚠️] 无法读取能耗数值: {energy_path}")
        return None


# 分配能耗
def split_energy_by_duration(total_energy, total_durations):
    total_time = sum(total_durations.values())
    energy_per_phase = {}
    for phase, dur in total_durations.items():
        energy_per_phase[phase] = total_energy * (dur / total_time) if total_time > 0 else 0
    return energy_per_phase

def draw_energy_bar(energy_per_phase, output_path="phase_energy.png"):
    import matplotlib.pyplot as plt

    phases = list(energy_per_phase.keys())
    values = [energy_per_phase[p] for p in phases]

    plt.figure(figsize=(8, 5))
    bars = plt.bar(phases, values, color="green")  # 设置颜色为绿色
    plt.ylabel("Energy (Joules)")
    plt.title("Energy Usage by Phase")
    plt.grid(axis='y')

    # 在柱子上添加具体的能耗数值
    for bar, value in zip(bars, values):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, height + max(values) * 0.01,
                 f"{value:.2f} J", ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    plt.savefig(output_path)
    print(f"[✅] 能耗柱状图已保存：{output_path}")
def process_log(log_path, output_dir):
    exp_name, grouped = parse_and_group_stages(log_path)
    if not exp_name:
        print(f"[⚠️] 无法识别实验名 for file: {log_path}")
        return

    print(f"\n[Experiment: {exp_name} from file: {os.path.basename(log_path)}]")

    print(f"{'Phase':<12} {'Stage ID':<9} {'Start':<15} {'End':<15} {'Duration(ms)':<14} Operation")
    print("-" * 90)
    for phase in grouped:
        for (start, end, duration, sid, detail) in grouped[phase]:
            print(f"{phase:<12} {sid:<9} {start:<15} {end:<15} {duration:<14} {detail}")

    total_durations = {phase: sum(r[2] for r in grouped[phase]) for phase in grouped}
    print("\n[阶段耗时统计]")
    for phase, dur in total_durations.items():
        print(f"{phase:<12}: {dur} ms")

    total_energy = read_total_energy(exp_name)
    if total_energy is not None:
        print(f"\n[总能耗] {total_energy:.2f} J")
        energy_by_phase = split_energy_by_duration(total_energy, total_durations)
        print("\n[阶段能耗分配]")
        for phase, energy in energy_by_phase.items():
            print(f"{phase:<12}: {energy:.2f} J")
        draw_energy_bar(energy_by_phase, output_path=os.path.join(output_dir, f"phase_energy_{exp_name}.png"))


if __name__ == "__main__":
    log_dir = "/home/zsong/continuum/eventloglocal/logs03/eventlog"
    output_dir = os.path.join(log_dir, "picforno1")
    os.makedirs(output_dir, exist_ok=True)

    for fname in os.listdir(log_dir):
        print(f"Found file: {fname}")
        fpath = os.path.join(log_dir, fname)
        if not os.path.isfile(fpath) or fname.startswith("phase_energy"):
            continue
        try:
            process_log(fpath, output_dir)
        except Exception as e:
            print(f"[❌] 处理文件失败: {fpath}, 错误: {e}")
