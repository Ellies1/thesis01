import subprocess
import threading
import time
import os
import time
import re
import matplotlib.pyplot as plt
from datetime import datetime
from vm_power_mapper import power_collector_with_vm, plot_vm_power_per_config

experiment_configs = [
    {"name": "T1-4", "scale": 1, "query": "q3-v2.4", "instances": 2, "cores": 3, "mem": "6g"},

]

# === location and config ===
RESULT_DIR = "result_3rd"
FIG_DIR = os.path.join(RESULT_DIR, "finalpic")
os.makedirs(RESULT_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)

ENERGY_FILE = "/var/lib/libvirt/scaphandre/cloud0_zsong/intel-rapl:0/energy_uj"
SCAPHANDRE_PATH = "/usr/local/bin/scaphandre"

vm_ip = "192.168.166.3"
vm_user = "cloud0_zsong"
vm_ssh_key = "/home/zsong/.ssh/id_rsa_continuum"
vm_data_dir = "/tpcds-data"

# === Spark submit commands ===
spark_submit_base = [
    os.path.expanduser("~/continuum/spark-3.4.4-bin-hadoop3/bin/spark-submit"),
    "--class", "ParquetGenerator",
    "--master", "k8s://https://192.168.166.2:6443",
    "--deploy-mode", "cluster",
    "--conf", "spark.kubernetes.container.image=elliesgood00/tpcds-image:v16",
    "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
    "--conf", "spark.kubernetes.executor.volumes.hostPath.tpcds.mount.path=/tpcds-data",
    "--conf", "spark.kubernetes.executor.volumes.hostPath.tpcds.options.path=/tpcds-data",
    "--conf", "spark.kubernetes.executor.volumes.hostPath.tpcds.options.readOnly=false",
    "--conf", "spark.kubernetes.executor.volumes.hostPath.tpcds.options.type=Directory",
    "--conf", "spark.kubernetes.driver.volumes.hostPath.tpcds.mount.path=/tpcds-data",
    "--conf", "spark.kubernetes.driver.volumes.hostPath.tpcds.options.path=/tpcds-data",
    "--conf", "spark.kubernetes.driver.volumes.hostPath.tpcds.options.readOnly=false",
    "--conf", "spark.kubernetes.driver.volumes.hostPath.tpcds.options.type=Directory",
    "--conf", "spark.sql.catalogImplementation=hive",
    "--conf", "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=/tpcds-data/metastore_db;create=true",
    "--conf", "spark.sql.warehouse.dir=/tpcds-data/hive-warehouse",
    "--conf", "spark.executor.memoryOverhead=2g",
    "--conf", "spark.driver.memory=4g",
    "--conf", "spark.driver.memoryOverhead=1g",
    "--jars", "local:///opt/tpcds/lib/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar",
    "local:///opt/tpcds/parquet-data-generator_2.12-1.0.jar"
]



def read_energy_uj():
    try:
        with open(ENERGY_FILE) as f:
            return int(f.read().strip())
    except Exception as e:
        print(f"[⚠️] Failed to read energy_uj: {e}")
        return None


def collect_query_job_energy():
    energy_map = {}
    job_start_energy = {}

    controller_user = "cloud_controller_zsong"
    controller_ip = "192.168.166.2"
    controller_ssh_key = "/home/zsong/.ssh/id_rsa_continuum"

    # 1. 获取 driver pod 名字
    get_log_cmd = (
    f"ssh -i {controller_ssh_key} -o StrictHostKeyChecking=no "
    f"{controller_user}@{controller_ip} "
    f"\"kubectl get pod -n default -l spark-role=driver --sort-by=.metadata.creationTimestamp "
    f"-o jsonpath='{{.items[-1].metadata.name}}'\""
    )

    try:
        driver_pod_name = subprocess.check_output(get_log_cmd, shell=True).decode().strip()
        print(f"[👀] Detected driver pod: {driver_pod_name}")
    except subprocess.CalledProcessError as e:
        print(f"[❌] Failed to get driver pod name:\n{e}")
        return energy_map

    # 2. 执行 kubectl logs -f 获取日志并实时解析
    stream_log_cmd = (
        f"ssh -i {controller_ssh_key} -o StrictHostKeyChecking=no "
        f"{controller_user}@{controller_ip} "
        f"\"kubectl logs -f {driver_pod_name} -n default\""
    )

    try:
        proc = subprocess.Popen(stream_log_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in iter(proc.stdout.readline, b''):
            line = line.decode(errors="ignore").strip()

            m_start = re.search(r"Got job (\d+)", line)
            if m_start:
                job_id = m_start.group(1)
                energy = read_energy_uj()
                if energy is not None:
                    job_start_energy[job_id] = energy
                    print(f"[🔵] Job {job_id} start energy: {energy}")

            m_end = re.search(r"Job (\d+) finished", line)
            if m_end:
                job_id = m_end.group(1)
                if job_id in job_start_energy:
                    energy_end = read_energy_uj()
                    if energy_end is not None:
                        delta = (energy_end - job_start_energy[job_id]) / 1e6
                        energy_map[f"query_job_{job_id}"] = delta
                        print(f"[🟢] Job {job_id} finished. Energy used: {delta:.3f} J")

                # 可选终止条件
                if len(energy_map) >= 10:
                    break

        proc.terminate()
    except Exception as e:
        print(f"[⚠️] Failed to stream logs: {e}")

    return energy_map


def run_spark_phase(phase_name, spark_cmd, config_label, phase_boundaries):
    print(f"\n===== Running phase: {phase_name} =====")
    start_energy = read_energy_uj()
    if start_energy is None:
        print("[⚠️] Cannot read start energy, skipping...")
        return 0.0

    # start time
    phase_start = time.time()
    stop_signal = threading.Event()
    collector_thread = threading.Thread(target=power_collector_with_vm, args=(stop_signal, config_label))
    collector_thread.start()

    try:
        subprocess.run(spark_cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"[⚠️] Spark job failed: {e}")
        stop_signal.set()
        collector_thread.join()
        return 0.0

    stop_signal.set()
    collector_thread.join()

    phase_end = time.time()
    phase_boundaries[config_label][phase_name] = phase_start 

    end_energy = read_energy_uj()
    if end_energy is None:
        print("[⚠️] Cannot read end energy.")
        return 0.0

    delta = (end_energy - start_energy) / 1e6
    print(f"✅ Phase {phase_name} done. Energy: {delta:.3f} J")

    if phase_name == "query":
        job_energy_map = collect_query_job_energy()
        for label, val in job_energy_map.items():
            phase_boundaries[config_label][label] = val

    return delta



def ensure_scaphandre_qemu_running():
    try:
        output = subprocess.check_output("ps aux | grep 'scaphandre qemu' | grep -v grep", shell=True).decode()
        if output.strip():
            print("[0] Scaphandre qemu exporter already running, skip starting it.")
            return
    except subprocess.CalledProcessError:
        pass
    print("[0] Scaphandre qemu exporter not running, starting it now...")
    subprocess.Popen(["sudo", SCAPHANDRE_PATH, "qemu"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)


def ensure_scaphandre_prometheus_running():
    try:
        output = subprocess.check_output("curl -s http://localhost:8081/metrics", shell=True)
        if b"scaph_host_power_microwatts" in output:
            print("[0] Scaphandre prometheus exporter already running, skip starting it.")
            return
    except subprocess.CalledProcessError:
        pass
    print("[0] Scaphandre prometheus exporter not running, starting it now...")
    subprocess.Popen(["sudo", SCAPHANDRE_PATH, "prometheus", "--address", "0.0.0.0", "--port", "8081"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)


def run(command, desc, exit_on_fail=True, shell=False):
    print(f"🔧 {desc}...")
    try:
        subprocess.run(command, check=True, shell=shell)
        print(f"✅ {desc} succeed\n")
    except subprocess.CalledProcessError as e:
        print(f"❌ {desc} failed: {e}\n")
        if exit_on_fail:
            exit(1)


def cleanup_k8s_pods():
    print("🧹 Cleaning up old Kubernetes pods on controller node...")
    ssh_cmd = (
        "ssh -o StrictHostKeyChecking=no "
        "-i /home/zsong/.ssh/id_rsa_continuum "
        "cloud_controller_zsong@192.168.166.2 "
        "'kubectl delete pod --all --namespace=default'"
    )
    try:
        result = subprocess.run(ssh_cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("✅ Kubernetes pods cleaned up:\n" + result.stdout.decode())
    except subprocess.CalledProcessError as e:
        print(f"[⚠️] Failed to clean up Kubernetes pods:\n{e.stderr.decode()}")


def is_host_idle(threshold_idle=95.0, check_duration=5, interval=1, pass_ratio=0.8):
    idle_counts = 0
    total_checks = check_duration // interval

    for _ in range(total_checks):
        try:
            output = subprocess.check_output("top -b -n 1 | grep '%Cpu(s)'", shell=True).decode()
            parts = output.split(",")
            for part in parts:
                if "id" in part:
                    try:
                        idle_val = float(part.strip().split()[0])
                        if idle_val >= threshold_idle:
                            idle_counts += 1
                    except:
                        continue
        except Exception as e:
            print(f"Error checking CPU idle: {e}")
        time.sleep(interval)

    return idle_counts >= int(total_checks * pass_ratio)

def wait_until_idle():
    print("🕓 Checking if host is idle before next experiment...")
    while not is_host_idle():
        print("Host not idle yet. Waiting 10s before rechecking...")
        time.sleep(10)
    print("✅ Host is idle. Proceeding...")

def main():
    ensure_scaphandre_qemu_running()
    ensure_scaphandre_prometheus_running()
    cleanup_k8s_pods() 
    energy_data = {}

    for exp in experiment_configs:
        wait_until_idle()
        config_name = exp["name"]
        scale_factor = exp["scale"]
        query_file = exp["query"]
        instances = exp["instances"]
        cores = exp["cores"]
        mem = exp["mem"]
        config_label = f"{config_name}"
        dataset_dir = f"{vm_data_dir}/dataset_tpcds_{scale_factor}g"
        check_dataset_cmd = f'ssh {vm_user}@{vm_ip} -i {vm_ssh_key} "test -d {dataset_dir} && echo FOUND || echo NOT_FOUND"'
        check_output = subprocess.getoutput(check_dataset_cmd)
        if "FOUND" in check_output:
            print("⚠️ Old data detected, about to be cleaned up...\n")
            cleanup_cmd = [
                "ssh", f"{vm_user}@{vm_ip}", "-i", vm_ssh_key,
                f"sudo rm -rf {vm_data_dir}"
            ]
            run(cleanup_cmd, f"Delete the old directory {vm_data_dir}")

        mkdir_cmd = [
            "ssh", f"{vm_user}@{vm_ip}", "-i", vm_ssh_key,
            f"sudo mkdir -p {vm_data_dir} && sudo chmod -R 777 {vm_data_dir}"
        ]
        run(mkdir_cmd, f"Create a directory on the VM {vm_data_dir}")
        spark_cmd_base = spark_submit_base.copy()
        spark_cmd_base += [
            "--conf", f"spark.executor.instances={instances}",
            "--conf", f"spark.executor.cores={cores}",
            "--conf", f"spark.executor.memory={mem}"
        ]

        jar_path = spark_submit_base[-1]  # "local:///opt/tpcds/parquet-data-generator_2.12-1.0.jar"
        submit_prefix = spark_submit_base[:-1]  
        dynamic_confs = [
            "--conf", f"spark.executor.instances={instances}",
            "--conf", f"spark.executor.cores={cores}",
            "--conf", f"spark.executor.memory={mem}"
        ]


        datagen_args = ["datagen", vm_data_dir, "/opt/tpcds-kit/tools", str(scale_factor)]
        datagen_cmd = submit_prefix + dynamic_confs + [jar_path] + datagen_args
        metagen_args = ["metagen", vm_data_dir, str(scale_factor)]
        metagen_cmd = submit_prefix + dynamic_confs + [jar_path] + metagen_args
        query_args = ["query", query_file, str(scale_factor)]
        query_cmd = submit_prefix + dynamic_confs + [jar_path] + query_args


        phase_boundaries = {config_label: {}}

        total_energy = 0.0
        e_datagen = run_spark_phase("datagen", datagen_cmd, config_label, phase_boundaries)
        e_metagen = run_spark_phase("metagen", metagen_cmd, config_label, phase_boundaries)
        e_query = run_spark_phase("query", query_cmd, config_label, phase_boundaries)
        total_energy = e_datagen + e_metagen + e_query

        energy_data[config_name] = (e_datagen, e_metagen, e_query)

        energy_out = os.path.join(RESULT_DIR, f"energy_{config_name}.txt")
        with open(energy_out, "w") as f:
            f.write(f"{total_energy:.6f}\n")
            f.write(f"{e_datagen:.6f}\n")
            f.write(f"{e_metagen:.6f}\n")
            f.write(f"{e_query:.6f}\n")
        print(f"✅ Total energy for {config_name}: {total_energy:.6f} J")

 
        print("\nDrawing power-over-time chart for:", config_label)
        plot_vm_power_per_config({config_label: phase_boundaries[config_label]}, config_label)
        # energy_data[config_name] = total_energy

        print("\nDrawing energy bar chart...")
        if energy_data:
            fig_width = min(max(8, len(energy_data) * 1.2), 24)
            plt.figure(figsize=(fig_width, 5))

            labels = list(energy_data.keys())
            e_dg = [energy_data[k][0] for k in labels]
            e_mt = [energy_data[k][1] for k in labels]

            # 准备 query_job_X 堆叠部分
            query_job_layers = []
            max_jobs = 0

            for k in labels:
                job_keys = sorted([j for j in phase_boundaries[k] if j.startswith("query_job_")],
                                key=lambda x: int(x.split("_")[-1]))
                max_jobs = max(max_jobs, len(job_keys))
                query_job_layers.append([phase_boundaries[k][j] for j in job_keys])

            # 填充短的 config，使得每个 config 的 query_job_X 数量一致
            for qj in query_job_layers:
                while len(qj) < max_jobs:
                    qj.append(0.0)

            # 转置：从 per-config 转为 per-job-layer
            stacked_query_jobs = list(zip(*query_job_layers))

            # 画前两段
            plt.bar(labels, e_dg, label="Data Generation", color="blue")
            plt.bar(labels, e_mt, bottom=e_dg, label="Table Creation", color="orange")

            bottom = [dg + mt for dg, mt in zip(e_dg, e_mt)]

            # 按照 job_0, job_1... 叠加画
            for idx, layer in enumerate(stacked_query_jobs):
                plt.bar(labels, layer, bottom=bottom, label=f"Query Job {idx}", alpha=0.6)
                bottom = [b + l for b, l in zip(bottom, layer)]

            # 总能耗标注
            for i, label in enumerate(labels):
                total = bottom[i]
                plt.text(i, total + 30, f"{total:.2f}", ha='center', va='bottom', fontsize=9, color='black')

            plt.ylabel("Energy (J)")
            plt.title("Spark Configurations vs. Energy Consumption (Detailed Query Jobs)")
            plt.xticks(rotation=30, ha='right')
            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            plt.grid(axis="y")
            plt.tight_layout()
            plt.savefig(os.path.join(FIG_DIR, "energy_comparison.png"))
            print("✅ Energy bar chart saved: result/finalpic/energy_comparison.png")
        else:
            print("⚠️ No energy data found, skipping energy comparison chart")

if __name__ == "__main__":
    main()
