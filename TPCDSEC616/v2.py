import subprocess
import threading
import time
import os
import matplotlib.pyplot as plt
from datetime import datetime
from vm_power_mapper import power_collector_with_vm, plot_vm_power_per_config

experiment_configs = [
    # Type 1: Strong Scaling
    {"name": "T1-1", "scale": 10, "query": "q3-v2.4", "instances": 1, "cores": 1, "mem": "4g", "repeat": 10},
    {"name": "T1-2", "scale": 10, "query": "q3-v2.4", "instances": 1, "cores": 1, "mem": "4g"},
]

# === location and config ===
RESULT_DIR = "result_4"
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
    "--conf", "spark.kubernetes.container.image=elliesgood00/tpcds-image:v17",
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
    "--conf", "spark.eventLog.enabled=true",
    "--conf", "spark.eventLog.dir=file:///tpcds-data/eventlog",
    "--conf", "spark.eventLog.compress=true",  # 可选项
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



def run_preparation_once(scale_factor=10):
    print("\n🛠️ Running one-time data + table preparation...")
    jar_path = spark_submit_base[-1]
    submit_prefix = spark_submit_base[:-1]
    dynamic_confs = ["--conf", "spark.executor.instances=2", "--conf", "spark.executor.cores=2", "--conf", "spark.executor.memory=6g"]

    mkdir_cmd = [
        "ssh", f"{vm_user}@{vm_ip}", "-i", vm_ssh_key,
        f"sudo mkdir -p {vm_data_dir} && sudo chmod -R 777 {vm_data_dir}"
    ]
    run(mkdir_cmd, f"Create directory on VM: {vm_data_dir}")

    datagen_args = ["datagen", vm_data_dir, "/opt/tpcds-kit/tools", str(scale_factor)]
    datagen_cmd = submit_prefix + dynamic_confs + [jar_path] + datagen_args
    metagen_args = ["metagen", vm_data_dir, str(scale_factor)]
    metagen_cmd = submit_prefix + dynamic_confs + [jar_path] + metagen_args

    run_spark_phase("datagen", datagen_cmd, "initial", { "initial": {} })
    run_spark_phase("metagen", metagen_cmd, "initial", { "initial": {} })

def main():
    ensure_scaphandre_qemu_running()
    ensure_scaphandre_prometheus_running()
    cleanup_k8s_pods()
    # run_preparation_once(scale_factor=10)
    energy_data = {}
    mkdir_cmd = [
            "ssh", f"{vm_user}@{vm_ip}", "-i", vm_ssh_key,
            f"sudo mkdir -p {vm_data_dir}/eventlog && sudo chmod -R 777 {vm_data_dir}"
        ]
    run(mkdir_cmd, f"Create a directory on the VM {vm_data_dir}")
    for exp in experiment_configs:
        wait_until_idle()
        config_name = exp["name"]
        scale_factor = exp["scale"]
        query_file = exp["query"]
        instances = exp["instances"]
        cores = exp["cores"]
        mem = exp["mem"]
        repeat = exp.get("repeat", 1)  # 默认1次
        config_label = f"{config_name}"

        jar_path = spark_submit_base[-1]
        submit_prefix = spark_submit_base[:-1]
        dynamic_confs = [
            "--conf", f"spark.executor.instances={instances}",
            "--conf", f"spark.executor.cores={cores}",
            "--conf", f"spark.executor.memory={mem}"
        ]
        query_args = ["query", query_file, str(scale_factor), str(repeat)]
        query_cmd = submit_prefix + dynamic_confs + [jar_path] + query_args

        phase_boundaries = {config_label: {}}

        e_datagen = 0.0
        e_metagen = 0.0
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

        print("\nDrawing energy bar chart...")
        if energy_data:
            fig_width = min(max(8, len(energy_data) * 1.2), 24)
            plt.figure(figsize=(fig_width, 5))

            labels = list(energy_data.keys())
            e_qr = [energy_data[k][2] for k in labels]

            plt.bar(labels, e_qr, label="Query", color="green")

            for i, label in enumerate(labels):
                plt.text(i, e_qr[i] / 2, f"{e_qr[i]:.2f}", ha='center', va='center', fontsize=8, color='white')

            plt.ylabel("Energy (J)")
            plt.title("Spark Configurations vs. Query Energy Consumption")
            plt.xticks(rotation=30, ha='right')
            plt.grid(axis="y")
            plt.tight_layout()
            plt.savefig(os.path.join(FIG_DIR, "energy_comparison.png"))
            print("✅ Energy bar chart saved: result/finalpic/energy_comparison.png")
        else:
            print("⚠️ No energy data found, skipping energy comparison chart")


if __name__ == "__main__":
    main()
