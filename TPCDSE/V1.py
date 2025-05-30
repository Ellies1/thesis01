import subprocess
import threading
import time
import os
from datetime import datetime
from vm_power_mapper import power_collector_with_vm, plot_vm_power_per_config

# === 用户可修改配置 ===
scale_factor = 1
query_file = "q3-v2.4"  # 容器内的查询脚本路径

# === 路径与设置 ===
RESULT_DIR = "result"
FIG_DIR = os.path.join(RESULT_DIR, "finalpic")
os.makedirs(RESULT_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)

ENERGY_FILE = "/var/lib/libvirt/scaphandre/cloud0_zsong/intel-rapl:0/energy_uj"
SCAPHANDRE_PATH = "/usr/local/bin/scaphandre"

vm_ip = "192.168.166.3"
vm_user = "cloud0_zsong"
vm_ssh_key = "/home/zsong/.ssh/id_rsa_continuum"
vm_data_dir = "/tpcds-data"
dataset_dir = f"{vm_data_dir}/dataset_tpcds_{scale_factor}g"

# === Spark 提交命令基础 ===
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
    "--conf", "spark.executor.memory=6g",
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


def run_spark_phase(phase_name, spark_cmd, config_label):
    print(f"\n===== Running phase: {phase_name} =====")
    start_energy = read_energy_uj()
    if start_energy is None:
        print("[⚠️] Cannot read start energy, skipping...")
        return 0.0

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
        print(f"✅ {desc} 成功\n")
    except subprocess.CalledProcessError as e:
        print(f"❌ {desc} 失败: {e}\n")
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


def main():
    config_name = f"tpcds_{query_file.replace('.sql','')}"

    ensure_scaphandre_qemu_running()
    ensure_scaphandre_prometheus_running()
    cleanup_k8s_pods()

    check_dataset_cmd = f'ssh {vm_user}@{vm_ip} -i {vm_ssh_key} "test -d {dataset_dir} && echo FOUND || echo NOT_FOUND"'
    check_output = subprocess.getoutput(check_dataset_cmd)
    if "FOUND" in check_output:
        print("⚠️ 检测到旧数据，即将清理...\n")
        cleanup_cmd = [
            "ssh", f"{vm_user}@{vm_ip}", "-i", vm_ssh_key,
            f"sudo rm -rf {vm_data_dir}"
        ]
        run(cleanup_cmd, f"删除旧目录 {vm_data_dir}")

    mkdir_cmd = [
        "ssh", f"{vm_user}@{vm_ip}", "-i", vm_ssh_key,
        f"sudo mkdir -p {vm_data_dir} && sudo chmod -R 777 {vm_data_dir}"
    ]
    run(mkdir_cmd, f"创建 VM 上的目录 {vm_data_dir}")

    datagen_cmd = spark_submit_base + ["datagen", vm_data_dir, "/opt/tpcds-kit/tools", str(scale_factor)]
    metagen_cmd = spark_submit_base + ["metagen", vm_data_dir, str(scale_factor)]
    query_cmd = spark_submit_base + ["query", query_file, str(scale_factor)]

    total_energy = 0.0
    total_energy += run_spark_phase("datagen", datagen_cmd, config_name)
    total_energy += run_spark_phase("metagen", metagen_cmd, config_name)
    total_energy += run_spark_phase("query", query_cmd, config_name)

    # 写入总能耗
    energy_out = os.path.join(RESULT_DIR, f"energy_{config_name}.txt")
    with open(energy_out, "w") as f:
        f.write(f"{total_energy:.6f}\n")
    print(f"✅ Total energy for {config_name}: {total_energy:.6f} J")

    # 绘图
    print("\nDrawing power-over-time charts...")
    plot_vm_power_per_config()

    # print("\nDrawing energy bar chart...")
    # energy_data = {}
    # for fname in os.listdir(RESULT_DIR):
    #     if fname.startswith("energy_") and fname.endswith(".txt"):
    #         key = fname.replace("energy_", "").replace(".txt", "")
    #         try:
    #             with open(os.path.join(RESULT_DIR, fname)) as f:
    #                 energy_data[key] = float(f.read().strip())
    #         except:
    #             continue

    # if energy_data:
    #     fig_width = min(max(8, len(energy_data) * 1.2), 24)
    #     plt.figure(figsize=(fig_width, 5))
    #     plt.bar(energy_data.keys(), energy_data.values(), width=0.6)
    #     plt.ylabel("Energy (J)")
    #     plt.title("Spark Configurations vs. Energy Consumption")
    #     plt.grid(axis="y")
    #     plt.xticks(rotation=30, ha='right')
    #     plt.tight_layout()
    #     plt.savefig(os.path.join(FIG_DIR, "energy_comparison.png"))
    #     print("✅ Energy bar chart saved: result/finalpic/energy_comparison.png")
    # else:
    #     print("⚠️ No energy data found, skipping energy comparison chart")

if __name__ == "__main__":
    main()
