
import subprocess
import time
import os
import matplotlib.pyplot as plt
import threading
import numpy as np
import pandas as pd
import shutil
from datetime import datetime
from vm_power_mapper import power_collector_with_vm, plot_vm_power_per_config

# === config settings ===
ENERGY_FILE = "/var/lib/libvirt/scaphandre/cloud0_zsong/intel-rapl:0/energy_uj"
SPARK_HOME = os.path.expanduser("~/continuum/spark-3.4.4-bin-hadoop3")
SPARK_SUBMIT = os.path.join(SPARK_HOME, "bin/spark-submit")
SCAPHANDRE_PATH = "/usr/local/bin/scaphandre"

RESULT_DIR = "result"
FIG_DIR = os.path.join(RESULT_DIR, "finalpic")
os.makedirs(RESULT_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)

# === Kubernetes settings ===
K8S_MASTER_URL = "k8s://https://192.168.166.2:6443"
SPARK_IMAGE = "elliesgood00/1213:k8s-trusted-with-example"
SPARK_JAR_PATH_IN_CONTAINER = "local:///opt/contistuff/spark-examples_2.12-3.4.4.jar"

CONFIGS = {
    # In the SparkPi example, it represents the number of random point pairs used to estimate the value of π. A larger value means a greater computational workload and a longer task time.
    "idle-baseline": {},

    # Strong Scaling: Fixed problem size, increased resources (number of threads)
    "st-1": {"params": 100000, "threads": 1, "memory": "1g", "instances": 1},
    "st-2": {"params": 100000, "threads": 2, "memory": "1g", "instances": 1},
    "st-4": {"params": 100000, "threads": 4, "memory": "1g", "instances": 1},

    # Increasing Problem Size: Fixed resources, increasing workload
    "sc-1":  {"params": 50000,  "threads": 2, "memory": "2g", "instances": 1},
    "sc-2": {"params": 100000, "threads": 2, "memory": "2g", "instances": 1},
    "sc-3":  {"params": 200000, "threads": 2, "memory": "2g", "instances": 1},

    # Weak Scaling: Fixed number of problems, multiple instances in parallel
    "w-1in": {"params": 100000, "threads": 1, "memory": "1g", "instances": 1},
    "w-2in": {"params": 100000, "threads": 1, "memory": "1g", "instances": 2},
    "w-4in": {"params": 100000, "threads": 1, "memory": "1g", "instances": 4},

    # Split Scaling: The total amount of tasks is fixed and each instance is allocated
    "sp-2in": {"params": 50000, "threads": 1, "memory": "1g", "instances": 2},  # 2*50000 = 100000
    "sp-4in": {"params": 25000, "threads": 1, "memory": "1g", "instances": 4},  # 4*25000 = 100000
}
# CONFIGS = {
#     # In the SparkPi example, it represents the number of random point pairs used to estimate the value of π. A larger value means a greater computational workload and a longer task time.
#     "idle-baseline": {},

#     # Strong Scaling: Fixed problem size, increased resources (number of threads)
#     "st-1": {"params": 3000, "threads": 1, "memory": "1g", "instances": 1},
# }
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

def is_host_idle(threshold_idle=95.0, check_duration=5, interval=1):
    idle_counts = 0
    total_checks = check_duration // interval

    for _ in range(total_checks):
        try:
            output = subprocess.check_output("top -b -n 1 | grep '%Cpu(s)'", shell=True).decode()
            parts = output.split(",")
            for part in parts:
                if "id" in part:
                    idle_str = part.strip().split()[0]
                    idle_val = float(idle_str)
                    if idle_val >= threshold_idle:
                        idle_counts += 1
        except Exception as e:
            print(f"Error checking CPU idle: {e}")
        time.sleep(interval)

    return idle_counts == total_checks

def wait_until_idle():
    print("Checking if host is idle before starting idle-baseline...")
    while not is_host_idle():
        print("Host not idle yet. Waiting 10s before rechecking...")
        time.sleep(10)
    print("✅ Host is idle. Proceeding with idle-baseline...")

def install_spark_if_needed():
    if not os.path.exists(SPARK_HOME):
        print("[0] Spark not found, downloading and installing...")
        os.makedirs(os.path.expanduser("~/continuum"), exist_ok=True)
        os.chdir(os.path.expanduser("~/continuum"))
        subprocess.run("wget https://dlcdn.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz", shell=True, check=True)
        subprocess.run("tar xvf spark-3.4.4-bin-hadoop3.tgz", shell=True, check=True)
        subprocess.run("rm spark-3.4.4-bin-hadoop3.tgz", shell=True, check=True)
        print("[0] Spark is installed.")
    else:
        print("[0] Spark is already installed, skip this step.")

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

def read_energy_uj():
    try:
        with open(ENERGY_FILE) as f:
            return int(f.read().strip())
    except Exception as e:
        print(f"[⚠️] Failed to read energy_uj: {e}")
        return None

def run_spark_job_on_k8s(threads, memory, params, instances):
    LOCAL_JAVA_HOME = "/home/zsong/.sdkman/candidates/java/11.0.26-amzn"
    cmd = f"""
export JAVA_HOME={LOCAL_JAVA_HOME} && export PATH=$JAVA_HOME/bin:$PATH && {SPARK_SUBMIT} --class org.apache.spark.examples.SparkPi --master {K8S_MASTER_URL} --deploy-mode cluster --conf spark.kubernetes.driver.uploadK8sResources=false --conf spark.executor.instances={instances} --conf spark.executor.cores={threads} --conf spark.executor.memory={memory} --conf spark.driver.memory=1g --conf spark.kubernetes.container.image.pullPolicy=Always --conf spark.kubernetes.driver.container.image={SPARK_IMAGE} --conf spark.kubernetes.executor.container.image={SPARK_IMAGE} --conf spark.kubernetes.driverEnv.JAVA_HOME=/opt/java/openjdk --conf spark.kubernetes.executorEnv.JAVA_HOME=/opt/java/openjdk --conf spark.app.name=spark-ca-debug --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark {SPARK_JAR_PATH_IN_CONTAINER} {params}
"""
    print("Submit command preview:")
    print(cmd)
    result = subprocess.run(cmd, shell=True, executable="/bin/bash")
    if result.returncode != 0:
        print(f"[⚠️] Spark job failed with return code {result.returncode}")
        return False
    return True

def run_experiment(name, config):
    print(f"\n===== Start the experiment: {name} =====")
    energy_out = os.path.join(RESULT_DIR, f"energy_{name}.txt")
    stop_signal = threading.Event()
    start_energy = read_energy_uj()
    if start_energy is None:
        print("[⚠️] Start energy read failed, skipping...")
        return

    print("[1] Start collecting host power and VM estimates...")
    collector_thread = threading.Thread(target=power_collector_with_vm, args=(stop_signal, name))
    collector_thread.start()

    if name == "idle-baseline":
        print(f"[2] No Spark task submitted for {name} (idle)...")
        time.sleep(10)
        print("   -> Now collecting valid idle-baseline data.")
        time.sleep(30)
    else:
        print(f"[2] Submit SparkPi task with {config['instances']} instance(s)...")
        
        # Adjust workload if weak scaling experiment (i.e., w-* prefix)
        if name.startswith("w-") or name.startswith("sp-"):
            total_params = config["params"] * config["instances"]
        else:
            total_params = config["params"]
        print(f"     Total params: {total_params}, Executor cores: {config['threads']}, Memory: {config['memory']}")
        success = run_spark_job_on_k8s(config["threads"], config["memory"], total_params, config["instances"])
        if not success:
            stop_signal.set()
            collector_thread.join()
            return
        time.sleep(5)

    print("[3] Stop power collection and record final energy")
    stop_signal.set()
    collector_thread.join()

    end_energy = read_energy_uj()
    if end_energy is None:
        print("[⚠️] End energy read failed")
        return

    total_energy_joules = (end_energy - start_energy) / 1e6
    with open(energy_out, "w") as f:
        f.write(f"{total_energy_joules}\n")
    print(f"✅ Experiment {name} finished. Estimated total energy: {total_energy_joules:.6f} Joules")
def main():
    cleanup_k8s_pods() 
    install_spark_if_needed()
    ensure_scaphandre_qemu_running()
    ensure_scaphandre_prometheus_running()

    for name in CONFIGS:
        print(f"\n⏳ Waiting for host to become idle before running: {name}")
        wait_until_idle()
        run_experiment(name, CONFIGS[name])
        if name == "idle-baseline":
            time.sleep(20)
        

    print("\nDrawing VM power charts...")
    plot_vm_power_per_config()
    print("\nDrawing Energy Comparison Chart...")
    energy_data = {}
    for name in CONFIGS:
        energy_file = os.path.join(RESULT_DIR, f"energy_{name}.txt")
        if os.path.exists(energy_file):
            with open(energy_file) as f:
                try:
                    joules = float(f.read().strip())
                    energy_data[name] = joules
                except:
                    continue

    if energy_data:
        fig_width = min(max(8, len(energy_data) * 1.2), 24)
        plt.figure(figsize=(fig_width,5))
        energy_j = {k: v for k,v in energy_data.items()}
        plt.bar(energy_j.keys(), energy_j.values(), width = 0.6)
        plt.ylabel("Energy (J)")
        plt.title("Spark Configurations vs. Energy Consumption")
        plt.grid(axis="y")
        plt.xticks(rotation=30, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(FIG_DIR, "energy_comparison.png"))
        print("✅ Energy bar chart saved: result/finalpic/energy_comparison.png")
    else:
        print("⚠️ No energy data found, skipping energy comparison chart")

if __name__ == "__main__":
    main()
