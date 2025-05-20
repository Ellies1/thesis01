import subprocess
import time
import os
import matplotlib.pyplot as plt
import threading
import numpy as np

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

def run_spark_job_on_k8s(threads, memory, params):
    LOCAL_JAVA_HOME = "/home/zsong/.sdkman/candidates/java/11.0.26-amzn"
    cmd = f"""
export JAVA_HOME={LOCAL_JAVA_HOME} && export PATH=$JAVA_HOME/bin:$PATH && \
{SPARK_SUBMIT} \
--class org.apache.spark.examples.SparkPi \
--master {K8S_MASTER_URL} \
--deploy-mode cluster \
--conf spark.kubernetes.driver.uploadK8sResources=false \
--conf spark.executor.instances=1 \
--conf spark.executor.cores={threads} \
--conf spark.executor.memory={memory} \
--conf spark.driver.memory=1g \
--conf spark.kubernetes.container.image.pullPolicy=Always \
--conf spark.kubernetes.driver.container.image={SPARK_IMAGE} \
--conf spark.kubernetes.executor.container.image={SPARK_IMAGE} \
--conf spark.kubernetes.driverEnv.JAVA_HOME=/opt/java/openjdk \
--conf spark.kubernetes.executorEnv.JAVA_HOME=/opt/java/openjdk \
--conf spark.app.name=spark-ca-debug \
--conf spark.driver.extraJavaOptions="-Djavax.net.ssl.trustStore=/opt/java/openjdk/lib/security/cacerts -Djavax.net.ssl.trustStorePassword=changeit" \
--conf spark.executor.extraJavaOptions="-Djavax.net.ssl.trustStore=/opt/java/openjdk/lib/security/cacerts -Djavax.net.ssl.trustStorePassword=changeit" \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
{SPARK_JAR_PATH_IN_CONTAINER} {params}
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
    power_out = os.path.join(RESULT_DIR, f"power_{name}.txt")
    energy_out = os.path.join(RESULT_DIR, f"energy_{name}.txt")

    power_data = []
    stop_signal = threading.Event()

    start_energy = read_energy_uj()
    if start_energy is None:
        print("[⚠️] Start energy read failed, skipping...")
        return

    def power_collector():
        while not stop_signal.is_set():
            try:
                metrics = subprocess.check_output("curl -s http://localhost:8081/metrics", shell=True).decode()
                for line in metrics.splitlines():
                    if "scaph_host_power_microwatts" in line:
                        try:
                            power = int(line.strip().split()[-1])
                            power_data.append((time.time(), power))
                            break
                        except ValueError:
                            continue
            except subprocess.CalledProcessError:
                pass
            time.sleep(2)

    print("[1] Start collecting power in background...")
    collector_thread = threading.Thread(target=power_collector)
    collector_thread.start()

    if not config:
        print(f"[2] No Spark task submitted for {name} (idle measurement)...")
        print("   -> Waiting 10s for idle stabilization before recording useful idle data.")
        time.sleep(10)
        power_data.clear()
        print("   -> Now collecting valid idle-baseline data.")
        time.sleep(30)
    else:
        print(f"[2] Submit SparkPi task, total {config['instances']} instances...")
        for i in range(config["instances"]):
            print(f"   -> Instance {i+1}/{config['instances']}")
            success = run_spark_job_on_k8s(config["threads"], config["memory"], config["params"])
            if not success:
                print(f"[⚠️] The Spark task for experiment {name} failed, stopping power collection")
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
    with open(power_out, "w") as f:
        for ts, pw in power_data:
            f.write(f"{ts},{pw}\n")

    with open(energy_out, "w") as f:
        f.write(f"{total_energy_joules}\n")

    print(f"✅ Experiment {name} finished. Estimated total energy: {total_energy_joules:.6f} Joules")

def main():
    install_spark_if_needed()
    ensure_scaphandre_qemu_running()
    ensure_scaphandre_prometheus_running()
    if "idle-baseline" in CONFIGS:
        run_experiment("idle-baseline", CONFIGS["idle-baseline"])
        print("Waiting extra 20s after idle-baseline to ensure full release of resources...")
        time.sleep(20)

    for name in CONFIGS:
        if name == "idle-baseline":
            continue
        run_experiment(name, CONFIGS[name])

    print("\nAnalyzing and drawing graphs...")
    SAMPLING_INTERVAL = 2
    energy_data = {}

    for name in CONFIGS.keys():
        power_file = os.path.join(RESULT_DIR, f"power_{name}.txt")
        energy_file = os.path.join(RESULT_DIR, f"energy_{name}.txt")

        if os.path.exists(power_file):
            powers = []
            with open(power_file) as f:
                for line in f:
                    try:
                        _, pw = map(float, line.strip().split(","))
                        powers.append(pw / 1e6)
                    except:
                        continue

            if powers:
                timestamps = [i * SAMPLING_INTERVAL for i in range(len(powers))]
                plt.figure(figsize=(8, 4))
                plt.plot(timestamps, powers)
                plt.xlabel("Time (s)")
                plt.ylabel("Power (W)")
                plt.title(f"Power Usage Over Time: {name}")
                plt.grid(True)
                plt.tight_layout()
                plt.savefig(os.path.join(FIG_DIR, f"power_over_time_{name}.png"))
                print(f"✅ Power chart saved: power_over_time_{name}.png")

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
