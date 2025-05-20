import subprocess
import time
import os
import matplotlib.pyplot as plt

# === config settings ===
SCAPH_PATH = "/home/zsong/scaphandre_setup/scaphandre/target/release/scaphandre"
SPARK_HOME = os.path.expanduser("~/continuum/spark-3.4.4-bin-hadoop3")
SPARK_SUBMIT = os.path.join(SPARK_HOME, "bin/spark-submit")

RESULT_DIR = "result"
FIG_DIR = os.path.join(RESULT_DIR, "finalpic")
os.makedirs(RESULT_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)

# === Kubernetes settings ===
K8S_MASTER_URL = "k8s://https://192.168.166.2:6443"
SPARK_IMAGE = "elliesgood00/1213:k8s-trusted-with-example"
SPARK_JAR_PATH_IN_CONTAINER = "local:///opt/contistuff/spark-examples_2.12-3.4.4.jar"

CONFIGS = {
    "idle-baseline": {},  
    "low-small":  {"params": 5000,  "threads": 1, "memory": "1g", "instances": 1},  
    "mid-medium": {"params": 10000, "threads": 2, "memory": "2g", "instances": 1},  
    "high-large": {"params": 20000, "threads": 2, "memory": "2g", "instances": 1},  
}

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

def start_scaphandre():
    try:
        subprocess.check_output("curl -s http://localhost:8081/metrics", shell=True)
        print("[💡] Scaphandre is already running and listening on 8081, skipping startup")
        return None
    except subprocess.CalledProcessError:
        print("[💡] Start Scaphandre to collect the energy consumption of the entire process at one time")
        return subprocess.Popen([
            "sudo", SCAPH_PATH, "prometheus",
            "--address", "0.0.0.0", "--port", "8081", "--qemu"
        ])

def stop_process(proc):
    if proc:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception as e:
            print(f"[!] Error stopping Scaphandre: {e}")

def collect_metrics(file):
    subprocess.run(f"curl http://localhost:8081/metrics > {file}", shell=True)

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

def analyze_diff(before_file, after_file, output_file):
    with open(before_file) as f1, open(after_file) as f2:
        before_lines = [line for line in f1 if "microjoules" in line.lower()]
        after_lines = [line for line in f2 if "microjoules" in line.lower()]

    before_dict = {}
    for line in before_lines:
        parts = line.strip().split()
        if len(parts) == 2:
            before_dict[parts[0]] = int(parts[1])

    with open(output_file, "w") as f:
        for line in after_lines:
            parts = line.strip().split()
            if len(parts) == 2:
                metric, after_val = parts[0], int(parts[1])
                before_val = before_dict.get(metric, 0)
                delta = after_val - before_val
                f.write(f"{metric} {delta}\n")


def run_experiment(name, config):
    print(f"\n===== Start the experiment: {name} =====")
    before = os.path.join(RESULT_DIR, f"before_{name}.txt")
    after = os.path.join(RESULT_DIR, f"after_{name}.txt")
    diff = os.path.join(RESULT_DIR, f"diff_{name}.txt")

    print("[1] Energy consumption of collection (before)")
    collect_metrics(before)

    if not config:
        print(f"[2] No Spark task submitted for {name} (idle measurement)...")
        time.sleep(10)
    else:
        print(f"[2] Submit SparkPi task, total {config['instances']} instances...")
        for i in range(config["instances"]):
            print(f"   -> Instance {i+1}/{config['instances']}")
            success = run_spark_job_on_k8s(config["threads"], config["memory"], config["params"])
            if not success:
                print(f"[⚠️] The Spark task for experiment {name} failed, skipping energy analysis")
                return

    print("[3] Wait for the task to complete and collect the energy consumption (after)...")
    collect_metrics(after)

    print("[4] Analyze changes in energy consumption")
    analyze_diff(before, after, diff)
    print(f"✅ Experiment {name} results saved to {diff}")

def extract_energy(filename):
    total = 0
    with open(filename) as f:
        for line in f:
            if "scaph_host_energy_microjoules" in line:
                try:
                    total += int(line.strip().split()[-1])
                except:
                    continue
    return total if total > 0 else None


def main():
    install_spark_if_needed()

    print("Start Scaphandre to collect the energy consumption of the entire process at one time")
    scaph_proc = start_scaphandre()
    time.sleep(5)

    if "idle-baseline" in CONFIGS:
        run_experiment("idle-baseline", CONFIGS["idle-baseline"])
        print("Waiting extra 20s after idle-baseline to ensure full release of resources...")
        time.sleep(20)

    for name in CONFIGS:
        if name == "idle-baseline":
            continue
        run_experiment(name, CONFIGS[name])

    print("[🛑] All experiments are completed, stop Scaphandre")
    stop_process(scaph_proc)

    print("\nAnalyzing energy consumption data and generating graphs...")
    energy_data = {}
    for name in CONFIGS.keys():
        diff_file = os.path.join(RESULT_DIR, f"diff_{name}.txt")
        if os.path.exists(diff_file):
            value = extract_energy(diff_file)
            if value is not None:
                energy_data[name] = value
            else:
                print(f"[⚠️] No valid energy value found in {diff_file}, skip.")


    if energy_data:
        plt.figure(figsize=(8, 5))
        energy_kj = {k: v / 1e6 for k, v in energy_data.items()}
        plt.bar(energy_kj.keys(), energy_kj.values())
        plt.ylabel("Energy (kJ)")
        plt.title("Spark Configurations vs. Energy Consumption")
        plt.grid(axis="y")
        plt.tight_layout()
        plt.savefig(os.path.join(FIG_DIR, "energy_comparison.png"))
        print("✅ The chart has been saved as result/finalpic/energy_comparison.png")
    else:
        print("⚠️ No diff files found, skipping drawing")

if __name__ == "__main__":
    main()
