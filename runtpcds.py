# import subprocess

# vm_ip = "192.168.166.3"
# vm_user = "cloud0_zsong"
# vm_ssh_key = "/home/zsong/.ssh/id_rsa_continuum"
# vm_data_dir = "/tpcds-data"
# dataset_dir = f"{vm_data_dir}/dataset_tpcds_100G"

# def run(command, desc, exit_on_fail=True, shell=False):
#     print(f"🔧 {desc}...")
#     try:
#         subprocess.run(command, check=True, shell=shell)
#         print(f"✅ {desc} 成功\n")
#     except subprocess.CalledProcessError as e:
#         print(f"❌ {desc} 失败: {e}\n")
#         if exit_on_fail:
#             exit(1)

# # Step 1: 检查 VM 上是否已有数据
# check_dataset_cmd = f'ssh {vm_user}@{vm_ip} -i {vm_ssh_key} "test -d {dataset_dir} && ls -lh {dataset_dir} || echo NOT_FOUND"'
# print("🔍 检查 VM 是否已有 dataset_tpcds_100G 数据目录...")
# check_output = subprocess.getoutput(check_dataset_cmd)
# if "NOT_FOUND" not in check_output:
#     print("⚠️ 发现已有数据，是否需要删除？你可以手动 ssh 进 VM 清理：")
#     print(f"   ssh {vm_user}@{vm_ip} -i {vm_ssh_key}")
#     print(f"   sudo rm -rf {dataset_dir}\n")
# else:
#     print("✅ 数据目录尚不存在，可以继续。\n")

# # Step 2: 创建 VM 挂载目录
# mkdir_cmd = [
#     "ssh", f"{vm_user}@{vm_ip}",
#     "-i", vm_ssh_key,
#     f"sudo mkdir -p {vm_data_dir} && sudo chmod -R 777 {vm_data_dir}"
# ]
# run(mkdir_cmd, f"创建 VM 上的目录 {vm_data_dir}")

# # Step 3: 执行 spark-submit
# print("🚀 正在提交 Spark parquet generator 任务...\n")
# spark_submit_cmd = [
#     "~/continuum/spark-3.4.4-bin-hadoop3/bin/spark-submit",
#     "--class", "ParquetGenerator",
#     "--master", "k8s://https://192.168.166.2:6443",
#     "--deploy-mode", "cluster",
#     "--conf", "spark.kubernetes.container.image=elliesgood00/tpcds-image:v9",
#     "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
#     "--conf", "spark.kubernetes.executor.volumes.hostPath.tpcds.mount.path=/tpcds-data",
#     "--conf", "spark.kubernetes.executor.volumes.hostPath.tpcds.options.path=/tpcds-data",
#     "--conf", "spark.kubernetes.driver.volumes.hostPath.tpcds.mount.path=/tpcds-data",
#     "--conf", "spark.kubernetes.driver.volumes.hostPath.tpcds.options.path=/tpcds-data",
#     "--conf", "spark.executor.memory=6g",
#     "--conf", "spark.executor.memoryOverhead=2g",
#     "--conf", "spark.driver.memory=4g",
#     "--conf", "spark.driver.memoryOverhead=1g",
#     "--jars", "local:///opt/tpcds/lib/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar",
#     "local:///opt/tpcds/parquet-data-generator_2.12-1.0.jar",
#     "datagen", "/tpcds-data", "/opt/tpcds-kit/tools", "5"
# ]
# spark_submit_cmd[0] = spark_submit_cmd[0].replace("~", subprocess.getoutput("echo $HOME"))

# run(spark_submit_cmd, "提交 Spark 任务")

# # Step 4: 再次检查是否生成数据
# print("🔍 检查生成结果是否存在...")
# verify_cmd = f'ssh {vm_user}@{vm_ip} -i {vm_ssh_key} "du -sh {dataset_dir} || echo 生成失败"'
# subprocess.run(verify_cmd, shell=True)

import subprocess

# === ===
scale_factor = 5  

# === ===
vm_ip = "192.168.166.3"
vm_user = "cloud0_zsong"
vm_ssh_key = "/home/zsong/.ssh/id_rsa_continuum"
vm_data_dir = "/tpcds-data"
dataset_dir = f"{vm_data_dir}/dataset_tpcds_{scale_factor}G"

def run(command, desc, exit_on_fail=True, shell=False):
    print(f"🔧 {desc}...")
    try:
        subprocess.run(command, check=True, shell=shell)
        print(f"✅ {desc} 成功\n")
    except subprocess.CalledProcessError as e:
        print(f"❌ {desc} 失败: {e}\n")
        if exit_on_fail:
            exit(1)

# Step 1: 检查 VM 上是否已有数据
check_dataset_cmd = f'ssh {vm_user}@{vm_ip} -i {vm_ssh_key} "test -d {dataset_dir} && ls -lh {dataset_dir} || echo NOT_FOUND"'
print(f"🔍 检查 VM 是否已有 dataset_tpcds_{scale_factor}G 数据目录...")
check_output = subprocess.getoutput(check_dataset_cmd)
if "NOT_FOUND" not in check_output:
    print("⚠️ 发现已有数据，是否需要删除？你可以手动 ssh 进 VM 清理：")
    print(f"   ssh {vm_user}@{vm_ip} -i {vm_ssh_key}")
    print(f"   sudo rm -rf {dataset_dir}\n")
else:
    print("✅ 数据目录尚不存在，可以继续。\n")

# Step 2: 创建 VM 挂载目录
mkdir_cmd = [
    "ssh", f"{vm_user}@{vm_ip}",
    "-i", vm_ssh_key,
    f"sudo mkdir -p {vm_data_dir} && sudo chmod -R 777 {vm_data_dir}"
]
run(mkdir_cmd, f"创建 VM 上的目录 {vm_data_dir}")

# Step 3: 执行 spark-submit
print("🚀 正在提交 Spark parquet generator 任务...\n")
spark_submit_cmd = [
    "~/continuum/spark-3.4.4-bin-hadoop3/bin/spark-submit",
    "--class", "ParquetGenerator",
    "--master", "k8s://https://192.168.166.2:6443",
    "--deploy-mode", "cluster",
    "--conf", "spark.kubernetes.container.image=elliesgood00/tpcds-image:v9",
    "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
    "--conf", "spark.kubernetes.executor.volumes.hostPath.tpcds.mount.path=/tpcds-data",
    "--conf", "spark.kubernetes.executor.volumes.hostPath.tpcds.options.path=/tpcds-data",
    "--conf", "spark.kubernetes.driver.volumes.hostPath.tpcds.mount.path=/tpcds-data",
    "--conf", "spark.kubernetes.driver.volumes.hostPath.tpcds.options.path=/tpcds-data",
    "--conf", "spark.executor.memory=6g",
    "--conf", "spark.executor.memoryOverhead=2g",
    "--conf", "spark.driver.memory=4g",
    "--conf", "spark.driver.memoryOverhead=1g",
    "--jars", "local:///opt/tpcds/lib/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar",
    "local:///opt/tpcds/parquet-data-generator_2.12-1.0.jar",
    "datagen", vm_data_dir, "/opt/tpcds-kit/tools", str(scale_factor)
]
spark_submit_cmd[0] = spark_submit_cmd[0].replace("~", subprocess.getoutput("echo $HOME"))

run(spark_submit_cmd, f"提交 Spark 任务 (scaleFactor={scale_factor}G)")

# Step 4: 再次检查是否生成数据
print("🔍 检查生成结果是否存在...")
verify_cmd = f'ssh {vm_user}@{vm_ip} -i {vm_ssh_key} "du -sh {dataset_dir} || echo 生成失败"'
subprocess.run(verify_cmd, shell=True)
