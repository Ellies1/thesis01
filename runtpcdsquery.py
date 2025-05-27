import subprocess

# === 用户可修改配置 ===
scale_factor = 1
query_file = "q3-v2.4"  # 容器内的查询脚本路径

# === 固定设置 ===
vm_ip = "192.168.166.3"
vm_user = "cloud0_zsong"
vm_ssh_key = "/home/zsong/.ssh/id_rsa_continuum"
vm_data_dir = "/tpcds-data"
dataset_dir = f"{vm_data_dir}/dataset_tpcds_{scale_factor}g"

def run(command, desc, exit_on_fail=True, shell=False):
    print(f"🔧 {desc}...")
    try:
        subprocess.run(command, check=True, shell=shell)
        print(f"✅ {desc} 成功\n")
    except subprocess.CalledProcessError as e:
        print(f"❌ {desc} 失败: {e}\n")
        if exit_on_fail:
            exit(1)

# Step 1: 检查并清理旧数据（如有）
check_dataset_cmd = f'ssh {vm_user}@{vm_ip} -i {vm_ssh_key} "test -d {dataset_dir} && echo FOUND || echo NOT_FOUND"'
check_output = subprocess.getoutput(check_dataset_cmd)
if "FOUND" in check_output:
    print("⚠️ 检测到旧数据，即将清理...\n")
    cleanup_cmd = [
        "ssh", f"{vm_user}@{vm_ip}", "-i", vm_ssh_key,
        f"sudo rm -rf {vm_data_dir}"
    ]
    run(cleanup_cmd, f"删除旧目录 {vm_data_dir}")

# Step 2: 创建 VM 上的数据目录
mkdir_cmd = [
    "ssh", f"{vm_user}@{vm_ip}", "-i", vm_ssh_key,
    f"sudo mkdir -p {vm_data_dir} && sudo chmod -R 777 {vm_data_dir}"
]
run(mkdir_cmd, f"创建 VM 上的目录 {vm_data_dir}")

# Step 3: 运行 datagen
spark_submit_base = [
    "~/continuum/spark-3.4.4-bin-hadoop3/bin/spark-submit",
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
spark_submit_base[0] = spark_submit_base[0].replace("~", subprocess.getoutput("echo $HOME"))

run(spark_submit_base + ["datagen", vm_data_dir, "/opt/tpcds-kit/tools", str(scale_factor)],
    f"提交 Spark 数据生成任务 (scaleFactor={scale_factor}g)")

# Step 4: 执行 metagen 建表
print("🚀 Metagen command:", " ".join(spark_submit_base + ["metagen", vm_data_dir, str(scale_factor)]))
run(spark_submit_base + ["metagen", vm_data_dir, str(scale_factor)],
    "提交 Spark 元数据任务 (metagen)")

# Step 5: 执行 query
query_cmd = spark_submit_base + ["query", query_file, str(scale_factor)]
print("🚀 Query command:", " ".join(query_cmd))
run(spark_submit_base + ["query", query_file, str(scale_factor)],
    f"提交 Spark 查询任务 ({query_file})")

