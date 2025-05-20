import subprocess
import time

# [0] Create silent log4j config locally
log4j_local_path = "/tmp/log4j-silent.properties"
with open(log4j_local_path, "w") as f:
    f.write("""log4j.rootCategory=ERROR, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c: %m%n
""")

# [0.1] Copy log4j config to VM (quiet mode)
subprocess.run([
    "scp", "-i", "/home/zsong/.ssh/id_rsa_continuum",
    log4j_local_path,
    "cloud0_zsong@192.168.166.3:~/spark/conf/log4j-silent.properties"
], stdout=subprocess.DEVNULL)

print("[1] Collecting pre-indicators...")
subprocess.run("curl http://localhost:8081/metrics > before.txt", shell=True)

print("[2] Submitting SparkPi to VM (this takes a few minutes)...")
ssh_cmd = (
    "SPARK_CONF_DIR=~/spark/conf "
    "~/spark/bin/spark-submit "
    "--class org.apache.spark.examples.SparkPi "
    "--master local "
    "~/spark/examples/jars/spark-examples_2.12-3.5.5.jar 100000"
)
# Run silently via subprocess
subprocess.run([
    "ssh", "-i", "/home/zsong/.ssh/id_rsa_continuum",
    "cloud0_zsong@192.168.166.3", ssh_cmd
], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

print("[3] Waiting 5 seconds before collecting post-metrics...")
time.sleep(5)
subprocess.run("curl http://localhost:8081/metrics > after.txt", shell=True)

print("[4] Analyzing the differences...")
with open("before.txt") as f1:
    before = f1.readlines()
with open("after.txt") as f2:
    after = f2.readlines()

# Only include newly added lines with 'joules'
diff_lines = [line for line in after if line not in before and "joules" in line.lower()]
with open("diff_result.txt", "w") as f:
    f.writelines(diff_lines)

print("Filtered diff saved in diff_result.txt")
