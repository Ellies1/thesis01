#!/bin/bash

# 
echo "[🔁] Rebuilding VMs..."
sleep 1
rm -rf /mnt/sdc/zsong/.continuum && \
    sudo sysctl -p /etc/sysctl.conf && \
    python3 continuum.py configuration/qemu_kube_only.cfg

# 
echo "[⚙️] Configuring controller VM..."
ssh cloud_controller_zsong@192.168.166.2 -i /home/zsong/.ssh/id_rsa_continuum 'bash -s' <<'EOF'

# 
sudo apt update && \
  sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin && \
  sudo apt install -y docker-compose && \
  sudo groupadd docker || true && \
  sudo usermod -aG docker $USER && \
  sudo systemctl enable docker.service && \
  sudo systemctl enable containerd.service && \
  sudo systemctl start docker

# 
cd ~ && \
  sudo apt install -y zip unzip && \
  curl -s "https://get.sdkman.io" | bash && \
  source "$HOME/.sdkman/bin/sdkman-init.sh" && \
  sdk install java 11.0.26-amzn

# 
git clone https://github.com/sacheendra/spark-data-generator.git && \
  cd ~/spark-data-generator/docker && \
  mkdir db_data warehouse && \
  docker-compose up -d

# 
sudo apt-get update && \
  sudo apt-get install apt-transport-https curl gnupg -yqq && \
  echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list && \
  echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list && \
  curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | \
  sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
  sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg && \
  sudo apt-get update && \
  sudo apt-get install sbt -y && \
  cd ~/spark-data-generator && \
  sbt package

# 
cd ~/spark-data-generator && \
  sudo docker build -t "spark-benchmark-v1.0" -f docker/Dockerfile . && \
  sudo docker tag spark-benchmark-v1.0 sacheendra/contispark:bench-v1.0

# 
cd ~ && \
  wget https://archive.apache.org/dist/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz && \
  tar xvf spark-3.4.4-bin-hadoop3.tgz && \
  rm spark-3.4.4-bin-hadoop3.tgz && \
  cp ~/spark-data-generator/docker/spark-conf/spark-defaults.conf ~/spark-3.4.4-bin-hadoop3/conf/

# 自动修改 spark-defaults.conf 中的 metastore URI
sed -i 's|spark.hive.metastore.uris.*|spark.hive.metastore.uris thrift://192.168.166.2:6443|' ~/spark-3.4.4-bin-hadoop3/conf/spark-defaults.conf

# 创建 serviceAccount 和 roleBinding
kubectl create serviceaccount spark || true
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default || true

# 开启匿名绑定（避免 system:anonymous 错误）
kubectl create clusterrolebinding spark-anonymous-binding \
  --clusterrole=edit \
  --user=system:anonymous || true

# 
sudo cp /etc/kubernetes/pki/ca.crt /home/cloud_controller_zsong/
sudo chown cloud_controller_zsong:cloud_controller_zsong /home/cloud_controller_zsong/ca.crt

# 初始化 spark parquet generator
cd ~/spark-3.4.4-bin-hadoop3 && \
./bin/spark-submit \
  --class "ParquetGenerator" \
  --master k8s://https://192.168.166.2:6443 \
  --deploy-mode cluster \
  --conf spark.executor.instances=1 \
  --conf spark.kubernetes.executor.request.cores=1 \
  --conf spark.kubernetes.executor.limit.cores=1 \
  --conf spark.executor.memory=512m \
  local:///opt/contistuff/parquet-data-generator_2.12-1.0.jar \
  test file:///opt/spark/output-lite

EOF

# 
echo " Importing CA certificate on node6..."
scp -i ~/.ssh/id_rsa_continuum cloud_controller_zsong@192.168.166.2:/home/cloud_controller_zsong/ca.crt ~/ca.crt

sudo ~/.sdkman/candidates/java/11.0.26-amzn/bin/keytool \
  -delete \
  -alias k8s-root-ca \
  -keystore ~/.sdkman/candidates/java/11.0.26-amzn/lib/security/cacerts \
  -storepass changeit || true

sudo ~/.sdkman/candidates/java/11.0.26-amzn/bin/keytool -import -trustcacerts \
  -alias k8s-root-ca \
  -file ~/ca.crt \
  -keystore ~/.sdkman/candidates/java/11.0.26-amzn/lib/security/cacerts \
  -storepass changeit \
  -noprompt

echo "done!"
