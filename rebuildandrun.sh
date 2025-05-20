#!/bin/bash

set -e  # 遇到错误就停止执行

echo "📦 1. 进入 spark-data-generator 目录并打包..."
cd /home/zsong/continuum/spark-data-generator
sbt clean package

echo "📁 2. 拷贝 jar 包到 tpcds 目录..."
cp target/scala-2.12/parquet-data-generator_2.12-1.0.jar /home/zsong/continuum/tpcds/

echo "🐳 3. 构建 Docker 镜像..."
cd /home/zsong/continuum/tpcds/
docker build -t elliesgood00/tpcds-image:v9 .

echo "🚀 4. 推送镜像到 Docker Hub..."
docker push elliesgood00/tpcds-image:v9

echo "🧪 5. 运行 runtpcds 脚本..."
cd /home/zsong/continuum
python3 runtpcds.py
