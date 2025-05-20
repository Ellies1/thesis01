#!/bin/bash

set -e

echo "✅ 1. 切换到工作目录..."
mkdir -p ~/scaphandre_latest
cd ~/scaphandre_latest

echo "✅ 2. 清理之前clone的scaphandre（如果有）..."
rm -rf scaphandre || true

echo "✅ 3. 安装依赖..."
sudo apt update
sudo apt install -y curl git cargo build-essential pkg-config libssl-dev libvirt-dev

echo "✅ 4. 克隆最新的 Scaphandre (main分支)..."
git clone https://github.com/hubblo-org/scaphandre.git
cd scaphandre

echo "✅ 5. 编译 (禁用默认特性，只启用 json prometheus qemu)..."
cargo build --release --no-default-features --features "json,prometheus,qemu"

echo "✅ 6. 安装新版本到 /usr/local/bin/scaphandre（不会覆盖你老版）..."
sudo cp target/release/scaphandre /usr/local/bin/scaphandre
sudo chmod +x /usr/local/bin/scaphandre

echo "✅ 完成！新版本信息："
scaphandre --help
