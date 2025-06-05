import os
import pandas as pd
from vm_power_mapper import plot_vm_power_per_config

# 1. 加载原始能耗数据
df = pd.read_csv("result/vm_power_dynamic.txt", names=["timestamp", "vm", "power", "config"])

# 2. 选择一个 config，例如 "T1-3"
target_conf = "T1-3"
timestamps = df[df["config"] == target_conf]["timestamp"].values
t0 = timestamps.min()

# 3. 假设阶段时长比例，自动生成分界点
# 比如 datagen: 40%，metagen: 40%，query: 20%
total_duration = timestamps.max() - t0
ts1 = t0 + total_duration * 0.4
ts2 = t0 + total_duration * 0.8

# 4. 构建 phase_boundaries 并绘图
phase_boundaries = {
    target_conf: {
        "datagen": t0,
        "metagen": ts1,
        "query": ts2
    }
}

plot_vm_power_per_config(phase_boundaries)
