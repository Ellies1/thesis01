import os
import numpy as np
import matplotlib.pyplot as plt

# === 可配置部分 ===
experiment_ids = ["T1-1", "T1-2", "T5-q64"]  # 修改这里即可添加或减少实验组
base_path = "/home/zsong/continuum/TPCDSEC616"
result_dirs = ["result_1", "result_2", "result_3"]
output_path = "1.png"

# === 准备数据 ===
means = []
stds = []
labels = []

for exp_id in experiment_ids:
    values = []
    for result_dir in result_dirs:
        file_path = os.path.join(base_path, result_dir, f"energy_{exp_id}.txt")
        try:
            with open(file_path, 'r') as f:
                value = float(f.readline().strip())
                values.append(value)
        except Exception as e:
            print(f"Failed to read {file_path}: {e}")
    
    if values:
        mean = np.mean(values)
        std = np.std(values, ddof=1)  # 样本标准差
        means.append(mean)
        stds.append(std)
        labels.append(exp_id)

# === 绘图 ===
x = np.arange(len(labels))
plt.figure(figsize=(10, 5))
bars = plt.bar(x, means, yerr=stds, capsize=5, color='green')
plt.xticks(x, labels)
plt.ylabel("Energy (J)")
exp_names = ', '.join(experiment_ids)
plt.title(f"Energy Consumption with Error Bars ({exp_names})")

# 添加柱子内部的数值标签
for i, exp_id in enumerate(experiment_ids):
    values = []
    for result_dir in result_dirs:
        file_path = os.path.join(base_path, result_dir, f"energy_{exp_id}.txt")
        try:
            with open(file_path, 'r') as f:
                value = float(f.readline().strip())
                values.append(value)
        except:
            continue
    if values:
        y = means[i]
        min_v = min(values)
        max_v = max(values)
        label = f"min: {min_v:.1f}\nmean: {y:.1f}\nmax: {max_v:.1f}"
        plt.text(x[i], y - 5, label, ha='center', va='top', fontsize=8, color='white')

plt.tight_layout()
plt.savefig(output_path)
plt.close()
print(f"[✅] Plot saved to {output_path}")

