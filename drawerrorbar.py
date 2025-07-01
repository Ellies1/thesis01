# import os
# import numpy as np
# import matplotlib.pyplot as plt

# # === 可配置部分 ===
# experiment_ids = ["T1-2", "T5-q5", "T5-q18", "T5-q64"]  # 修改这里即可添加或减少实验组
# base_path = "/home/zsong/continuum/TPCDSEC616"
# result_dirs = ["result_1", "result_2", "result_3"]
# output_path = "5.png"

# # === 准备数据 ===
# means = []
# stds = []
# labels = []

# for exp_id in experiment_ids:
#     values = []
#     for result_dir in result_dirs:
#         file_path = os.path.join(base_path, result_dir, f"energy_{exp_id}.txt")
#         try:
#             with open(file_path, 'r') as f:
#                 value = float(f.readline().strip())
#                 values.append(value)
#         except Exception as e:
#             print(f"Failed to read {file_path}: {e}")
    
#     if values:
#         mean = np.mean(values)
#         std = np.std(values, ddof=1)  # 样本标准差
#         means.append(mean)
#         stds.append(std)
#         labels.append(exp_id)

# # === 绘图 ===
# x = np.arange(len(labels))
# plt.figure(figsize=(10, 5))
# bars = plt.bar(x, means, yerr=stds, capsize=5, color='green')
# plt.xticks(x, labels)
# plt.ylabel("Energy (J)")
# exp_names = ', '.join(experiment_ids)
# plt.title(f"Energy Consumption with Error Bars ({exp_names})")

# # 添加柱子内部的数值标签
# for i, exp_id in enumerate(experiment_ids):
#     values = []
#     for result_dir in result_dirs:
#         file_path = os.path.join(base_path, result_dir, f"energy_{exp_id}.txt")
#         try:
#             with open(file_path, 'r') as f:
#                 value = float(f.readline().strip())
#                 values.append(value)
#         except:
#             continue
#     if values:
#         y = means[i]
#         min_v = min(values)
#         max_v = max(values)
#         label = f"min: {min_v:.1f}\nmean: {y:.1f}\nmax: {max_v:.1f}"
#         plt.text(x[i], y / 2, label, ha='center', va='center', fontsize=10, color='black', fontweight='bold')


# plt.tight_layout()
# plt.savefig(output_path)
# plt.close()
# print(f"[✅] Plot saved to {output_path}")

import os
import numpy as np
import matplotlib.pyplot as plt

# === 可配置部分 ===
# experiment_ids = ["T4-1", "T4-2", "T4-3"]  # 示例
# experiment_ids = ["T3-1", "T3-2", "T3-3"]  # 示例
# experiment_ids = ["T2-1", "T2-2", "T2-3"]  # 示例
experiment_ids = ["T1-2", "T5-q5", "T5-q18", "T5-q64"]  # 示例
output_path = "5.png"

label_map = {
    # Type 1: Strong Scaling
    "T1-1": "1E×1C\n4g",
    # "T1-2": "2E×2C\n6g",
    "T1-2": "Query q3",
    "T1-3": "1E×4C\n8g",
    "T1-4": "2E×3C\n6g",

    # Type 2: Fixed Resource, Increasing Load
    "T2-1": "2E×2C\n6g\n×6x",
    "T2-2": "2E×2C\n6g\n×12x",
    "T2-3": "2E×2C\n6g\n×18x",

    # Type 3: Weak Scaling
    "T3-1": "1E×2C\n4g\n×3x",
    "T3-2": "2E×2C\n4g\n×6x",
    "T3-3": "4E×2C\n4g\n×12x",

    # Type 4: Fixed Load, Split Across Workers
    "T4-1": "1E×6C\n12g",
    "T4-2": "2E×3C\n6g",
    "T4-3": "3E×2C\n4g",

    # Type 5: Query Comparison
    "T5-q5":  "Query q5",
    "T5-q18": "Query q18",
    "T5-q64": "Query q64"
}

base_path = "/home/zsong/continuum/TPCDSEC616"
result_dirs = ["result_1", "result_2", "result_3"]

# === 准备数据 ===
means = []
stds = []
xlabels = []

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
        std = np.std(values, ddof=1)
        means.append(mean)
        stds.append(std)
        xlabels.append(label_map.get(exp_id, exp_id))  # 显示映射后的标签

# === 绘图 ===
x = np.arange(len(xlabels))
plt.figure(figsize=(8, 4))  # 更紧凑
bars = plt.bar(x, means, yerr=stds, capsize=5, color='seagreen')  # 改为浅绿色
plt.xticks(x, xlabels, fontsize=11)
plt.ylabel("Energy (J)")
# plt.ylim(0, max(means) * 1.1)  # Y轴顶部紧贴最大值
# top = int(np.ceil(max(means) * 1.1))  # Y轴最大值向上取整成整数
# step = 20 if top < 200 else 50
# ticks = list(range(0, top, step))
# if ticks[-1] < top:
#     ticks.append(top)  # 确保顶端值一定显示

# plt.ylim(0, top)
# plt.yticks(ticks)

raw_top = max(means) * 1.1
step = 50
top = int(np.ceil(raw_top / step) * step)  # 比如 548 -> 550

ticks = list(range(0, top, step))
if ticks[-1] < top:
    ticks.append(top)

plt.ylim(0, top)
plt.yticks(ticks)



plt.grid(axis='y', linestyle='--', alpha=0.6)  # 加横向网格线
# ❌ 去掉标题
# plt.title(...)

# 添加柱内标签（改为白字以增强对比）
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
        plt.text(x[i], y / 2, label, ha='center', va='center', fontsize=10,
                 color='white', fontweight='bold')

plt.tight_layout()
plt.savefig(output_path)
plt.close()
print(f"[✅] Plot saved to {output_path}")
