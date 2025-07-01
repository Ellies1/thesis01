import os
import matplotlib.pyplot as plt

# === 配置 ===
BASE_DIR = "/home/zsong/continuum/TEST/result"
FIG_DIR = "result/finalpic"
os.makedirs(FIG_DIR, exist_ok=True)

# === 读取能耗数据 ===
energy_data = {}
for filename in os.listdir(BASE_DIR):
    if filename.startswith("energy_") and filename.endswith(".txt"):
        filepath = os.path.join(BASE_DIR, filename)
        try:
            with open(filepath, "r") as f:
                value = float(f.readline().strip())
                label = filename.replace("energy_", "").replace(".txt", "")
                energy_data[label] = value
        except Exception as e:
            print(f"❌ Failed to read {filename}: {e}")

# # === 绘图 ===
# if energy_data: 
#     fig_width = min(max(8, len(energy_data) * 1.2), 24)
#     plt.figure(figsize=(fig_width, 5))
#     labels = list(energy_data.keys())
#     values = list(energy_data.values())

#     bars = plt.bar(labels, values, width=0.6)
#     plt.ylabel("Energy (J)")
#     plt.title("Spark Configurations vs. Energy Consumption", fontsize=16)
#     plt.grid(axis="y")
#     plt.xticks(rotation=30, ha='right')

#     # 添加柱顶数值
#     for bar in bars:
#         height = bar.get_height()
#         plt.text(bar.get_x() + bar.get_width() / 2, height + 20, f"{height:.0f}", 
#                  ha='center', va='bottom', fontsize=9)

#     plt.tight_layout()
#     output_path = os.path.join(FIG_DIR, "energy_comparisonv2.png")
#     plt.savefig(output_path)
#     print(f"✅ Energy bar chart saved: {output_path}")
# else:
#     print("⚠️ No energy data found, skipping energy comparison chart")
# === 自定义顺序 ===
custom_order = [
    "idle-baseline",
    "ss-1", "ss-2", "ss-4",
    "iw-1", "iw-2", "iw-3",
    "ws-1", "ws-2", "ws-3",
    "sp-2in", "sp-4in"
]

# === 按顺序提取数据 ===
labels = [label for label in custom_order if label in energy_data]
values = [energy_data[label] for label in labels]

# === 绘图（维持原逻辑）===
fig_width = min(max(8, len(labels) * 1.2), 24)
plt.figure(figsize=(fig_width, 5))

bars = plt.bar(labels, values, width=0.6)
plt.ylabel("Energy (J)")
plt.title("Spark Configurations vs. Energy Consumption", fontsize=16)
plt.grid(axis="y")
plt.xticks(rotation=30, ha='right')

# 添加柱顶数值（保留 1 位小数）
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, height + 20, f"{height:.1f}", 
             ha='center', va='bottom', fontsize=9)

plt.tight_layout()
output_path = os.path.join(FIG_DIR, "energy_comparisonv2.png")
plt.savefig(output_path)
print(f"✅ Energy bar chart saved: {output_path}")
