import os
import matplotlib.pyplot as plt

# === 路径配置 ===
RESULT_DIR = "result"
FIG_DIR = os.path.join(RESULT_DIR, "finalpic")
os.makedirs(FIG_DIR, exist_ok=True)

# === 读取已有的 energy_xxx.txt 文件 ===
energy_data = {}  # {config_name: (datagen, metagen, query)}
for filename in os.listdir(RESULT_DIR):
    if filename.startswith("energy_") and filename.endswith(".txt"):
        config_name = filename.replace("energy_", "").replace(".txt", "")
        with open(os.path.join(RESULT_DIR, filename)) as f:
            lines = f.readlines()
            if len(lines) == 1:
                # 单段数据（老格式），跳过或报错
                continue
            elif len(lines) >= 4:
                try:
                    dg = float(lines[1])
                    mt = float(lines[2])
                    qr = float(lines[3])
                    energy_data[config_name] = (dg, mt, qr)
                except:
                    print(f"⚠️ Cannot parse: {filename}")

# === 绘图 ===
if energy_data:
    labels = list(energy_data.keys())
    e_dg = [energy_data[k][0] for k in labels]
    e_mt = [energy_data[k][1] for k in labels]
    e_qr = [energy_data[k][2] for k in labels]
    bottom_sum = [dg + mt for dg, mt in zip(e_dg, e_mt)]

    fig_width = min(max(8, len(labels) * 1.2), 24)
    plt.figure(figsize=(fig_width, 5))
    plt.bar(labels, e_dg, label="Data Generation", color="blue")
    plt.bar(labels, e_mt, bottom=e_dg, label="Table Creation", color="orange")
    plt.bar(labels, e_qr, bottom=bottom_sum, label="Query Execution", color="green")

    for i, label in enumerate(labels):
        plt.text(i, e_dg[i] / 2, f"{e_dg[i]:.2f}", ha='center', va='center', fontsize=8, color='white')
        plt.text(i, e_dg[i] + e_mt[i] / 2, f"{e_mt[i]:.2f}", ha='center', va='center', fontsize=8, color='black')
        plt.text(i, bottom_sum[i] + e_qr[i] / 2, f"{e_qr[i]:.2f}", ha='center', va='center', fontsize=6, color='black')
        total = e_dg[i] + e_mt[i] + e_qr[i]
        plt.text(i, total + 30, f"{total:.2f}", ha='center', va='bottom', fontsize=9, color='black')

    plt.ylabel("Energy (J)")
    plt.title("Spark Configurations vs. Energy Consumption (Split by Phase)")
    plt.xticks(rotation=30, ha='right')
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.grid(axis="y")
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, "energy_comparison_manual.png"))
    print("✅ 图已生成：energy_comparison_manual.png")
else:
    print("⚠️ 没有找到合法的能耗数据")
