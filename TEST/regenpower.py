import os
import pandas as pd
import matplotlib.pyplot as plt

RESULT_DIR = "/home/zsong/continuum/TEST/result"
FIG_DIR = "./figures"
os.makedirs(FIG_DIR, exist_ok=True)

def plot_grouped_vm_power():
    vm_file = os.path.join(RESULT_DIR, "vm_power_dynamic.txt")
    if not os.path.exists(vm_file):
        print("⚠️ No VM power data found.")
        return

    df = pd.read_csv(vm_file, names=["timestamp", "vm", "power", "config"])
    df = df[df["vm"] == "cloud0_zsong"]

    # 根据配置名的前缀分组
    def get_group(config):
        if config == "idle-baseline":
            return "idle-baseline"
        return config.split("-")[0]

    df["group"] = df["config"].apply(get_group)

    grouped = df.groupby("group")
    for group_name, group_df in grouped:
        plt.figure(figsize=(8, 4))
        for config in sorted(group_df["config"].unique()):
            sub_df = group_df[group_df["config"] == config].copy()
            sub_df["timestamp"] = sub_df["timestamp"] - sub_df["timestamp"].min()
            plt.plot(sub_df["timestamp"], sub_df["power"], label=config)

        plt.xlabel("Time (s)")
        plt.ylabel("Estimated VM Power (W)")
        plt.title(f"Power Over Time - cloud0_zsong - {group_name}")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        out_path = os.path.join(FIG_DIR, f"vm_power_group_{group_name}.png")
        plt.savefig(out_path)
        print(f"✅ Saved: {out_path}")

plot_grouped_vm_power()
