import subprocess
import time
import os
import matplotlib.pyplot as plt

SCAPH_PATH = "/home/zsong/scaphandre_setup/scaphandre/target/release/scaphandre"
SCAPH_PORT = 8081

PREVIOUS_EXPERIMENTS = {
    "low-small": "diff_low-small.txt",
    "mid-medium": "diff_mid-medium.txt",
    "high-large": "diff_high-large.txt",
    "idle-baseline": "diff_idle-baseline.txt"
}

def start_scaphandre():
    print("[🔁] Restarting Scaphandre...")
    return subprocess.Popen([
        "sudo", SCAPH_PATH, "prometheus",
        "--address", "0.0.0.0",
        "--port", str(SCAPH_PORT),
        "--qemu"
    ])

def stop_scaphandre(proc):
    if proc:
        proc.terminate()
        proc.wait()

def collect_metrics(filename):
    subprocess.run(f"curl -s http://localhost:{SCAPH_PORT}/metrics > {filename}", shell=True)

def extract_energy(filepath):
    with open(filepath) as f:
        for line in f:
            if line.startswith("scaph_host_energy_microjoules"):
                try:
                    return int(line.strip().split()[-1])
                except ValueError:
                    pass
    return None

def main():
    print("Starting idle-clean energy collection experiment...")
    proc = start_scaphandre()
    time.sleep(3)  # wait for scaphandre to initialize

    print("Collecting before energy metrics...")
    collect_metrics("before_idle-clean.txt")

    print("Sleeping 60 seconds with no activity...")
    time.sleep(60)

    print("Collecting after energy metrics...")
    collect_metrics("after_idle-clean.txt")

    print("[🛑] Stopping Scaphandre")
    subprocess.run(f"sudo pkill -f '{SCAPH_PATH}'", shell=True)
    time.sleep(1)

    before = extract_energy("before_idle-clean.txt")
    after = extract_energy("after_idle-clean.txt")

    if before is not None and after is not None:
        delta_microjoules = after - before
        delta_kj = delta_microjoules / 1_000_000
        print(f"[✅] Finished. Energy difference: {delta_kj:.2f} KJ")

        with open("diff_idle-clean.txt", "w") as f:
            f.write(f"scaph_host_energy_microjoules {delta_microjoules}\n")
    else:
        print("[❌] Failed to extract energy values.")
        return

    # Combine all energy results
    energy_data = {}

    for name, filepath in PREVIOUS_EXPERIMENTS.items():
        if os.path.exists(filepath):
            with open(filepath) as f:
                for line in f:
                    if line.startswith("scaph_host_energy_microjoules"):
                        try:
                            microjoules = int(line.strip().split()[-1])
                            energy_data[name] = microjoules / 1_000_000  # to KJ
                        except ValueError:
                            continue

    # Add current measurement
    energy_data["idle-clean"] = delta_kj

    # Plot
    if energy_data:
        plt.figure(figsize=(10, 6))
        plt.bar(energy_data.keys(), energy_data.values())
        plt.ylabel("Energy (KJ)")
        plt.title("Spark Configurations vs. Energy Consumption (with idle-clean)")
        plt.grid(axis="y")
        plt.tight_layout()
        plt.savefig("energy_comparison_with_idle_clean.png")
        print("✅ Chart saved as: energy_comparison_with_idle_clean.png")
    else:
        print("⚠️ No energy data found to plot.")

if __name__ == "__main__":
    main()
