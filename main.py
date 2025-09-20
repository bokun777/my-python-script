import subprocess
import sys
import os

def install_requirements():
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
            check=True
        )
    except subprocess.CalledProcessError:
        print("❌ Błąd podczas instalacji bibliotek. Sprawdź requirements.txt")
        sys.exit(1)

def run_script(script_name):
    print(f"▶️ Uruchamiam: {script_name}")
    try:
        subprocess.run([sys.executable, script_name], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Błąd w skrypcie {script_name}: {e}")

if __name__ == "__main__":
    install_requirements()

    scripts = [
        "csfloat_prices.py",
        "steam_prices.py",
        "csgocasetracker_popularity.py",
        "csroi_unbox_data_csfloat.py",
        "csroi_unbox_data_steam.py",
        "steamcharts_playercounts.py",
        "final_data_output.py"
    ]

    for script in scripts:
        run_script(script)
