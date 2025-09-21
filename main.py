#!/usr/bin/env python3
# main.py – uruchamia scrapery i agregator PO KOLEI (sekwencyjnie)

import subprocess
import sys
from pathlib import Path
from datetime import datetime
import time

# Kolejność uruchamiania (ostatni to agregator)
SCRIPTS = [
    "csfloat_prices.py",
    "steam_prices.py",
    "csroi_unbox_data_csfloat.py",
    "csroi_unbox_data_steam.py",
    "csgocasetracker_popularity.py",
    "steamcharts_playercounts.py",
    "final_data_output.py",
]

def run_script(script_path: Path) -> int:
    stamp = datetime.now().strftime("%H:%M:%S")
    print(f"\n[INFO {stamp}] Start: {script_path.name}")
    try:
        rc = subprocess.call([sys.executable, str(script_path)])
        if rc == 0:
            print(f"[OK] {script_path.name} zakończony")
        else:
            print(f"[ERROR] {script_path.name} -> rc={rc}")
        return rc
    except KeyboardInterrupt:
        print("\n[WARN] Przerwano przez użytkownika.")
        raise
    except Exception as e:
        print(f"[ERROR] Nie udało się uruchomić {script_path.name}: {e}")
        return 1

def main():
    base = Path(__file__).parent

    while True:
        results = {}
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n====== RUN START: {start_time} ======\n")

        for script in SCRIPTS:
            sp = base / script
            if not sp.exists():
                print(f"[WARN] Skrypt {script} nie istnieje w katalogu")
                results[script] = 127
                continue

            rc = run_script(sp)
            results[script] = rc

        # Podsumowanie
        ok = [s for s, rc in results.items() if rc == 0]
        failed = [s for s, rc in results.items() if rc != 0]

        print("\n=== PODSUMOWANIE ===")
        print(f"OK     : {len(ok)} → {', '.join(ok) if ok else '-'}")
        print(f"FAILED : {len(failed)} → {', '.join(failed) if failed else '-'}")

        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n====== RUN END: {end_time} ======")

        # Czekaj 15 minut
        print("\n[INFO] Oczekiwanie 15 minut przed kolejnym runem...\n")
        time.sleep(900)  # 900 sekund = 15 minut

if __name__ == "__main__":
    main()
