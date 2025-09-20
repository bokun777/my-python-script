#!/usr/bin/env python3
# csgocasetracker_popularity.py
# Scraper popularności skrzynek z csgocasetracker.com
# Zbiera dane: opened_last_day, opened_last_week, opened_last_month + zmiany %
# Dodatkowo: daily_sales, market_listings

import re
import json
import argparse
import time
import random
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

# Pełna lista skrzynek
CASES = [
    "Fever-Case",
    "Gallery-Case",
    "Kilowatt-Case",
    "Dreams-&-Nightmares-Case",
    "Fracture-Case",
    "Recoil-Case",
    "Revolution-Case",
    "Glove-Case",
    "Chroma-Case",
    "Chroma-2-Case",
    "Chroma-3-Case",
    "Clutch-Case",
    "CSGO-Weapon-Case",
    "CSGO-Weapon-Case-2",
    "CSGO-Weapon-Case-3",
    "CS20-Case",
    "Danger-Zone-Case",
    "eSports-2013-Case",
    "eSports-2013-Winter-Case",
    "eSports-2014-Summer-Case",
    "Falchion-Case",
    "Gamma-Case",
    "Gamma-2-Case",
    "Horizon-Case",
    "Huntsman-Weapon-Case",
    "Operation-Bravo-Case",
    "Operation-Breakout-Weapon-Case",
    "Operation-Broken-Fang-Case",
    "Operation-Hydra-Case",
    "Operation-Phoenix-Weapon-Case",
    "Operation-Riptide-Case",
    "Operation-Vanguard-Weapon-Case",
    "Operation-Wildfire-Case",
    "Prisma-Case",
    "Prisma-2-Case",
    "Revolver-Case",
    "Shadow-Case",
    "Shattered-Web-Case",
    "Snakebite-Case",
    "Spectrum-Case",
    "Spectrum-2-Case",
    "Winter-Offensive-Weapon-Case"
]

OUTFILE = Path("data/csgocasetracker_popularity.ndjson")
PROFILE_DIR = Path("playwright_profile_csgocasetracker")
WAIT_MS = 4000  # czas na załadowanie strony

# --- Parametry retry/second-pass (dodane) ---
RETRIES = 5
BASE_DELAY = 1.0
BACKOFF = 1.6
MAX_DELAY = 12.0
JITTER = 0.3
SECOND_PASS_PAUSE = 3.0


def parse_case_data(raw_text):
    """Wyciąga dane liczbowe i procentowe z surowego tekstu strony"""
    def extract_int(pattern):
        match = re.search(pattern, raw_text)
        if match:
            return int(match.group(1).replace(" ", "").replace(",", ""))
        return None

    def extract_float(pattern):
        match = re.search(pattern, raw_text)
        if match:
            return float(match.group(1))
        return None

    raw_text = raw_text.replace("\xa0", " ")

    return {
        "opened_last_day": extract_int(r"Opened last day:\s*([\d ]+)"),
        "day_change": extract_float(r"1D:\s*([\-0-9.]+)"),
        "opened_last_week": extract_int(r"Opened last week:\s*([\d ]+)"),
        "week_change": extract_float(r"1W:\s*([\-0-9.]+)"),
        "opened_last_month": extract_int(r"Opened last month:\s*([\d ]+)"),
        "month_change": extract_float(r"1M:\s*([\-0-9.]+)"),
        "daily_sales": extract_int(r"Daily sales:\s*([\d ]+)"),
        "market_listings": extract_int(r"Market listings:\s*([\d ]+)")
    }


def _with_retries(fn):
    """Minimalny wrapper retry z backoffem + jitter."""
    delay = BASE_DELAY
    last_exc = None
    for attempt in range(1, RETRIES + 1):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            if attempt == RETRIES:
                break
            time.sleep(min(delay, MAX_DELAY) + random.uniform(0, JITTER))
            delay *= BACKOFF
    raise last_exc


def scrape_popularity(limit=None):
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    OUTFILE.parent.mkdir(parents=True, exist_ok=True)

    cases_to_scrape = CASES if limit is None else CASES[:limit]
    total = len(cases_to_scrape)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=True,
            args=["--no-sandbox"]
        )
        page = context.new_page()

        # --- Runda 1 ---
        still_failed = []
        with open(OUTFILE, "a", encoding="utf-8") as f:
            for idx, case in enumerate(cases_to_scrape, start=1):
                url = f"https://csgocasetracker.com/history/cases/{case}"
                print(f"[INFO] ({idx}/{total}) Pobieram: {case}")
                try:
                    def _task():
                        page.goto(url, wait_until='domcontentloaded', timeout=60000)
                        page.wait_for_timeout(WAIT_MS)
                        raw_text = page.locator("body").inner_text()
                        parsed = parse_case_data(raw_text)
                        record = {
                            "case": case.replace("-", " "),
                            "url": url,
                            "timestamp": datetime.utcnow().isoformat(),
                            **parsed
                        }
                        f.write(json.dumps(record, ensure_ascii=False) + "\n")
                        print(f"[OK] {case} -> {parsed}")
                        return True
                    _with_retries(_task)
                except Exception as e:
                    print(f"[ERROR] {case} -> {e}")
                    still_failed.append(case)

        # --- Runda 2 (tylko nieudane) ---
        if still_failed:
            print(f"\n⏳ Druga runda dla {len(still_failed)} case’ów…\n")
            time.sleep(SECOND_PASS_PAUSE)
            remaining = []

            with open(OUTFILE, "a", encoding="utf-8") as f:
                for idx, case in enumerate(still_failed, start=1):
                    url = f"https://csgocasetracker.com/history/cases/{case}"
                    print(f"[INFO] (2nd pass {idx}/{len(still_failed)}) Pobieram: {case}")
                    try:
                        def _task2():
                            page.goto(url, wait_until='domcontentloaded', timeout=60000)
                            page.wait_for_timeout(WAIT_MS)
                            raw_text = page.locator("body").inner_text()
                            parsed = parse_case_data(raw_text)
                            record = {
                                "case": case.replace("-", " "),
                                "url": url,
                                "timestamp": datetime.utcnow().isoformat(),
                                **parsed
                            }
                            f.write(json.dumps(record, ensure_ascii=False) + "\n")
                            print(f"[OK] {case} -> {parsed}")
                            return True
                        _with_retries(_task2)
                    except Exception as e:
                        print(f"[ERROR] {case} -> {e}")
                        remaining.append(case)

            if remaining:
                print(f"\n⚠️  Po dwóch rundach nadal brak: {len(remaining)} → {', '.join(remaining)}")
            else:
                print("\n✅ Druga runda domknęła wszystkie brakujące.")

        context.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper popularności skrzynek z csgocasetracker.com")
    parser.add_argument("--limit", type=int, help="Ilość skrzynek do przetestowania")
    args = parser.parse_args()

    scrape_popularity(limit=args.limit)
