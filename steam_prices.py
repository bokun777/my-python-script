#!/usr/bin/env python3
# Steam Market — "Starting at ..." (fallback: priceoverview), 41 skrzynek
# pip install httpx

import re
import json
import time
import random
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from urllib.parse import quote

import httpx

LISTING_BASE = "https://steamcommunity.com/market/listings/730/"
PRICEOVERVIEW = "https://steamcommunity.com/market/priceoverview"

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTFILE = DATA_DIR / "steam_prices.ndjson"

CASES = [
    "Fever Case","Gallery Case","Kilowatt Case","Dreams & Nightmares Case","Fracture Case",
    "Recoil Case","Revolution Case","Glove Case","Chroma Case","Chroma 2 Case","Chroma 3 Case",
    "Clutch Case","CS:GO Weapon Case","CS:GO Weapon Case 2","CS:GO Weapon Case 3","CS20 Case",
    "Danger Zone Case","eSports 2013 Case","eSports 2013 Winter Case","eSports 2014 Summer Case",
    "Falchion Case","Gamma Case","Gamma 2 Case","Horizon Case","Huntsman Weapon Case",
    "Operation Bravo Case","Operation Breakout Weapon Case","Operation Broken Fang Case",
    "Operation Hydra Case","Operation Phoenix Weapon Case","Operation Riptide Case",
    "Operation Vanguard Weapon Case","Operation Wildfire Case","Prisma Case","Prisma 2 Case",
    "Revolver Case","Shadow Case","Shattered Web Case","Snakebite Case","Spectrum Case",
    "Spectrum 2 Case","Winter Offensive Weapon Case",
]

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def parse_money_any(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.replace("\xa0", " ").replace("$", "").strip()
    m = re.search(r"([0-9]+(?:[.,][0-9]{1,2})?)", s)
    if not m:
        return None
    num = m.group(1)
    if "," in num and "." not in num:
        num = num.replace(",", ".")
    try:
        return float(num)
    except:
        return None

def extract_price_from_html(html: str) -> Optional[float]:
    m = re.search(r"Starting at[^0-9]*([0-9]+(?:[.,][0-9]{1,2})?)", html, flags=re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", "."))
    m = re.search(r'class="market_listing_price(?:_with_fee)?[^"]*">([^<]+)<', html, flags=re.IGNORECASE)
    if m:
        return parse_money_any(m.group(1))
    m = re.search(r"lowest_price[^0-9]*([0-9]+(?:[.,][0-9]{1,2})?)", html, flags=re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", "."))
    return None

def fetch_starting_from_listing(client: httpx.Client, case_name: str, retries: int = 5) -> Optional[float]:
    url = LISTING_BASE + quote(case_name, safe="")
    params = {"l": "english", "cc": "US"}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CS2Bot/1.0"}
    delay = 0.5
    for _ in range(retries):
        try:
            r = client.get(url, params=params, headers=headers, timeout=15)
            if r.status_code in (429,) or 500 <= r.status_code < 600:
                time.sleep(delay); delay *= 1.5; continue
            r.raise_for_status()
            return extract_price_from_html(r.text)
        except httpx.HTTPError:
            time.sleep(delay); delay *= 1.5
    return None

def fetch_priceoverview_fallback(client: httpx.Client, case_name: str, retries: int = 5) -> Optional[float]:
    params = {"appid": 730, "currency": 1, "market_hash_name": case_name}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CS2Bot/1.0"}
    delay = 0.5
    for _ in range(retries):
        try:
            r = client.get(PRICEOVERVIEW, params=params, headers=headers, timeout=15)
            if r.status_code in (429,) or 500 <= r.status_code < 600:
                time.sleep(delay); delay *= 1.5; continue
            r.raise_for_status()
            data = r.json()
            if data and data.get("success"):
                return parse_money_any(data.get("lowest_price") or "") or parse_money_any(data.get("median_price") or "")
        except httpx.HTTPError:
            time.sleep(delay); delay *= 1.5
    return None

def fetch_case_price(client: httpx.Client, case_name: str) -> Optional[Dict[str, Any]]:
    p = fetch_starting_from_listing(client, case_name)
    source = "Steam-StartingAt"
    if p is None:
        p = fetch_priceoverview_fallback(client, case_name)
        if p is not None:
            source = "Steam-Priceoverview"
    if p is None:
        return None
    return {"case": case_name, "price": round(p, 2), "timestamp": now_iso(), "source": source}

def run(outpath: str, per_request_sleep: float = 0.6, second_pass_sleep: float = 6.0) -> int:
    out = Path(outpath)
    out.parent.mkdir(parents=True, exist_ok=True)

    ok, failed = [], []
    total = len(CASES)
    print(f"[steam_prices] zapis do: {out}")

    with httpx.Client(timeout=15) as client:
        # Runda 1
        for idx, name in enumerate(CASES, start=1):
            print(f"[INFO] ({idx}/{total}) Pobieram: {name}")
            rec = fetch_case_price(client, name)
            if rec:
                print(f"[OK-1] {name}: {rec.get('price')}")
                ok.append(rec)
            else:
                print(f"[MISS-1] {name}")
                failed.append(name)
            time.sleep(per_request_sleep + random.uniform(0, 0.2))

        # Runda 2
        if failed:
            print(f"\n⏳ Druga runda dla {len(failed)} case’ów…\n")
            time.sleep(second_pass_sleep)
            still_failed = []
            for idx, name in enumerate(failed, start=1):
                print(f"[INFO] (2nd pass {idx}/{len(failed)}) Pobieram: {name}")
                rec = fetch_case_price(client, name)
                if rec:
                    print(f"[OK-2] {name}: {rec.get('price')}")
                    ok.append(rec)
                else:
                    print(f"[MISS-2] {name}")
                    still_failed.append(name)
                time.sleep(per_request_sleep + random.uniform(0, 0.2))

            if still_failed:
                print(f"\n⚠️ Po dwóch rundach nadal brak {len(still_failed)}: {', '.join(still_failed)}")
            else:
                print("\n✅ Druga runda domknęła wszystkie brakujące.")

    wrote = 0
    with out.open("a", encoding="utf-8") as f:
        for rec in ok:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            wrote += 1
    print(f"\nZapisano {wrote}/{len(CASES)} rekordów do {out}")
    return wrote

if __name__ == "__main__":
    run(outpath=str(OUTFILE), per_request_sleep=0.6, second_pass_sleep=6.0)
