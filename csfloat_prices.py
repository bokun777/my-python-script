#!/usr/bin/env python3
# csfloat_prices_all_cases.py — CSFloat direct (avg of 10 lowest BIN listings), NDJSON
# Wymaga: pip install httpx python-dotenv
# W .env: CSFLOAT_API_KEY=TWÓJ_KLUCZ

import os
import json
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from dotenv import load_dotenv

API_URL = "https://csfloat.com/api/v1/listings"

CASES = [
    "Fever Case",
    "Gallery Case",
    "Kilowatt Case",
    "Dreams & Nightmares Case",
    "Fracture Case",
    "Recoil Case",
    "Revolution Case",
    "Glove Case",
    "Chroma Case",
    "Chroma 2 Case",
    "Chroma 3 Case",
    "Clutch Case",
    "CS:GO Weapon Case",
    "CS:GO Weapon Case 2",
    "CS:GO Weapon Case 3",
    "CS20 Case",
    "Danger Zone Case",
    "eSports 2013 Case",
    "eSports 2013 Winter Case",
    "eSports 2014 Summer Case",
    "Falchion Case",
    "Gamma Case",
    "Gamma 2 Case",
    "Horizon Case",
    "Huntsman Weapon Case",
    "Operation Bravo Case",
    "Operation Breakout Weapon Case",
    "Operation Broken Fang Case",
    "Operation Hydra Case",
    "Operation Phoenix Weapon Case",
    "Operation Riptide Case",
    "Operation Vanguard Weapon Case",
    "Operation Wildfire Case",
    "Prisma Case",
    "Prisma 2 Case",
    "Revolver Case",
    "Shadow Case",
    "Shattered Web Case",
    "Snakebite Case",
    "Spectrum Case",
    "Spectrum 2 Case",
    "Winter Offensive Weapon Case",
]

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def parse_listings(j: Any) -> List[Dict[str, Any]]:
    if isinstance(j, list):
        return j
    if isinstance(j, dict):
        if isinstance(j.get("data"), list):
            return j["data"]
        if isinstance(j.get("listings"), list):
            return j["listings"]
    return []

def auth_headers(api_key: str, bearer: bool) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}" if bearer else api_key,
        "User-Agent": "Mozilla/5.0 (compatible; CS2Bot/1.0)",
        "Accept": "application/json",
    }

async def fetch_avg_price(
    client: httpx.AsyncClient,
    case_name: str,
    api_key: str,
    retries: int = 3,
    timeout: float = 20.0,
) -> Optional[Dict[str, Any]]:
    """
    10 najtańszych, bez aukcji -> średnia/min/max.
    Zwraca rekord NDJSON albo None.
    """
    params = {
        "market_hash_name": case_name,
        "sort_by": "lowest_price",
        "limit": 10,
        "auction": "false",   # ignoruj aukcje
    }

    for bearer_mode in (True, False):  # spróbuj z Bearer i bez Bearer
        delay = 0.8
        for attempt in range(1, retries + 1):
            try:
                r = await client.get(
                    API_URL,
                    params=params,
                    headers=auth_headers(api_key, bearer_mode),
                    timeout=timeout,
                )
                if r.status_code in (401, 403):
                    if bearer_mode:
                        break  # spróbuj kolejnego stylu autoryzacji
                    else:
                        print(f"[ERROR] {case_name} -> {r.status_code} {r.text[:200]}")
                        return None
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue

                r.raise_for_status()
                data = r.json()
                listings = parse_listings(data)
                prices_cents = [it.get("price") for it in listings if isinstance(it.get("price"), (int, float))]
                if not prices_cents:
                    print(f"[WARN] Brak ofert BIN dla: {case_name}")
                    return None

                prices = [p / 100.0 for p in prices_cents]
                avg_price = round(sum(prices) / len(prices), 2)
                min_price = round(min(prices), 2)
                max_price = round(max(prices), 2)

                return {
                    "item": case_name,
                    "price": avg_price,         # średnia z 10
                    "timestamp": now_iso(),
                }
            except httpx.HTTPError as e:
                if attempt < retries:
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                print(f"[ERROR] {case_name} -> {e}")
                return None
            except Exception as e:
                print(f"[ERROR] {case_name} -> {e}")
                return None
        # przejdź do trybu bez Bearer jeśli pierwszy tryb zawiódł

    return None

async def run_async(outpath: str, api_key: str, concurrency: int = 16) -> int:
    out = Path(outpath)
    out.parent.mkdir(parents=True, exist_ok=True)

    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    timeout = httpx.Timeout(20.0)
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        sem = asyncio.Semaphore(concurrency)

        async def worker(name: str):
            async with sem:
                return await fetch_avg_price(client, name, api_key)

        results = await asyncio.gather(*[worker(c) for c in CASES])

    wrote = 0
    with out.open("a", encoding="utf-8") as f:
        for rec in results:
            if isinstance(rec, dict):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                wrote += 1
    return wrote

def main():
    load_dotenv()
    api_key = os.getenv("CSFLOAT_API_KEY")
    if not api_key:
        raise SystemExit("❌ Brak CSFLOAT_API_KEY w .env")

    outpath = "data/csfloat_prices.ndjson"
    wrote = asyncio.run(run_async(outpath, api_key, concurrency=16))
    print(f"Zapisano {wrote}/{len(CASES)} rekordów do {outpath}")

if __name__ == "__main__":
    main()
