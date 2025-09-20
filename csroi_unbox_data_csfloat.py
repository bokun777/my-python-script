#!/usr/bin/env python3
# csroi_unbox_data_csfloat.py
# Scraper Average Unbox (USD) i Unbox ROI (%) z CSROI (pricing source: CSFloat – ustaw ręcznie przy 1. runie)
# - persistent profile: playwright_profile_csroi_csfloat
# - zapis: data/csroi_unbox_data_csfloat.ndjson
# - domyślnie: wszystkie skrzynki; --limit N ogranicza do N pierwszych

import re
import json
import argparse
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

PROFILE_DIR = Path("playwright_profile_csroi_csfloat")
OUTFILE = Path("data/csroi_unbox_data_csfloat.ndjson")

# --- Domyślne, poprawne slug-i (w tym CS:GO Weapon Case 1/2/3) ---
CASES = [
    "fever-case",
    "gallery-case",
    "kilowatt-case",
    "dreams-nightmares-case",
    "fracture-case",
    "recoil-case",
    "revolution-case",
    "glove-case",
    "chroma-case",
    "chroma-2-case",
    "chroma-3-case",
    "clutch-case",
    "cs-go-weapon-case",       # ✅ poprawny slug
    "cs-go-weapon-case-2",     # ✅ poprawny slug
    "cs-go-weapon-case-3",     # ✅ poprawny slug
    "cs20-case",
    "danger-zone-case",
    "esports-2013-case",
    "esports-2013-winter-case",
    "esports-2014-summer-case",
    "falchion-case",
    "gamma-case",
    "gamma-2-case",
    "horizon-case",
    "huntsman-weapon-case",
    "operation-bravo-case",
    "operation-breakout-weapon-case",
    "operation-broken-fang-case",
    "operation-hydra-case",
    "operation-phoenix-weapon-case",
    "operation-riptide-case",
    "operation-vanguard-weapon-case",
    "operation-wildfire-case",
    "prisma-case",
    "prisma-2-case",
    "revolver-case",
    "shadow-case",
    "shattered-web-case",
    "snakebite-case",
    "spectrum-case",
    "spectrum-2-case",
    "winter-offensive-weapon-case",
]

# --- Ciche mapowanie starych slugów -> kanoniczne (gdyby kiedyś się trafiły) ---
CANONICAL_SLUG: Dict[str, str] = {
    "csgo-weapon-case": "cs-go-weapon-case",
    "csgo-weapon-case-2": "cs-go-weapon-case-2",
    "csgo-weapon-case-3": "cs-go-weapon-case-3",
}

# --- Ładne nazwy do zapisów/printów (overrides) ---
DISPLAY_NAME: Dict[str, str] = {
    "dreams-nightmares-case": "Dreams & Nightmares Case",
    "cs-go-weapon-case": "CS:GO Weapon Case",
    "cs-go-weapon-case-2": "CS:GO Weapon Case 2",
    "cs-go-weapon-case-3": "CS:GO Weapon Case 3",
    "cs20-case": "CS20 Case",
    "esports-2013-case": "eSports 2013 Case",
    "esports-2013-winter-case": "eSports 2013 Winter Case",
    "esports-2014-summer-case": "eSports 2014 Summer Case",
}

USD_RE = re.compile(r"([0-9]+(?:[.,]\d{1,2})?)\s*USD", re.IGNORECASE)
PCT_RE = re.compile(r"([\-+]?[0-9]+(?:[.,]\d{1,2})?)\s*%")

def to_float_num(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.replace("\xa0", " ").strip().replace(",", ".")
    try:
        return float(s)
    except:
        return None

def pretty_name_from_slug(slug: str) -> str:
    if slug in DISPLAY_NAME:
        return DISPLAY_NAME[slug]
    base = slug.replace("-", " ").strip()
    return " ".join(w.capitalize() for w in base.split())

def find_avg_unbox_by_dom(page) -> Optional[float]:
    try:
        el = page.locator("text=Average Unbox").first
        if el.count() > 0:
            node = el.locator("xpath=..")
            txt = (node.inner_text() or "")
            m = USD_RE.search(txt)
            if not m:
                node2 = node.locator("xpath=..")
                txt2 = (node2.inner_text() or "")
                m = USD_RE.search(txt2)
            if m:
                return to_float_num(m.group(1))
    except:
        pass
    return None

def find_roi_by_dom(page) -> Optional[float]:
    try:
        el = page.locator("text=Unbox ROI").first
        if el.count() == 0:
            el = page.locator("text=Unboxing ROI").first
        if el.count() > 0:
            node = el.locator("xpath=..")
            txt = (node.inner_text() or "")
            m = PCT_RE.search(txt)
            if not m:
                node2 = node.locator("xpath=..")
                txt2 = (node2.inner_text() or "")
                m = PCT_RE.search(txt2)
            if m:
                return to_float_num(m.group(1))
    except:
        pass
    return None

def find_avg_unbox_by_text(body_text: str) -> Optional[float]:
    body_text = body_text.replace("\xa0", " ")
    mctx = re.search(r"Average\s+Unbox(.{0,160})", body_text, flags=re.IGNORECASE|re.DOTALL)
    if mctx:
        m = USD_RE.search(mctx.group(1))
        if m:
            return to_float_num(m.group(1))
    m = USD_RE.search(body_text)
    if m:
        return to_float_num(m.group(1))
    return None

def find_roi_by_text(body_text: str) -> Optional[float]:
    body_text = body_text.replace("\xa0", " ")
    mctx = re.search(r"Unbox(?:ing)?\s+ROI(.{0,160})", body_text, flags=re.IGNORECASE|re.DOTALL)
    if mctx:
        m = PCT_RE.search(mctx.group(1))
        if m:
            return to_float_num(m.group(1))
    m = PCT_RE.search(body_text)
    if m:
        return to_float_num(m.group(1))
    return None

def scrape_case(page, slug: str, wait_ms: int, item_name: str):
    url = f"https://csroi.com/item/{slug}"
    # (Brak dodatkowego [INFO] tutaj — log robimy w main)
    page.goto(url, wait_until="networkidle", timeout=60000)
    page.wait_for_timeout(wait_ms)

    avg_unbox = find_avg_unbox_by_dom(page)
    roi = find_roi_by_dom(page)

    if avg_unbox is None or roi is None:
        body_text = page.locator("body").inner_text()
        if avg_unbox is None:
            avg_unbox = find_avg_unbox_by_text(body_text)
        if roi is None:
            roi = find_roi_by_text(body_text)

    return {
        "item": item_name,
        "average_unbox_usd": avg_unbox,
        "unbox_roi_pct": roi,
        "timestamp": datetime.now(timezone.utc).isoformat()  # timezone-aware
    }

def main():
    parser = argparse.ArgumentParser(description="CSROI CSFloat – Average Unbox + Unbox ROI scraper (cookies profile)")
    parser.add_argument("--visible", action="store_true", help="Otwórz okno i ręcznie ustaw Pricing source: CSFloat (pierwszy run)")
    parser.add_argument("--wait", type=int, default=3000, help="Czekanie po wejściu na stronę (ms)")
    parser.add_argument("--limit", type=int, default=0, help="Ile skrzynek przetworzyć (0/brak = wszystkie)")
    args = parser.parse_args()

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    OUTFILE.parent.mkdir(parents=True, exist_ok=True)

    base_list = CASES[:args.limit] if args.limit and args.limit > 0 else CASES
    # ciche sprowadzenie do kanonicznych slugów (gdyby coś przyszło w starej formie)
    cases_to_run = [CANONICAL_SLUG.get(s, s) for s in base_list]

    headless = not args.visible
    total_all = len(cases_to_run)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=headless,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        page = context.new_page()
        page.set_default_timeout(60000)

        # Pierwszy run: ustaw Pricing source: CSFloat i zaakceptuj cookies (jeśli trzeba)
        if args.visible:
            page.goto("https://csroi.com/item/horizon-case", wait_until="domcontentloaded", timeout=60000)
            print("➡️ Ustaw na stronie Pricing source: CSFloat (na górze strony / w menu).")
            input("Gdy ustawisz CSFloat i zaakceptujesz cookies, wciśnij ENTER tutaj...")

        total_ok = 0
        failed = []

        # --- Runda 1 ---
        with OUTFILE.open("a", encoding="utf-8") as f:
            for i, slug in enumerate(cases_to_run, 1):
                item_name = pretty_name_from_slug(slug)
                try:
                    print(f"[INFO] ({i}/{total_all}) Pobieram: {item_name}")
                    rec = scrape_case(page, slug, args.wait, item_name)
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    print(f"[OK-1] {item_name} → avg={rec['average_unbox_usd']} USD, roi={rec['unbox_roi_pct']}%")
                    total_ok += 1
                except (PWTimeout, Exception) as e:
                    print(f"[MISS-1] {item_name} -> {e}")
                    failed.append(slug)

        # --- Runda 2 (tylko nieudane) ---
        if failed:
            print(f"\n⏳ Druga runda dla {len(failed)} case’ów…\n")
            time.sleep(5)  # mała pauza pomaga, jeśli serwis się dławi
            still_failed = []
            with OUTFILE.open("a", encoding="utf-8") as f:
                for j, slug in enumerate(failed, 1):
                    item_name = pretty_name_from_slug(slug)
                    try:
                        print(f"[INFO] (2nd pass {j}/{len(failed)}) Pobieram: {item_name}")
                        rec = scrape_case(page, slug, args.wait, item_name)
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        print(f"[OK-2] {item_name} → avg={rec['average_unbox_usd']} USD, roi={rec['unbox_roi_pct']}%")
                        total_ok += 1
                    except (PWTimeout, Exception) as e:
                        print(f"[MISS-2] {item_name} -> {e}")
                        still_failed.append(slug)

            if still_failed:
                nice = ", ".join(pretty_name_from_slug(s) for s in still_failed)
                print(f"\n⚠️  Po dwóch rundach nadal brak {len(still_failed)}: {nice}")
            else:
                print("\n✅ Druga runda domknęła wszystkie brakujące.")

        context.close()
        print(f"\n✅ Zakończono. Zapisano {total_ok} rekordów → {OUTFILE}")

if __name__ == "__main__":
    main()
