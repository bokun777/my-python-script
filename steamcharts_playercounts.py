#!/usr/bin/env python3
# steamcharts_playercounts.py
# Poprawione: nie miesza etykiet, używa sekwencyjnego wzorca:
#  playing -> 24-hour peak -> all-time peak
# + zapisuje gain i gain % z "Last 30 Days"
import re
import csv
import json
import time
from pathlib import Path
from datetime import datetime, timezone
import requests

URL = "https://steamcharts.com/app/730"

OUT_CURRENT = Path("data/player_count.ndjson")
OUT_HIST_CSV = Path("data/player_count_history.csv")
OUT_HIST_NDJSON = Path("data/player_count_history.ndjson")

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()

def to_int_num(s: str) -> int:
    return int(s.replace(",", "").replace(" ", ""))

def to_float_num(s: str) -> float:
    s = s.replace(",", "")
    parts = s.split(".")
    if len(parts) > 2:
        s = parts[0] + "." + "".join(parts[1:])
    return float(s)

def fetch_body_text() -> str:
    r = requests.get(
        URL,
        headers={"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/120.0.0.0 Safari/537.36")},
        timeout=30
    )
    r.raise_for_status()
    html = r.text
    body = re.search(r"<body[^>]*>(.*?)</body>", html, flags=re.I | re.S)
    src = body.group(1) if body else html
    src = re.sub(r"<script[^>]*>.*?</script>", " ", src, flags=re.I | re.S)
    src = re.sub(r"<style[^>]*>.*?</style>", " ", src, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", src)
    text = (text.replace("&#43;", "+")
                .replace("&nbsp;", " ")
                .replace("–", "-")
                .replace("—", "-"))
    return clean_spaces(text)

def parse_current_metrics(text: str):
    """
    Zwraca dict: {playing, peak_24h, peak_all_time}
    Najpierw próbuje sekwencyjnie (LABEL -> liczba), potem (liczba -> LABEL).
    """
    # Wariant A: label przed liczbą, w poprawnej kolejności
    pat_a = re.compile(
        r"playing\s+([\d,]+).*?24-hour peak\s+([\d,]+).*?all-time peak\s+([\d,]+)",
        re.IGNORECASE | re.DOTALL
    )
    m = pat_a.search(text)
    if m:
        return {
            "playing": to_int_num(m.group(1)),
            "peak_24h": to_int_num(m.group(2)),
            "peak_all_time": to_int_num(m.group(3)),
        }

    # Wariant B: liczba przed label, w tej samej kolejności logicznej
    pat_b = re.compile(
        r"([\d,]+)\s+playing.*?([\d,]+)\s+24-hour peak.*?([\d,]+)\s+all-time peak",
        re.IGNORECASE | re.DOTALL
    )
    m2 = pat_b.search(text)
    if m2:
        return {
            "playing": to_int_num(m2.group(1)),
            "peak_24h": to_int_num(m2.group(2)),
            "peak_all_time": to_int_num(m2.group(3)),
        }

    snippet = text[:200]
    raise RuntimeError("Nie udało się sparsować bieżących metryk. Fragment: " + snippet)

MONTHS = ("January","February","March","April","May","June","July","August",
          "September","October","November","December")

def parse_history_rows(text: str):
    """
    Zwraca listę dictów: {Month, Avg Players, Gain, % Gain, Peak Players}
    Obejmuje również "Last 30 Days".
    """
    rows = []
    header_idx = text.lower().find("month avg. players gain % gain peak players")
    scope = text[header_idx:] if header_idx != -1 else text

    month_names = "|".join(MONTHS)
    pat = re.compile(
        rf"(Last 30 Days|(?:{month_names})\s+\d{{4}})\s+"
        r"([+\-]?[0-9][0-9,\.]*)\s+"       # Avg Players
        r"([+\-]?[0-9][0-9,\.]*|-)\\?\s+"   # Gain (może być '-')
        r"([+\-]?[0-9][0-9,\.]*%)\\?\s+"    # % Gain
        r"([0-9][0-9,]*)",                  # Peak Players
        re.IGNORECASE
    )

    for m in pat.finditer(scope):
        month_label = m.group(1).strip()
        avg_players_s = m.group(2).replace(" ", "")
        gain_s        = m.group(3).replace(" ", "")
        pct_s         = m.group(4).replace(" ", "")
        peak_s        = m.group(5).replace(" ", "")

        try:
            avg_players = to_float_num(avg_players_s.replace(",", ""))
        except:
            continue

        gain = None if gain_s == "-" else to_float_num(gain_s.replace(",", "")) if gain_s else None
        pct  = None
        try:
            pct = to_float_num(pct_s.replace("%",""))
        except:
            pct = None

        try:
            peak_players = to_int_num(peak_s)
        except:
            peak_players = None

        rows.append({
            "Month": month_label,
            "Avg Players": round(avg_players, 2) if avg_players is not None else None,
            "Gain": None if gain is None else round(gain, 2),
            "% Gain": None if pct is None else round(pct, 2),
            "Peak Players": peak_players
        })
    return rows

def save_history_csv(rows):
    OUT_HIST_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_HIST_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Month","Avg Players","Gain","% Gain","Peak Players"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

def save_history_ndjson(rows):
    OUT_HIST_NDJSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_HIST_NDJSON.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# ---- DWIE PRÓBY: cała logika w run_once(), a main() podejmuje do 2 podejść ----
def run_once():
    text = fetch_body_text()

    # 1) Bieżące metryki (sekwencyjne dopasowanie)
    cur = parse_current_metrics(text)
    # 2) Historia (potrzebna do Last 30 Days gain/%)
    hist_rows = parse_history_rows(text)
    last30 = next((r for r in hist_rows if r["Month"].lower().startswith("last 30 days")), None)
    if not last30:
        raise RuntimeError("Nie znaleziono wiersza 'Last 30 Days' w tabeli historii.")

    gain_last = last30.get("Gain")
    pct_last  = last30.get("% Gain")

    # 3) Zapis bieżących metryk do NDJSON
    OUT_CURRENT.parent.mkdir(parents=True, exist_ok=True)
    ts = now_iso()
    records = [
        {"item": "playing",       "timestamp": ts, "value": cur["playing"],        "source": URL},
        {"item": "24-hour peak",  "timestamp": ts, "value": cur["peak_24h"],       "source": URL},
        {"item": "all-time peak", "timestamp": ts, "value": cur["peak_all_time"],  "source": URL},
        {"item": "gain",          "timestamp": ts, "value": gain_last,             "source": URL},
        {"item": "gain %",        "timestamp": ts, "value": pct_last,              "source": URL},
    ]
    with OUT_CURRENT.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"playing        : {cur['playing']:,}")
    print(f"24-hour peak   : {cur['peak_24h']:,}")
    print(f"all-time peak  : {cur['peak_all_time']:,}")
    print(f"gain (30d)     : {gain_last:+,.2f}" if gain_last is not None else "gain (30d): -")
    print(f"gain % (30d)   : {pct_last:+.2f}%"  if pct_last  is not None else "gain % (30d): -")
    print(f"→ zapisano 5 rekordów do {OUT_CURRENT}")

    # 4) Zapis pełnej historii (CSV + NDJSON)
    save_history_csv(hist_rows)
    save_history_ndjson(hist_rows)
    print(f"→ zapisano historię do {OUT_HIST_CSV} i {OUT_HIST_NDJSON}")

def main():
    last_exc = None
    for attempt in (1, 2):
        try:
            run_once()
            return
        except Exception as e:
            last_exc = e
            if attempt == 1:
                time.sleep(4)  # krótka pauza przed drugą próbą
            else:
                raise last_exc

if __name__ == "__main__":
    main()
