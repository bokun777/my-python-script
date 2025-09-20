#!/usr/bin/env python3
# final_data_output.py – NDJSON z nagłówkiem script_start, bez timestamp w rekordach

from pathlib import Path
from datetime import datetime, timezone
import json
from typing import Any, Optional

from metrics_db import (
    open_db, upsert_series, add_tick,
    latest_value, value_at_or_before, peak_since,
    pct_change, window_epochs
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

FINAL_BASENAME = "final_data_output"
OUT_LATEST = DATA_DIR / f"{FINAL_BASENAME}_latest.ndjson"
def OUT_SNAP(ts: str) -> Path:
    return DATA_DIR / f"{FINAL_BASENAME}_{ts}.ndjson"

ITEM_KEYS  = ["item_id", "item", "case", "name", "market_hash_name", "hash_name", "skin"]
TIME_KEYS  = ["timestamp", "scraped_at", "ts", "time", "date"]

# METRYKI, dla których wycinamy 24h TYLKO jeśli pochodzą z csgocasetracker_popularity
DROP_24H_METRICS = {
    "daily_sales",
    "market_listings",
    "opened_last_month",
    "opened_last_week",
}

def to_epoch(v: Any) -> Optional[int]:
    if v is None: return None
    if isinstance(v, (int, float)):
        return int(v / 1000) if v > 10_000_000_000 else int(v)
    if isinstance(v, str):
        s = v.strip().replace("Z", "+00:00")
        try:
            return int(datetime.fromisoformat(s).timestamp())
        except Exception:
            return None
    return None

def to_float(v: Any) -> Optional[float]:
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    if isinstance(v, str):
        s = v.replace("%", "").replace("$", "").replace(",", "").replace(" ", "")
        try: return float(s)
        except Exception: return None
    return None

def pick_first(rec: dict, keys: list[str]):
    for k in rec:
        if k in keys and rec[k] not in (None, ""):
            return rec[k]
    return None

def metric_present(rec: dict, candidates: list[str]):
    for k in candidates:
        if k in rec and rec[k] not in (None, ""):
            val = to_float(rec[k])
            if val is not None:
                return k, val
    return None

def pct_from_peak(curr: Optional[float], peak: Optional[float]) -> Optional[float]:
    if curr is None or peak is None or peak == 0: return None
    return (peak - curr) / peak * 100.0

def main():
    cn = open_db()

    script_start_dt = datetime.now(timezone.utc)
    ts_start_iso = script_start_dt.isoformat()
    win = window_epochs(script_start_dt)
    ts_tag = script_start_dt.strftime("%Y%m%d_%H%M%S")

    records_buffer = []

    # emit: pola 24h są opcjonalne w zależności od include_24h
    def emit(item, metric,
             change_24h_pct, change_7d_pct, change_30d_pct,
             pct_from_peak_24h, pct_from_peak_7d, pct_from_peak_30d,
             include_24h: bool):
        line = {
            "item": item,
            "metric": metric,
            "change_7d_pct": change_7d_pct,
            "change_30d_pct": change_30d_pct,
            "pct_from_peak_7d": pct_from_peak_7d,
            "pct_from_peak_30d": pct_from_peak_30d
        }
        if include_24h:
            line["change_24h_pct"] = change_24h_pct
            line["pct_from_peak_24h"] = pct_from_peak_24h
        records_buffer.append(line)

    for f in sorted(DATA_DIR.glob("*.ndjson")):
        name = f.name
        if name.startswith(FINAL_BASENAME) or name.endswith("_latest.ndjson"):
            continue

        source = Path(name).stem  # np. csfloat_prices, steam_prices, csgocasetracker_popularity, ...
        with f.open("r", encoding="utf-8") as fh:
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    rec = json.loads(raw)
                except Exception:
                    continue

                item = pick_first(rec, ITEM_KEYS) or ("CS2" if "playing" in rec else None)
                if not item:
                    continue
                ts = to_epoch(pick_first(rec, TIME_KEYS)) or int(script_start_dt.timestamp())

                def map_metric_name(metric_name: str, src: str) -> str:
                    # price -> wpisujemy źródło do nazwy metryki
                    if metric_name == "price":
                        if src == "csfloat_prices":
                            return "csfloat_price"
                        if src == "steam_prices":
                            return "steam_price"
                        return f"{src}_price"
                    if metric_name == "average_unbox_usd":
                        return "avg_unbox_usd"
                    return metric_name

                def handle(metric_name: str, value_any):
                    v = to_float(value_any)
                    if v is None:
                        return

                    key = f"{metric_name}:{source}:{item}"
                    upsert_series(cn, key, metric_name, source, item)
                    add_tick(cn, key, ts, v)

                    curr     = latest_value(cn, key)
                    base_h24 = value_at_or_before(cn, key, win["h24"])
                    base_d7  = value_at_or_before(cn, key, win["d7"])
                    base_d30 = value_at_or_before(cn, key, win["d30"])

                    pk_24h = peak_since(cn, key, win["h24"])
                    pk_7d  = peak_since(cn, key, win["d7"])
                    pk_30d = peak_since(cn, key, win["d30"])

                    metric_out = map_metric_name(metric_name, source)

                    # PROSTA ZASADA:
                    # - Dla metryk {daily_sales, market_listings, opened_last_month, opened_last_week}
                    #   z csgocasetracker_popularity -> usuń 24h
                    # - Dla wszystkich pozostałych metryk -> zachowaj 24h
                    include_24h = not (
                        source == "csgocasetracker_popularity"
                        and metric_out in DROP_24H_METRICS
                    )

                    emit(
                        item, metric_out,
                        pct_change(curr, base_h24),
                        pct_change(curr, base_d7),
                        pct_change(curr, base_d30),
                        pct_from_peak(curr, pk_24h),
                        pct_from_peak(curr, pk_7d),
                        pct_from_peak(curr, pk_30d),
                        include_24h=include_24h
                    )

                # --- PRICE
                mp = metric_present(rec, ["price", "avg_price", "median_price", "latest_price", "lowest_price"])
                if mp: handle("price", mp[1])

                # --- POPULARITY
                if "daily_sales" in rec: handle("daily_sales", rec["daily_sales"])
                if "market_listings" in rec: handle("market_listings", rec["market_listings"])

                # --- OPEN RATES
                if "opened_last_week" in rec: handle("opened_last_week", rec["opened_last_week"])
                if "opened_last_month" in rec: handle("opened_last_month", rec["opened_last_month"])

                # --- PLAYER COUNTS
                if "playing" in rec: handle("playing", rec["playing"])

                # --- AVERAGE UNBOX USD
                if "average_unbox_usd" in rec: handle("average_unbox_usd", rec["average_unbox_usd"])

                # --- UNBOX ROI PCT
                if "unbox_roi_pct" in rec: handle("unbox_roi_pct", rec["unbox_roi_pct"])

    # sortowanie i deduplikacja (bez source)
    records_buffer.sort(key=lambda r: (
        str(r.get("item", "")).lower(),
        str(r.get("metric", ""))
    ))

    dedup = {}
    for rec in records_buffer:
        key = (rec["item"], rec["metric"])
        dedup[key] = rec
    records_buffer = list(dedup.values())

    snap_fp = OUT_SNAP(ts_tag)
    with snap_fp.open("w", encoding="utf-8") as snap, OUT_LATEST.open("w", encoding="utf-8") as latest:
        header = {"script_start": ts_start_iso}
        snap.write(json.dumps(header, ensure_ascii=False) + "\n")
        latest.write(json.dumps(header, ensure_ascii=False) + "\n")
        for rec in records_buffer:
            line = json.dumps(rec, ensure_ascii=False)
            snap.write(line + "\n")
            latest.write(line + "\n")

    print(f"[OK] zapisano: {OUT_LATEST.resolve()} oraz snapshot {snap_fp.name}")

if __name__ == "__main__":
    main()
