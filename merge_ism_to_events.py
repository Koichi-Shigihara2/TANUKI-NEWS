#!/usr/bin/env python3
"""
ISM Manufacturing PMI を 05_events.csv に安全にマージするスクリプト
使い方: python3 merge_ism_to_events.py
実行場所: リポジトリルート（data/05_events.csv がある場所）
"""
import csv, os, sys
from datetime import datetime

EVENTS_PATH = "data/05_events.csv"
ISM_PATH    = "ism_mfg_import.csv"   # このスクリプトと同じ場所に置く

EVENTS_COLUMNS = [
    "event_id", "indicator", "release_date",
    "actual", "consensus", "surprise", "surprise_pct",
    "regime", "ff_rate", "yc_10y2y", "hy_spread", "vix", "cuts_implied",
    "sp500_t0", "sp500_t1", "sp500_t5", "sp500_t10", "sp500_t20",
    "ret_t1", "ret_t5", "ret_t10", "ret_t20",
    "forecast_source", "data_source", "analysis", "updated_at",
]

def main():
    if not os.path.exists(EVENTS_PATH):
        print(f"ERROR: {EVENTS_PATH} が見つかりません。リポジトリルートで実行してください。")
        sys.exit(1)
    if not os.path.exists(ISM_PATH):
        print(f"ERROR: {ISM_PATH} が見つかりません。")
        sys.exit(1)

    # 既存events読み込み
    with open(EVENTS_PATH, encoding="utf-8") as f:
        existing = list(csv.DictReader(f))
    print(f"既存 events: {len(existing)} 行")

    # 既存のISMキーを収集（重複防止）
    existing_keys = {
        (r.get("indicator",""), r.get("release_date",""))
        for r in existing
    }
    ism_existing = sum(1 for r in existing if r.get("indicator") == "ISM Manufacturing PMI")
    print(f"  うち ISM Manufacturing PMI: {ism_existing} 行")

    # 投入データ読み込み
    with open(ISM_PATH, encoding="utf-8") as f:
        to_import = list(csv.DictReader(f))

    # 重複除外
    new_rows = []
    skipped  = []
    for r in to_import:
        key = (r["indicator"], r["release_date"])
        if key in existing_keys:
            skipped.append(r["release_date"])
        else:
            new_rows.append(r)

    print(f"投入データ: {len(to_import)} 行")
    print(f"  スキップ（重複）: {len(skipped)} 件 → {skipped[:5]}{'...' if len(skipped)>5 else ''}")
    print(f"  新規追加: {len(new_rows)} 件")

    if not new_rows:
        print("追加するデータがありません。終了します。")
        return

    # マージ & ソート
    combined = existing + new_rows
    combined.sort(key=lambda r: (r.get("release_date",""), r.get("indicator","")))

    # バックアップ
    backup = EVENTS_PATH + ".bak"
    import shutil
    shutil.copy2(EVENTS_PATH, backup)
    print(f"バックアップ: {backup}")

    # 書き出し（列順を EVENTS_COLUMNS に統一）
    with open(EVENTS_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EVENTS_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for row in combined:
            # 不足列は空文字で補完
            out = {col: row.get(col, "") for col in EVENTS_COLUMNS}
            w.writerow(out)

    print(f"✅ 書き込み完了: {EVENTS_PATH}  合計 {len(combined)} 行")
    print(f"   （ISM Manufacturing PMI: {ism_existing + len(new_rows)} 行）")
    print()
    print("次のステップ:")
    print("  git add data/05_events.csv")
    print("  git commit -m 'add: ISM Manufacturing PMI 79件 (2019-09〜2026-03)'")
    print("  git push")
    print()
    print("  その後 GitHub Actions で --fill-returns を実行するか、")
    print("  手動で: python3 05_main.py --fill-returns")

if __name__ == "__main__":
    main()
