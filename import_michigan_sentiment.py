#!/usr/bin/env python3
"""
Michigan Consumer Sentiment (UMCSENT) の過去データを 05_events.csv に投入するスクリプト
使い方:
    $env:FRED_API_KEY="your_key"
    python import_michigan_sentiment.py
実行場所: リポジトリルート（data/05_events.csv がある場所）
"""
import csv, os, sys, shutil
from datetime import date, timedelta

EVENTS_PATH    = "data/05_events.csv"
INDICATOR_NAME = "Michigan Consumer Sentiment"
FRED_ID        = "UMCSENT"
START_DATE     = "2019-01-01"   # 投入開始日（これ以前は不要）

EVENTS_COLUMNS = [
    "event_id", "indicator", "release_date",
    "actual", "consensus", "surprise", "surprise_pct",
    "regime", "ff_rate", "yc_10y2y", "hy_spread", "vix", "cuts_implied",
    "sp500_t0", "sp500_t1", "sp500_t5", "sp500_t10", "sp500_t20",
    "ret_t1", "ret_t5", "ret_t10", "ret_t20",
    "forecast_source", "data_source", "analysis", "updated_at",
]


def get_fred():
    try:
        from fredapi import Fred
        key = os.environ.get("FRED_API_KEY", "")
        if not key:
            print("ERROR: FRED_API_KEY が設定されていません。")
            sys.exit(1)
        return Fred(api_key=key)
    except ImportError:
        print("ERROR: fredapi がインストールされていません。pip install fredapi")
        sys.exit(1)


def observation_to_release_date(obs_date: date) -> date:
    """
    FREDのobservation_date（月初）を実際の発表日（第2金曜）に変換する。
    ミシガン大学消費者信頼感は毎月第2金曜に速報値を発表。
    """
    # 当月の第2金曜を算出
    first_day = date(obs_date.year, obs_date.month, 1)
    # weekday(): 月=0, 火=1, ..., 金=4
    days_to_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_to_friday)
    second_friday = first_friday + timedelta(weeks=1)
    return second_friday


def main():
    if not os.path.exists(EVENTS_PATH):
        print(f"ERROR: {EVENTS_PATH} が見つかりません。リポジトリルートで実行してください。")
        sys.exit(1)

    # FRED からデータ取得
    print(f"FRED から {FRED_ID} を取得中...")
    fred = get_fred()
    series = fred.get_series(FRED_ID, observation_start=START_DATE).dropna()
    print(f"取得件数: {len(series)} 件 ({series.index[0].date()} 〜 {series.index[-1].date()})")

    # 既存events読み込み
    with open(EVENTS_PATH, encoding="utf-8") as f:
        existing = list(csv.DictReader(f))
    print(f"既存 events: {len(existing)} 行")

    existing_keys = {
        (r.get("indicator", ""), r.get("release_date", ""))
        for r in existing
    }
    sent_existing = sum(1 for r in existing if r.get("indicator") == INDICATOR_NAME)
    print(f"  うち {INDICATOR_NAME}: {sent_existing} 行")

    # 新規行を作成
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_rows = []
    skipped  = []

    for obs_date, actual_val in series.items():
        obs_date_obj = obs_date.date() if hasattr(obs_date, 'date') else obs_date
        release_date = observation_to_release_date(obs_date_obj)
        release_str  = release_date.strftime("%Y-%m-%d")
        event_id     = f"MICH_SENT_{release_str}"

        key = (INDICATOR_NAME, release_str)
        if key in existing_keys:
            skipped.append(release_str)
            continue

        row = {col: "" for col in EVENTS_COLUMNS}
        row.update({
            "event_id":     event_id,
            "indicator":    INDICATOR_NAME,
            "release_date": release_str,
            "actual":       str(round(float(actual_val), 1)),
            "data_source":  "FRED",
            "updated_at":   now,
        })
        new_rows.append(row)

    print(f"投入データ: {len(series)} 件")
    print(f"  スキップ（重複）: {len(skipped)} 件")
    print(f"  新規追加: {len(new_rows)} 件")

    if not new_rows:
        print("追加するデータがありません。終了します。")
        return

    # マージ & ソート
    combined = existing + new_rows
    combined.sort(key=lambda r: (r.get("release_date", ""), r.get("indicator", "")))

    # バックアップ
    backup = EVENTS_PATH + ".bak"
    shutil.copy2(EVENTS_PATH, backup)
    print(f"バックアップ: {backup}")

    # 書き出し
    with open(EVENTS_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EVENTS_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for row in combined:
            out = {col: row.get(col, "") for col in EVENTS_COLUMNS}
            w.writerow(out)

    print(f"✅ 書き込み完了: {EVENTS_PATH}  合計 {len(combined)} 行")
    print(f"   （{INDICATOR_NAME}: {sent_existing + len(new_rows)} 行）")
    print()
    print("次のステップ:")
    print("  python 05_main.py --fill-returns")
    print("  git add data/05_events.csv")
    print(f"  git commit -m 'add: {INDICATOR_NAME} {len(new_rows)}件 ({START_DATE[:7]}〜)'")
    print("  git push")


if __name__ == "__main__":
    main()
