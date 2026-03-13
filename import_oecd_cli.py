#!/usr/bin/env python3
"""
OECD CLI (USALOLITONOSTSAM) 過去データを 05_events.csv に一括投入するスクリプト

使い方:
  FRED_API_KEY=your_key python3 import_oecd_cli.py

実行場所: リポジトリルート（data/05_events.csv がある場所）

取得範囲: 2019-01-01 〜 今日
発表日補正: FREDのobservation_dateは「データ対象月の月初」
           → 実際のOECD CLI発表は「翌月第2週月曜」なので +5〜6週ずらす
"""
import os, sys, csv, shutil
from datetime import date, timedelta

# ── FRED_API_KEY チェック ──
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
if not FRED_API_KEY:
    print("ERROR: 環境変数 FRED_API_KEY が設定されていません。")
    print("  実行例: FRED_API_KEY=abcdef1234 python3 import_oecd_cli.py")
    sys.exit(1)

try:
    from fredapi import Fred
except ImportError:
    print("ERROR: fredapi がインストールされていません。")
    print("  pip install fredapi")
    sys.exit(1)

import pandas as pd

# ── 設定 ──
SERIES_ID   = "USALOLITONOSTSAM"
INDICATOR   = "Conference Board LEI"   # events.csv での指標名（既存と合わせる）
EVENTS_PATH = "data/05_events.csv"
START_DATE  = "2019-01-01"

EVENTS_COLUMNS = [
    "event_id", "indicator", "release_date",
    "actual", "consensus", "surprise", "surprise_pct",
    "regime", "ff_rate", "yc_10y2y", "hy_spread", "vix", "cuts_implied",
    "sp500_t0", "sp500_t1", "sp500_t5", "sp500_t10", "sp500_t20",
    "ret_t1", "ret_t5", "ret_t10", "ret_t20",
    "forecast_source", "data_source", "analysis", "updated_at",
]

def nth_monday_of_month(year: int, month: int, nth: int) -> date:
    """指定月のn番目の月曜日を返す"""
    first = date(year, month, 1)
    days_to_monday = (7 - first.weekday()) % 7  # 0=月曜
    first_monday = first + timedelta(days=days_to_monday)
    return first_monday + timedelta(weeks=nth - 1)

def observation_to_release_date(obs_date: date) -> date:
    """
    FREDのobservation_date（データ対象月の月初）→ 実際の発表日に変換。
    OECD CLIは対象月の翌々月第2週月曜日前後に発表。
    例: 2024-12-01（12月データ）→ 2025-02 第2週月曜 = 2025-02-10
    """
    # 翌々月を計算
    release_month = obs_date.month + 2
    release_year  = obs_date.year
    if release_month > 12:
        release_month -= 12
        release_year  += 1
    # その月の第2月曜
    return nth_monday_of_month(release_year, release_month, 2)

def main():
    if not os.path.exists(EVENTS_PATH):
        print(f"ERROR: {EVENTS_PATH} が見つかりません。リポジトリルートで実行してください。")
        sys.exit(1)

    # ── FRED から全データ取得 ──
    print(f"FREDから {SERIES_ID} を取得中 ({START_DATE} 〜 今日)...")
    fred = Fred(api_key=FRED_API_KEY)
    try:
        series = fred.get_series(
            SERIES_ID,
            observation_start=START_DATE,
            observation_end=date.today().strftime("%Y-%m-%d"),
        )
    except Exception as e:
        print(f"ERROR: FRED取得失敗: {e}")
        sys.exit(1)

    series = series.dropna()
    print(f"取得件数: {len(series)} 件  ({series.index[0].date()} 〜 {series.index[-1].date()})")

    # ── 既存events読み込み ──
    df_events = pd.read_csv(EVENTS_PATH, dtype=str).fillna("")
    existing_keys = set(zip(df_events["indicator"], df_events["release_date"]))
    lei_existing  = (df_events["indicator"] == INDICATOR).sum()
    print(f"既存 events: {len(df_events)} 行  (LEI: {lei_existing} 件)")

    # ── 新規行の生成 ──
    now_str  = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    new_rows = []
    skipped  = []

    for obs_ts, val in series.items():
        obs_date      = obs_ts.date()
        release_date  = observation_to_release_date(obs_date)

        # 未来の発表日はスキップ（データがまだ確定していない可能性）
        if release_date > date.today():
            skipped.append(f"{release_date} (未来)")
            continue

        release_str = release_date.strftime("%Y-%m-%d")
        key = (INDICATOR, release_str)
        if key in existing_keys:
            skipped.append(f"{release_str} (重複)")
            continue

        event_id = f"cb_lei_{release_str.replace('-', '')}"
        row = {col: "" for col in EVENTS_COLUMNS}
        row.update({
            "event_id":       event_id,
            "indicator":      INDICATOR,
            "release_date":   release_str,
            "actual":         f"{val:.4f}",
            "consensus":      "",           # 過去のコンセンサスは取得不可
            "surprise":       "",
            "surprise_pct":   "",
            "forecast_source": "actual_as_forecast",
            "data_source":    f"FRED:{SERIES_ID}",
            "analysis":       "",
            "updated_at":     now_str,
        })
        new_rows.append(row)

    print(f"新規追加: {len(new_rows)} 件  スキップ: {len(skipped)} 件")
    if skipped[:5]:
        print(f"  スキップ例: {skipped[:5]}")

    if not new_rows:
        print("追加するデータがありません。終了します。")
        return

    # ── サンプル表示 ──
    print("\n--- 最新5件プレビュー ---")
    for r in new_rows[-5:]:
        print(f"  {r['release_date']}  actual={r['actual']}")

    # ── バックアップ & マージ & 保存 ──
    backup = EVENTS_PATH + ".bak"
    shutil.copy2(EVENTS_PATH, backup)
    print(f"\nバックアップ: {backup}")

    df_new    = pd.DataFrame(new_rows, columns=EVENTS_COLUMNS)
    df_merged = pd.concat([df_events, df_new], ignore_index=True)
    df_merged = df_merged.drop_duplicates(subset=["event_id"], keep="last")
    df_merged = df_merged.sort_values(["release_date", "indicator"]).reset_index(drop=True)
    df_merged.to_csv(EVENTS_PATH, index=False, encoding="utf-8")

    print(f"✅ 書き込み完了: {EVENTS_PATH}  合計 {len(df_merged)} 行")
    print(f"   (Conference Board LEI: {lei_existing + len(new_rows)} 行)")
    print()
    print("次のステップ:")
    print("  git add data/05_events.csv")
    print("  git commit -m 'add: OECD CLI 過去データ一括投入 (2019〜)'")
    print("  git push")
    print()
    print("  その後: python3 05_main.py --fill-returns")
    print("  → S&P500リターン (ret_t1〜ret_t20) が自動補完されます")

if __name__ == "__main__":
    main()
