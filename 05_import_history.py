#!/usr/bin/env python3
"""
MACRO PULSE v6.0 — 過去データ一括投入スクリプト
================================================
使用方法:
  python 05_import_history.py --source <CSV_FILE> --indicator <INDICATOR_NAME>
  python 05_import_history.py --auto-fred --from 2020-01-01

機能:
  1. --auto-fred: FRED から過去データを一括取得して 05_events.csv に投入
  2. --source: tradingeconomics 等から手動DLした CSV を変換して投入

対応指標（FRED自動）:
  NFP, Initial Claims 4W MA, Michigan Inflation 1Y, Michigan Inflation 5Y,
  CB Consumer Confidence, Building Permits,
  Yield Curve 10Y-2Y, HY Spread, VIX

手入力指標:
  ISM Manufacturing PMI, ISM Non-Manufacturing PMI, Conference Board LEI
  → --source オプションでCSVを渡す

入力CSVフォーマット（手入力指標用）:
  date,actual,consensus
  2024-01-02,47.4,47.0
  2024-02-01,49.1,49.5
  ...

注意:
  - 既存 event_id は上書きしない（--overwrite フラグで上書き可）
  - fed_context.csv の最新 regime を全行に適用（金融環境はダミー）
  - sp500_t0〜t20 は後から --fill-returns で補完
"""

import os, sys, time, json, logging, argparse
from datetime import datetime, timedelta, date

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# 05_main.py と同じパス・定数を参照
sys.path.insert(0, os.path.dirname(__file__))
from importlib import import_module

# 05_main.py のモジュールを動的ロード（ファイル名に数字があるため）
import importlib.util, pathlib

_main_path = pathlib.Path(__file__).parent / "05_main.py"
_spec = importlib.util.spec_from_file_location("main05", _main_path)
_m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

EVENTS_PATH      = _m.EVENTS_PATH
SCHEDULE_PATH    = _m.SCHEDULE_PATH
FED_CONTEXT_PATH = _m.FED_CONTEXT_PATH
EVENTS_COLUMNS   = _m.EVENTS_COLUMNS
INDICATOR_CONFIG = _m.INDICATOR_CONFIG

make_event_id    = _m.make_event_id
load_events      = _m.load_events
save_events      = _m.save_events
fred_latest      = _m.fred_latest
get_fred         = _m.get_fred
get_ff_current   = _m.get_ff_current
_fmt             = _m._fmt
_safe_float      = _m._safe_float


# ─────────────────────────────────────────────────────────────────
#  金融環境キャッシュ（全期間を一括取得してメモリに保持）
# ─────────────────────────────────────────────────────────────────

_CTX_CACHE: dict = {}   # {series_id: pd.Series}


def _load_ctx_cache(fred, from_date: str, to_date: str):
    """
    金融環境に使う全系列を1回だけ FRED から一括取得してキャッシュする。
    API呼び出しは系列数（5本）のみ。行数には依存しない。
    """
    global _CTX_CACHE
    series_ids = ["T10Y2Y", "BAMLH0A0HYM2", "VIXCLS", "DFEDTARU", "DFEDTARL"]
    for sid in series_ids:
        if sid in _CTX_CACHE:
            continue
        for attempt in range(3):
            try:
                s = fred.get_series(sid, observation_start=from_date, observation_end=to_date)
                _CTX_CACHE[sid] = s.dropna() if s is not None else pd.Series(dtype=float)
                logger.info(f"[CTX cache] {sid}: {len(_CTX_CACHE[sid])} obs")
                time.sleep(0.5)
                break
            except Exception as e:
                wait = 2 ** attempt
                logger.warning(f"[CTX cache] {sid} attempt {attempt+1}: {e} -> retry {wait}s")
                time.sleep(wait)
        else:
            _CTX_CACHE[sid] = pd.Series(dtype=float)


def _lookup_ctx(series_id: str, target_date):
    """キャッシュから target_date 以前の最新値を返す"""
    s = _CTX_CACHE.get(series_id)
    if s is None or s.empty:
        return None
    td = pd.Timestamp(target_date)
    s_before = s[s.index <= td]
    if s_before.empty:
        return None
    return float(s_before.iloc[-1])


def get_historical_context(fred, target_date) -> dict:
    """キャッシュから指定日付の金融環境スナップショットを返す（API呼び出しなし）"""
    ctx = {"regime": "", "ff_rate": "", "yc_10y2y": "", "hy_spread": "", "vix": "", "cuts_implied": ""}
    yc    = _lookup_ctx("T10Y2Y",       target_date)
    hy    = _lookup_ctx("BAMLH0A0HYM2", target_date)
    vx    = _lookup_ctx("VIXCLS",       target_date)
    ff_hi = _lookup_ctx("DFEDTARU",     target_date)
    ff_lo = _lookup_ctx("DFEDTARL",     target_date)
    if ff_hi and ff_lo: ctx["ff_rate"]  = str(round((ff_hi + ff_lo) / 2, 4))
    if yc: ctx["yc_10y2y"]  = str(round(yc, 4))
    if hy: ctx["hy_spread"]  = str(round(hy, 4))
    if vx: ctx["vix"]        = str(round(vx, 2))
    return ctx


# ─────────────────────────────────────────────────────────────────
#  FRED 一括取得
# ─────────────────────────────────────────────────────────────────

FRED_INDICATORS = {
    "NFP":                   "PAYEMS",
    "Initial Claims 4W MA":  "IC4WSA",
    "Michigan Inflation 1Y": "MICH",
    "Michigan Inflation 5Y": "T5YIE",       # 5-Year Breakeven Inflation Rate（代替）
    "CB Consumer Confidence":"CSCICP03USM665S",
    "Building Permits":      "PERMIT",
    "Yield Curve 10Y-2Y":    "T10Y2Y",
    "HY Spread":             "BAMLH0A0HYM2",
    "VIX":                   "VIXCLS",
}


def import_from_fred(from_date: str, to_date: str, overwrite: bool = False,
                     indicators: list = None):
    """
    FRED から指定期間の過去データを取得して 05_events.csv に投入。
    """
    fred = get_fred()
    if fred is None:
        logger.error("FRED client unavailable. Set FRED_API_KEY.")
        sys.exit(1)

    # 金融環境系列を事前に一括取得（5本のAPIコールで完結）
    logger.info("金融環境系列をキャッシュ中（T10Y2Y / HY / VIX / FF上下限）...")
    _load_ctx_cache(fred, from_date, to_date)

    events    = load_events()
    existing  = set(events["event_id"].tolist()) if not events.empty else set()
    new_rows  = []

    target_indicators = indicators or list(FRED_INDICATORS.keys())

    for ind_name in target_indicators:
        fred_id = FRED_INDICATORS.get(ind_name)
        if not fred_id:
            logger.warning(f"[{ind_name}] FRED IDなし。スキップ。")
            continue

        logger.info(f"[{ind_name}] FRED ID={fred_id} を取得中...")
        try:
            s = fred.get_series(fred_id,
                                observation_start=from_date,
                                observation_end=to_date)
            if s is None or s.empty:
                logger.warning(f"[{ind_name}] データなし")
                continue
            s = s.dropna()

            for obs_date, val in s.items():
                rd     = obs_date.date() if hasattr(obs_date, 'date') else obs_date
                rd_str = rd.strftime("%Y-%m-%d")
                eid    = make_event_id(ind_name, rd)

                if eid in existing and not overwrite:
                    continue

                ctx = get_historical_context(fred, rd)
                row = {col: "" for col in EVENTS_COLUMNS}
                row.update({
                    "event_id":      eid,
                    "indicator":     ind_name,
                    "release_date":  rd_str,
                    "actual":        str(round(float(val), 4)),
                    "consensus":     "",
                    "surprise":      "",
                    "surprise_pct":  "",
                    "regime":        ctx["regime"],
                    "ff_rate":       ctx["ff_rate"],
                    "yc_10y2y":      ctx["yc_10y2y"],
                    "hy_spread":     ctx["hy_spread"],
                    "vix":           ctx["vix"],
                    "cuts_implied":  ctx["cuts_implied"],
                    "forecast_source": "FRED",
                    "data_source":   "FRED",
                    "updated_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
                new_rows.append(row)

            logger.info(f"[{ind_name}] {len(s)} 件取得完了")
            time.sleep(0.3)  # FRED API レート制限対策

        except Exception as e:
            logger.error(f"[{ind_name}] エラー: {e}")

    if not new_rows:
        logger.info("新規データなし。終了。")
        return

    new_df  = pd.DataFrame(new_rows, columns=EVENTS_COLUMNS)
    key_new = set(new_df["event_id"])
    if overwrite:
        existing_filtered = events[~events["event_id"].isin(key_new)]
    else:
        existing_filtered = events[~events["event_id"].isin(key_new)]
    combined = pd.concat([existing_filtered, new_df], ignore_index=True)
    save_events(combined)
    logger.info(f"インポート完了: {len(new_rows)} 行追加 → {EVENTS_PATH}")


# ─────────────────────────────────────────────────────────────────
#  手動 CSV 投入
# ─────────────────────────────────────────────────────────────────

def import_from_csv(source_path: str, indicator: str, overwrite: bool = False):
    """
    tradingeconomics 等から手動DLした CSV を 05_events.csv に投入。

    入力フォーマット（必須列）:
      date        : YYYY-MM-DD または MM/DD/YYYY
      actual      : 実際値
      consensus   : コンセンサス（任意）

    追加で以下列があれば利用:
      previous    : 前回値
    """
    if not os.path.exists(source_path):
        logger.error(f"ファイルが見つかりません: {source_path}")
        sys.exit(1)

    try:
        src_df = pd.read_csv(source_path, dtype=str).fillna("")
    except Exception as e:
        logger.error(f"CSV読み込みエラー: {e}")
        sys.exit(1)

    # 列名を正規化（小文字・空白除去）
    src_df.columns = [c.strip().lower() for c in src_df.columns]

    required = ["date", "actual"]
    missing  = [c for c in required if c not in src_df.columns]
    if missing:
        logger.error(f"必須列なし: {missing}。列名確認: {list(src_df.columns)}")
        sys.exit(1)

    fred   = get_fred()
    if fred:
        # 日付範囲を src_df から推定してキャッシュ
        dates = src_df["date"].str.strip()
        try:
            from_d = min(dates)[:10]
            to_d   = max(dates)[:10]
            logger.info("金融環境系列をキャッシュ中...")
            _load_ctx_cache(fred, from_d, to_d)
        except Exception:
            pass
    events = load_events()
    existing = set(events["event_id"].tolist()) if not events.empty else set()
    new_rows = []
    skipped  = 0

    for _, src_row in src_df.iterrows():
        # 日付パース
        date_raw = src_row["date"].strip()
        rd = None
        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d/%m/%Y"]:
            try:
                rd = datetime.strptime(date_raw, fmt).date()
                break
            except ValueError:
                continue
        if rd is None:
            logger.warning(f"日付パース失敗: {date_raw} → スキップ")
            skipped += 1
            continue

        # 実際値パース
        try:
            actual_val = float(src_row["actual"].replace(",", ""))
        except (ValueError, AttributeError):
            logger.warning(f"実際値パース失敗: {src_row['actual']} ({rd}) → スキップ")
            skipped += 1
            continue

        eid = make_event_id(indicator, rd)
        if eid in existing and not overwrite:
            skipped += 1
            continue

        # コンセンサス・サプライズ
        consensus_val = None
        surprise      = None
        surprise_pct  = None
        if "consensus" in src_row and src_row["consensus"].strip():
            try:
                consensus_val = float(src_row["consensus"].replace(",", ""))
                surprise      = round(actual_val - consensus_val, 4)
                surprise_pct  = round(surprise / abs(consensus_val) * 100, 4) if consensus_val != 0 else 0.0
            except (ValueError, AttributeError):
                pass

        ctx = get_historical_context(fred, rd) if fred else {}
        rd_str = rd.strftime("%Y-%m-%d")

        row = {col: "" for col in EVENTS_COLUMNS}
        row.update({
            "event_id":       eid,
            "indicator":      indicator,
            "release_date":   rd_str,
            "actual":         str(actual_val),
            "consensus":      str(consensus_val) if consensus_val is not None else "",
            "surprise":       str(surprise)      if surprise is not None else "",
            "surprise_pct":   str(surprise_pct)  if surprise_pct is not None else "",
            "regime":         ctx.get("regime", ""),
            "ff_rate":        ctx.get("ff_rate", ""),
            "yc_10y2y":       ctx.get("yc_10y2y", ""),
            "hy_spread":      ctx.get("hy_spread", ""),
            "vix":            ctx.get("vix", ""),
            "cuts_implied":   ctx.get("cuts_implied", ""),
            "forecast_source": "user_retroactive" if consensus_val is not None else "actual_as_forecast",
            "data_source":    "manual_import",
            "updated_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        new_rows.append(row)

    if not new_rows:
        logger.info(f"新規データなし（スキップ: {skipped}件）。終了。")
        return

    new_df = pd.DataFrame(new_rows, columns=EVENTS_COLUMNS)
    key_new = set(new_df["event_id"])
    existing_filtered = events[~events["event_id"].isin(key_new)] if overwrite else \
                        events[~events["event_id"].isin(key_new)]
    combined = pd.concat([existing_filtered, new_df], ignore_index=True)
    save_events(combined)
    logger.info(f"インポート完了: {len(new_rows)} 行追加、{skipped} 行スキップ → {EVENTS_PATH}")


# ─────────────────────────────────────────────────────────────────
#  Entry Point
# ─────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="MACRO PULSE v6.0 — 過去データ一括投入")
    sub = p.add_subparsers(dest="mode", required=True)

    # FRED 自動取得モード
    fred_p = sub.add_parser("fred", help="FRED から過去データを一括取得")
    fred_p.add_argument("--from",  dest="from_date", default="2020-01-01", help="開始日 YYYY-MM-DD")
    fred_p.add_argument("--to",    dest="to_date",   default=date.today().strftime("%Y-%m-%d"), help="終了日")
    fred_p.add_argument("--indicators", nargs="*",   help="取得する指標名（省略時は全FRED指標）")
    fred_p.add_argument("--overwrite", action="store_true", help="既存データを上書き")

    # 手動CSV投入モード
    csv_p = sub.add_parser("csv", help="手動DLしたCSVを投入")
    csv_p.add_argument("--source",    required=True, help="入力CSVファイルパス")
    csv_p.add_argument("--indicator", required=True, help="指標名（例: 'ISM Manufacturing PMI'）")
    csv_p.add_argument("--overwrite", action="store_true")

    args = p.parse_args()

    if args.mode == "fred":
        import_from_fred(args.from_date, args.to_date, args.overwrite, args.indicators)
    elif args.mode == "csv":
        import_from_csv(args.source, args.indicator, args.overwrite)


if __name__ == "__main__":
    main()
