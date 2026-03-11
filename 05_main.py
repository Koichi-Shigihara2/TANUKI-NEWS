#!/usr/bin/env python3
"""
MACRO PULSE — Economic Indicators Auto-Update  v4.0
====================================================
変更点 (v3 → v4):
  [発表検知]  FRED observation_date マッチング（ほぼ失敗）→
              data/05_indicator_schedule.csv を起点にした確定的な検知に変更。
  [期待値]    FMP / スクレイピング依存を廃止。
              優先: ① CSV 既存値 → ② actual_as_forecast（実績=予想, Surprise=0）
  [再計算]    --recalc フラグ: ブラウザ側で予想値が事後入力された後、
              CSV の forecast_source / Surprise を一括更新する。
  [ISM PMI]  FRED 未収録。schedule.csv の actual 列に手入力 → override で取込。
  [列追加]    forecast_source 列を CSV に追加。
              'user'               = ユーザー事前入力（LocalStorage → CSV 書き戻し時）
              'user_retroactive'   = 事後入力（再計算済み）
              'actual_as_forecast' = 予想なし自動代替（Surprise=0, グレー表示）
              'FRED'               = FRED から直接取得
              'none'               = No Indicators / YieldCurve 行
"""

import os, sys, time, json, logging, argparse, traceback
from datetime import datetime, timedelta, date
from io import StringIO

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

CSV_PATH      = "data/05_economic_history.csv"
SCHEDULE_PATH = "data/05_indicator_schedule.csv"

CSV_COLUMNS = [
    "指標名", "リリース日", "実際値", "期待値(Consensus)", "前回値",
    "Surprise(実際-期待)", "forecast_source", "YoY変化(%)",
    "S&P500", "Nasdaq", "10Y-2Y(YieldCurve)",
    "付随データ", "市場反応(自動生成)", "データソース", "更新日時",
]

SCHEDULE_COLUMNS = [
    "指標名", "発表予定日", "fred_id", "閾値_強気", "閾値_弱気", "単位", "actual", "備考",
]

# ─────────────────────────────────────────────────────────────────
#  指標マスタ
# ─────────────────────────────────────────────────────────────────
INDICATOR_CONFIG = {
    "ISM Manufacturing PMI": {
        "fred_id": "",                # FRED 未収録。schedule.csv の actual 列から手入力
        "companion_key":  "Mfg Employment",
        "companion_fred": "MANEMP",
        "threshold_bull": 50.0,
        "threshold_bear": 50.0,
        "unit": "index",
    },
    "New Residential Starts": {
        "fred_id": "HOUST",
        "companion_key":  "Mortgage Rate 30Y",
        "companion_fred": "MORTGAGE30US",
        "threshold_bull": 1400,
        "threshold_bear": 1200,
        "unit": "千件",
    },
    "Durable Goods Orders": {
        "fred_id": "DGORDER",
        "companion_key":  "Durable Ex-Transport",
        "companion_fred": "ADXTNO",
        "threshold_bull": 0,
        "threshold_bear": 0,
        "unit": "%MoM",
    },
    "Initial Jobless Claims": {
        "fred_id": "ICSA",
        "companion_key":  "4W Moving Avg",
        "companion_fred": "IC4WSA",
        "threshold_bull": 250000,
        "threshold_bear": 300000,
        "unit": "件",
    },
    "Average Hourly Earnings YoY": {
        "fred_id": "AHETPI",
        "companion_key":  "CPI (CPIAUCSL)",
        "companion_fred": "CPIAUCSL",
        "threshold_bull": 0,
        "threshold_bear": 0,
        "unit": "%YoY",
    },
    "Michigan Consumer Sentiment": {
        "fred_id": "UMCSENT",
        "companion_key":  "Michigan 1Y Inflation Exp",
        "companion_fred": "MICH",
        "threshold_bull": 80.0,
        "threshold_bear": 65.0,
        "unit": "index",
    },
}

# ─────────────────────────────────────────────────────────────────
#  スケジュール CSV
# ─────────────────────────────────────────────────────────────────

def ensure_schedule_csv():
    """schedule.csv が存在しなければテンプレートを生成する。"""
    os.makedirs("data", exist_ok=True)
    if os.path.exists(SCHEDULE_PATH):
        return
    rows = []
    for name, cfg in INDICATOR_CONFIG.items():
        rows.append({
            "指標名":    name,
            "発表予定日": "",          # ユーザーが YYYY-MM-DD で記入
            "fred_id":  cfg.get("fred_id", ""),
            "閾値_強気": cfg.get("threshold_bull", ""),
            "閾値_弱気": cfg.get("threshold_bear", ""),
            "単位":      cfg.get("unit", ""),
            "actual":   "",            # ISM 等 FRED 未収録の手入力実績値
            "備考":      "",
        })
    pd.DataFrame(rows, columns=SCHEDULE_COLUMNS).to_csv(
        SCHEDULE_PATH, index=False, encoding="utf-8")
    logger.info(f"Created schedule template: {SCHEDULE_PATH}")


def load_schedule() -> pd.DataFrame:
    if not os.path.exists(SCHEDULE_PATH):
        logger.warning(f"{SCHEDULE_PATH} not found.")
        return pd.DataFrame(columns=SCHEDULE_COLUMNS)
    df = pd.read_csv(SCHEDULE_PATH, encoding="utf-8", dtype=str).fillna("")
    df["発表予定日"] = df["発表予定日"].str.strip()
    return df


def get_scheduled_for(target_date: date, schedule: pd.DataFrame) -> list[dict]:
    """target_date に発表予定の指標行リストを返す。"""
    date_str = target_date.strftime("%Y-%m-%d")
    return schedule[schedule["発表予定日"] == date_str].to_dict("records")

# ─────────────────────────────────────────────────────────────────
#  API クライアント
# ─────────────────────────────────────────────────────────────────

def get_fred():
    try:
        from fredapi import Fred
        key = os.environ.get("FRED_API_KEY", "")
        if not key:
            logger.warning("FRED_API_KEY not set.")
            return None
        return Fred(api_key=key)
    except ImportError:
        logger.warning("fredapi not installed.")
        return None

# ─────────────────────────────────────────────────────────────────
#  FRED helpers
# ─────────────────────────────────────────────────────────────────

def fred_latest(fred, series_id: str, target_date: date, lookback: int = 60):
    """(value, obs_date) または (None, None)"""
    try:
        end   = target_date.strftime("%Y-%m-%d")
        start = (target_date - timedelta(days=lookback)).strftime("%Y-%m-%d")
        s = fred.get_series(series_id, observation_start=start, observation_end=end)
        if s is None or s.empty:
            return None, None
        s = s.dropna()
        if s.empty:
            return None, None
        return float(s.iloc[-1]), s.index[-1].date()
    except Exception as e:
        logger.warning(f"FRED [{series_id}]: {e}")
        return None, None


def fred_previous(fred, series_id: str, current_date: date):
    try:
        end   = (current_date - timedelta(days=1)).strftime("%Y-%m-%d")
        start = (current_date - timedelta(days=90)).strftime("%Y-%m-%d")
        s = fred.get_series(series_id, observation_start=start, observation_end=end)
        if s is None or s.empty:
            return None
        s = s.dropna()
        return float(s.iloc[-1]) if not s.empty else None
    except Exception as e:
        logger.warning(f"FRED [{series_id}] prev: {e}")
        return None


def fred_yoy(fred, series_id: str, release_date: date, current_val: float):
    try:
        yr_ago = release_date - timedelta(days=365)
        s = fred.get_series(
            series_id,
            observation_start=(yr_ago - timedelta(days=45)).strftime("%Y-%m-%d"),
            observation_end  =(yr_ago + timedelta(days=45)).strftime("%Y-%m-%d"),
        )
        if s is None or s.empty:
            return None
        s = s.dropna()
        if s.empty:
            return None
        v = float(s.iloc[-1])
        return round((current_val - v) / abs(v) * 100, 4) if v != 0 else None
    except Exception as e:
        logger.warning(f"FRED [{series_id}] YoY: {e}")
        return None

# ─────────────────────────────────────────────────────────────────
#  市場データ取得
# ─────────────────────────────────────────────────────────────────

def _stooq(symbol: str, target_date: date):
    try:
        d1 = (target_date - timedelta(days=10)).strftime("%Y%m%d")
        d2 = target_date.strftime("%Y%m%d")
        url = f"https://stooq.com/q/d/l/?s={symbol}&d1={d1}&d2={d2}&i=d"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
        txt = r.text.strip()
        if not txt or "No data" in txt:
            return None
        df = pd.read_csv(StringIO(txt))
        df.columns = [c.strip() for c in df.columns]
        if "Close" not in df.columns or df.empty:
            return None
        df["Date"] = pd.to_datetime(df["Date"])
        return round(float(df.sort_values("Date")["Close"].iloc[-1]), 2)
    except Exception as e:
        logger.warning(f"stooq [{symbol}]: {e}")
        return None


def get_market(target_date: date, fred=None):
    """(sp500, nasdaq, yield_curve, sp500_prev) — 失敗時は float('nan')"""
    sp500 = nasdaq = yield_curve = sp500_prev = float("nan")

    # S&P500
    if fred:
        v, _ = fred_latest(fred, "SP500", target_date, lookback=10)
        if v:
            sp500 = v
            logger.info(f"S&P500 (FRED): {sp500}")
    if sp500 != sp500:   # nan
        v = _stooq("%5Espx", target_date)
        if v:
            sp500 = v
            logger.info(f"S&P500 (stooq): {sp500}")

    # Nasdaq
    v = _stooq("%5Endx", target_date)
    if v:
        nasdaq = v
        logger.info(f"Nasdaq (stooq): {nasdaq}")

    # Yield Curve
    if fred:
        v, _ = fred_latest(fred, "T10Y2Y", target_date)
        if v is not None:
            yield_curve = round(v, 4)
            logger.info(f"YieldCurve (FRED): {yield_curve}")

    # 前日 S&P（Surprise/市場反応テキスト用）
    prev_date = target_date - timedelta(days=1)
    if fred:
        v, _ = fred_latest(fred, "SP500", prev_date, lookback=10)
        if v:
            sp500_prev = v
    if sp500_prev != sp500_prev:
        v = _stooq("%5Espx", prev_date)
        if v:
            sp500_prev = v

    return sp500, nasdaq, yield_curve, sp500_prev

# ─────────────────────────────────────────────────────────────────
#  期待値解決 (v4 コア)
# ─────────────────────────────────────────────────────────────────

def resolve_forecast(ind_name: str, release_str: str, actual_val,
                     existing: pd.DataFrame):
    """
    優先順位:
      1. 既存 CSV に期待値(Consensus) が記録済み → そのまま使用
      2. 予想なし → actual_as_forecast (Surprise=0, グレー表示)

    Returns: (forecast_val, forecast_source, surprise)
    """
    if not existing.empty:
        mask = (
            (existing["指標名"]   == ind_name) &
            (existing["リリース日"] == release_str) &
            (existing["期待値(Consensus)"].notna())
        )
        hits = existing[mask]
        if not hits.empty:
            row  = hits.iloc[-1]
            try:
                fv  = float(row["期待値(Consensus)"])
                src = str(row.get("forecast_source", "stored") or "stored")
                surp = round(actual_val - fv, 4) if actual_val is not None else float("nan")
                logger.info(f"[{ind_name}] forecast from CSV: {fv} (src={src})")
                return fv, src, surp
            except (ValueError, TypeError):
                pass

    # フォールバック: 実績=予想
    if actual_val is not None:
        logger.info(f"[{ind_name}] no forecast → actual_as_forecast ({actual_val})")
        return actual_val, "actual_as_forecast", 0.0

    return float("nan"), "none", float("nan")

# ─────────────────────────────────────────────────────────────────
#  付随データ
# ─────────────────────────────────────────────────────────────────

def build_companion(ind_name: str, fred, target_date: date, actual_val) -> str:
    cfg = INDICATOR_CONFIG.get(ind_name, {})
    companion = {}
    ck, cf = cfg.get("companion_key"), cfg.get("companion_fred")
    if ck and cf and fred:
        v, _ = fred_latest(fred, cf, target_date)
        if v is not None:
            companion[ck] = v
    if ind_name == "Average Hourly Earnings YoY" and fred and actual_val is not None:
        cpi, _ = fred_latest(fred, "CPIAUCSL", target_date)
        if cpi is not None:
            companion["Real Wage Diff (AHE-CPI)"] = round(actual_val - cpi, 4)
    if ind_name == "Michigan Consumer Sentiment" and fred:
        m1, _ = fred_latest(fred, "MICH", target_date)
        if m1 is not None:
            companion["1Y Inflation Exp"] = m1
    return json.dumps(companion, ensure_ascii=False) if companion else "{}"

# ─────────────────────────────────────────────────────────────────
#  市場反応テキスト
# ─────────────────────────────────────────────────────────────────

def market_reaction(surprise, forecast_source: str, sp_today, sp_prev) -> str:
    parts = []
    if forecast_source == "actual_as_forecast":
        parts.append("No forecast (actual used)")
    elif pd.isna(surprise):
        parts.append("Surprise: N/A")
    elif surprise > 0:
        parts.append(f"Positive surprise (+{surprise:.3f})")
    elif surprise < 0:
        parts.append(f"Negative surprise ({surprise:.3f})")
    else:
        parts.append("In-line")
    try:
        if sp_today and sp_prev and not (pd.isna(sp_today) or pd.isna(sp_prev)):
            chg = (sp_today - sp_prev) / sp_prev * 100
            parts.append(f"S&P {'up' if chg >= 0 else 'down'} {abs(chg):.2f}%")
    except Exception:
        pass
    return "; ".join(parts)

# ─────────────────────────────────────────────────────────────────
#  指標1件フェッチ
# ─────────────────────────────────────────────────────────────────

def fetch_indicator(ind_name: str, target_date: date, fred,
                    sp500, nasdaq, yield_curve, sp500_prev,
                    existing: pd.DataFrame,
                    override_actual=None) -> dict:
    """
    override_actual: FRED 未収録指標（ISM 等）向け。
                     schedule.csv の actual 列から渡す手入力実績値。
    """
    row = {col: float("nan") for col in CSV_COLUMNS}
    row.update({
        "指標名":    ind_name,
        "リリース日": target_date.strftime("%Y-%m-%d"),
        "S&P500":   sp500,
        "Nasdaq":   nasdaq,
        "10Y-2Y(YieldCurve)": yield_curve,
        "更新日時":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })

    fred_id    = INDICATOR_CONFIG.get(ind_name, {}).get("fred_id", "")
    actual_val = override_actual
    prev_val   = None
    release_date_used = target_date
    sources    = []

    # FRED: 実際値・前回値（fred_id がある場合のみ）
    if fred and fred_id and actual_val is None:
        for attempt in range(3):
            try:
                a, d = fred_latest(fred, fred_id, target_date)
                if a is not None:
                    actual_val = a
                    if d:
                        release_date_used = d
                    prev_val = fred_previous(fred, fred_id, release_date_used)
                    sources.append("FRED")
                    logger.info(f"[{ind_name}] FRED: actual={a}, prev={prev_val}, date={d}")
                break
            except Exception as e:
                logger.warning(f"[{ind_name}] FRED attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)

    # override_actual 時も前回値を FRED から取得
    if fred and fred_id and override_actual is not None and prev_val is None:
        prev_val = fred_previous(fred, fred_id, release_date_used)

    release_str = release_date_used.strftime("%Y-%m-%d")

    # 期待値の決定（v4 コア）
    forecast_val, forecast_src, surprise = resolve_forecast(
        ind_name, release_str, actual_val, existing)

    row["リリース日"]         = release_str
    row["実際値"]             = actual_val if actual_val is not None else float("nan")
    row["期待値(Consensus)"]  = forecast_val
    row["前回値"]             = prev_val   if prev_val   is not None else float("nan")
    row["Surprise(実際-期待)"] = surprise
    row["forecast_source"]    = forecast_src

    # YoY
    if fred and fred_id and actual_val is not None:
        yoy = fred_yoy(fred, fred_id, release_date_used, actual_val)
        if yoy is not None:
            row["YoY変化(%)"] = yoy

    row["付随データ"]         = build_companion(ind_name, fred, target_date, actual_val)
    row["市場反応(自動生成)"] = market_reaction(surprise, forecast_src, sp500, sp500_prev)
    row["データソース"]       = (", ".join(sources)
                                 if sources else ("手入力" if override_actual is not None else "N/A"))
    return row

# ─────────────────────────────────────────────────────────────────
#  CSV I/O
# ─────────────────────────────────────────────────────────────────

def load_csv() -> pd.DataFrame:
    if not os.path.exists(CSV_PATH):
        return pd.DataFrame(columns=CSV_COLUMNS)
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8")
        for c in CSV_COLUMNS:
            if c not in df.columns:
                df[c] = float("nan")
        return df
    except Exception as e:
        logger.warning(f"CSV read error: {e}")
        return pd.DataFrame(columns=CSV_COLUMNS)


def save_csv(df: pd.DataFrame):
    os.makedirs("data", exist_ok=True)
    df = df.drop_duplicates(subset=["リリース日", "指標名"], keep="last")
    df = df.sort_values(["リリース日", "指標名"]).reset_index(drop=True)
    df.to_csv(CSV_PATH, index=False, encoding="utf-8")
    logger.info(f"CSV updated: {CSV_PATH} ({len(df)} rows)")

# ─────────────────────────────────────────────────────────────────
#  再計算モード (--recalc)
# ─────────────────────────────────────────────────────────────────

def recalc(df: pd.DataFrame) -> pd.DataFrame:
    """
    CSV の全レコードを走査し:
      - forecast_source == 'actual_as_forecast' かつ 期待値(Consensus) が実際値と異なる
        → ユーザーが事後入力した証拠: Surprise を再計算し forecast_source を 'user_retroactive' に更新
      - forecast_source in ('user', 'user_retroactive')
        → Surprise が古ければ再計算
    """
    updated = 0
    for idx, row in df.iterrows():
        try:
            actual   = float(row["実際値"])
            forecast = float(row["期待値(Consensus)"])
        except (ValueError, TypeError):
            continue

        src     = str(row.get("forecast_source", "") or "")
        new_sur = round(actual - forecast, 4)
        old_sur = row.get("Surprise(実際-期待)")

        if src == "actual_as_forecast" and forecast != actual:
            # 期待値が事後更新されている → user_retroactive に昇格
            df.at[idx, "Surprise(実際-期待)"]  = new_sur
            df.at[idx, "forecast_source"]      = "user_retroactive"
            df.at[idx, "市場反応(自動生成)"]   = market_reaction(
                new_sur, "user_retroactive", row.get("S&P500"), None)
            df.at[idx, "更新日時"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            updated += 1
            logger.info(f"[RECALC] {row['指標名']} {row['リリース日']}: {old_sur} → {new_sur}")

        elif src in ("user", "user_retroactive") and old_sur != new_sur:
            df.at[idx, "Surprise(実際-期待)"] = new_sur
            df.at[idx, "更新日時"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            updated += 1
            logger.info(f"[RECALC] {row['指標名']} {row['リリース日']}: refreshed {old_sur} → {new_sur}")

    logger.info(f"Recalc done: {updated} rows updated.")
    return df

# ─────────────────────────────────────────────────────────────────
#  メインオーケストレーター
# ─────────────────────────────────────────────────────────────────

def run(target_date: date, test_mode: bool = False, do_recalc: bool = False):
    logger.info(f"=== MACRO PULSE v4 | {target_date} | test={test_mode} | recalc={do_recalc} ===")

    ensure_schedule_csv()
    fred     = get_fred()
    schedule = load_schedule()
    existing = load_csv()

    # ── 再計算モード ────────────────────────────────────────────
    if do_recalc:
        logger.info("=== RECALC MODE ===")
        updated = recalc(existing)
        save_csv(updated)
        logger.info("=== Recalc complete ===")
        return

    # ── 市場データ ──────────────────────────────────────────────
    sp500, nasdaq, yield_curve, sp500_prev = get_market(target_date, fred)
    logger.info(f"Market: SP={sp500} NQ={nasdaq} YC={yield_curve} SP_prev={sp500_prev}")

    # ── 発表予定指標（スケジュール CSV 起点） ───────────────────
    scheduled = get_scheduled_for(target_date, schedule)
    logger.info(f"Scheduled: {[s['指標名'] for s in scheduled]}")

    new_rows = []

    if not scheduled:
        logger.info("No indicators scheduled. Recording market data only.")
        r = {col: float("nan") for col in CSV_COLUMNS}
        r.update({
            "指標名": "No Indicators",
            "リリース日": target_date.strftime("%Y-%m-%d"),
            "S&P500": sp500, "Nasdaq": nasdaq,
            "10Y-2Y(YieldCurve)": yield_curve,
            "付随データ": "{}",
            "forecast_source": "none",
            "市場反応(自動生成)": market_reaction(float("nan"), "none", sp500, sp500_prev),
            "データソース": "FRED/stooq",
            "更新日時": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        new_rows.append(r)
    else:
        for sched in scheduled:
            ind = sched["指標名"]

            # schedule.csv の actual 列（ISM 等 FRED 未収録向け手入力）
            override = None
            raw_actual = str(sched.get("actual", "")).strip()
            if raw_actual and raw_actual.lower() not in ("", "nan"):
                try:
                    override = float(raw_actual)
                    logger.info(f"[{ind}] schedule override actual: {override}")
                except ValueError:
                    pass

            try:
                row = fetch_indicator(
                    ind, target_date, fred,
                    sp500, nasdaq, yield_curve, sp500_prev,
                    existing, override_actual=override,
                )
                new_rows.append(row)
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"[{ind}]: {e}\n{traceback.format_exc()}")

    # ── Yield Curve 行（毎日必ず記録） ─────────────────────────
    yc_row = {col: float("nan") for col in CSV_COLUMNS}
    yc_row.update({
        "指標名": "Yield Curve 10Y-2Y",
        "リリース日": target_date.strftime("%Y-%m-%d"),
        "実際値": yield_curve,
        "S&P500": sp500, "Nasdaq": nasdaq,
        "10Y-2Y(YieldCurve)": yield_curve,
        "付随データ": "{}",
        "forecast_source": "none",
        "市場反応(自動生成)": "Daily yield curve record",
        "データソース": "FRED T10Y2Y",
        "更新日時": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    new_rows.append(yc_row)

    # ── CSV upsert ──────────────────────────────────────────────
    new_df = pd.DataFrame(new_rows, columns=CSV_COLUMNS)
    combined = (pd.concat([existing, new_df], ignore_index=True)
                if not existing.empty else new_df)
    save_csv(combined)
    logger.info("=== Run complete ===")

# ─────────────────────────────────────────────────────────────────
#  Entry Point
# ─────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="MACRO PULSE Economic Indicators v4")
    p.add_argument("--test",   action="store_true", help="Test mode")
    p.add_argument("--recalc", action="store_true",
                   help="Recalculate Surprise for all rows where forecast was updated")
    p.add_argument("--date", type=str, default=None,
                   help="Target date YYYY-MM-DD (default: yesterday)")
    args = p.parse_args()
    target = (datetime.strptime(args.date, "%Y-%m-%d").date()
              if args.date else (datetime.now() - timedelta(days=1)).date())
    run(target, test_mode=args.test, do_recalc=args.recalc)


if __name__ == "__main__":
    main()
