#!/usr/bin/env python3
"""
Economic Indicators Auto-Update System  v3.0

v3 変更点 (根本修正):
  [最重要] yfinance → FRED + stooq.com に完全置換
    - S&P500: FRED SP500 series (daily, no rate limit)
    - Nasdaq:  stooq.com CSV (無料・制限なし)
    - yfinance は前日比計算用prev_closeのみ保持し、失敗時はFREDで代替
  [FMP] v3/v4 両方403の場合 → FRED単独で完結するよう設計
  [FMP v3] 無料プランでは economic_calendar は未公開のため削除、
           代わりに FRED リリース日マッチングを主軸に変更
  [Node.js 20 警告] update.yml の actions を v4 に統一 (別ファイル)
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

CSV_PATH = "data/economic_history.csv"

CSV_COLUMNS = [
    "指標名", "リリース日", "実際値", "期待値(Consensus)", "前回値",
    "Surprise(実際-期待)", "YoY変化(%)", "S&P500", "Nasdaq",
    "10Y-2Y(YieldCurve)", "付随データ", "市場反応(自動生成)", "データソース", "更新日時",
]

INDICATOR_CONFIG = {
    "ISM Manufacturing PMI": {
        "fred_id": "",                     # ISMは非公開のためFRED未収録
        "companion_key": "Mfg Employment",
        "companion_fred": "MANEMP",
    },
    "New Residential Starts": {
        "fred_id": "HOUST",
        "companion_key": "Mortgage Rate 30Y",
        "companion_fred": "MORTGAGE30US",
    },
    "Durable Goods Orders": {
        "fred_id": "DGORDER",
        "companion_key": "Durable Ex-Transport",
        "companion_fred": "ADXTNO",
    },
    "Initial Jobless Claims": {
        "fred_id": "ICSA",
        "companion_key": "4W Moving Avg",
        "companion_fred": "IC4WSA",
    },
    "Average Hourly Earnings YoY": {
        "fred_id": "AHETPI",
        "companion_key": "CPI (CPIAUCSL)",
        "companion_fred": "CPIAUCSL",
    },
    "Michigan Consumer Sentiment": {
        "fred_id": "UMCSENT",
        "companion_key": "Michigan 1Y Inflation Exp",
        "companion_fred": "MICH",
    },
}

FMP_KEYWORDS = {
    "ISM Manufacturing PMI":       ["ism manufacturing", "manufacturing pmi"],
    "New Residential Starts":      ["housing starts", "residential starts"],
    "Durable Goods Orders":        ["durable goods"],
    "Initial Jobless Claims":      ["initial jobless", "initial claims"],
    "Average Hourly Earnings YoY": ["average hourly earnings"],
    "Michigan Consumer Sentiment": ["michigan consumer sentiment", "consumer sentiment"],
}

# ─── FRED Client ──────────────────────────────────────────────────────────────

def get_fred_client():
    try:
        from fredapi import Fred
        key = os.environ.get("FRED_API_KEY", "")
        if not key:
            logger.warning("FRED_API_KEY not set — market data will be limited.")
            return None
        return Fred(api_key=key)
    except ImportError:
        logger.warning("fredapi not installed.")
        return None

def get_fmp_api_key():
    return os.environ.get("FMP_API_KEY", "")

# ─── FRED helpers ─────────────────────────────────────────────────────────────

def fred_get_latest(fred, series_id, target_date, lookback=60):
    """最新観測値を返す。(value, release_date) or (None, None)"""
    try:
        end   = target_date.strftime("%Y-%m-%d")
        start = (target_date - timedelta(days=lookback)).strftime("%Y-%m-%d")
        s = fred.get_series(series_id, observation_start=start, observation_end=end)
        if s is None or s.empty: return None, None
        s = s.dropna()
        if s.empty: return None, None
        return float(s.iloc[-1]), s.index[-1].date()
    except Exception as e:
        logger.warning(f"FRED [{series_id}]: {e}")
        return None, None

def fred_get_previous(fred, series_id, current_date):
    try:
        end   = (current_date - timedelta(days=1)).strftime("%Y-%m-%d")
        start = (current_date - timedelta(days=90)).strftime("%Y-%m-%d")
        s = fred.get_series(series_id, observation_start=start, observation_end=end)
        if s is None or s.empty: return None
        s = s.dropna()
        return float(s.iloc[-1]) if not s.empty else None
    except Exception as e:
        logger.warning(f"FRED [{series_id}] prev: {e}")
        return None

def fred_get_yoy(fred, series_id, release_date, current_val):
    try:
        yr_ago = release_date - timedelta(days=365)
        s = fred.get_series(series_id,
            observation_start=(yr_ago - timedelta(days=45)).strftime("%Y-%m-%d"),
            observation_end  =(yr_ago + timedelta(days=45)).strftime("%Y-%m-%d"))
        if s is None or s.empty: return None
        s = s.dropna()
        if s.empty: return None
        v = float(s.iloc[-1])
        return round((current_val - v) / abs(v) * 100, 4) if v != 0 else None
    except Exception as e:
        logger.warning(f"FRED [{series_id}] YoY: {e}")
        return None

# ─── 株価取得: FRED (SP500) + stooq.com (Nasdaq) ─────────────────────────────
# yfinance はCI環境でレートリミットが回避不能なため廃止

def get_sp500_via_fred(fred, target_date):
    """
    FRED SP500 series (daily closing price, 1週間lookback).
    FREDのSP500シリーズはS&P500終値を収録。
    """
    if fred is None:
        return None, None
    try:
        val, obs_date = fred_get_latest(fred, "SP500", target_date, lookback=10)
        if val is not None:
            logger.info(f"S&P500 (FRED SP500): {val} @ {obs_date}")
            return val, obs_date
    except Exception as e:
        logger.warning(f"FRED SP500: {e}")
    return None, None

def get_sp500_prev_via_fred(fred, prev_date):
    """前日終値をFREDから取得"""
    if fred is None:
        return None
    val, _ = get_sp500_via_fred(fred, prev_date)
    return val

def get_nasdaq_via_stooq(target_date):
    """
    stooq.com の CSV API でNasdaq総合指数 (^NDX) を取得。
    URL例: https://stooq.com/q/d/l/?s=^ndx&d1=20260301&d2=20260311&i=d
    レートリミットなし、認証不要。
    """
    try:
        d1 = (target_date - timedelta(days=10)).strftime("%Y%m%d")
        d2 = target_date.strftime("%Y%m%d")
        url = f"https://stooq.com/q/d/l/?s=%5Endx&d1={d1}&d2={d2}&i=d"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        text = resp.text.strip()
        if not text or "No data" in text or len(text) < 20:
            logger.warning("stooq Nasdaq: no data returned")
            return None
        df = pd.read_csv(StringIO(text))
        df.columns = [c.strip() for c in df.columns]
        if "Close" not in df.columns or df.empty:
            logger.warning(f"stooq Nasdaq: unexpected columns: {df.columns.tolist()}")
            return None
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.sort_values("Date")
        val = round(float(df["Close"].iloc[-1]), 2)
        logger.info(f"Nasdaq (stooq ^NDX): {val}")
        return val
    except Exception as e:
        logger.warning(f"stooq Nasdaq: {e}")
    return None

def get_sp500_via_stooq(target_date):
    """
    FRED SP500 が取得できない場合の最終フォールバック。
    stooq.com で ^SPX を取得。
    """
    try:
        d1 = (target_date - timedelta(days=10)).strftime("%Y%m%d")
        d2 = target_date.strftime("%Y%m%d")
        url = f"https://stooq.com/q/d/l/?s=%5Espx&d1={d1}&d2={d2}&i=d"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        text = resp.text.strip()
        if not text or "No data" in text:
            return None
        df = pd.read_csv(StringIO(text))
        df.columns = [c.strip() for c in df.columns]
        if "Close" not in df.columns or df.empty:
            return None
        df["Date"] = pd.to_datetime(df["Date"])
        val = round(float(df.sort_values("Date")["Close"].iloc[-1]), 2)
        logger.info(f"S&P500 (stooq ^SPX fallback): {val}")
        return val
    except Exception as e:
        logger.warning(f"stooq S&P500: {e}")
    return None

# ─── 市場データ統合取得 ────────────────────────────────────────────────────────

def get_market_data(target_date, fred=None):
    """
    S&P500: FRED SP500 → stooq ^SPX の順で試行
    Nasdaq:  stooq ^NDX
    Yield curve: FRED T10Y2Y
    """
    sp500 = nasdaq = yield_curve = float("nan")

    # S&P500
    sp_val, _ = get_sp500_via_fred(fred, target_date)
    if sp_val is None:
        sp_val = get_sp500_via_stooq(target_date)
    if sp_val is not None:
        sp500 = sp_val
    else:
        logger.warning("S&P500: all sources failed — CSV will show NaN.")

    # Nasdaq
    nq_val = get_nasdaq_via_stooq(target_date)
    if nq_val is not None:
        nasdaq = nq_val
    else:
        logger.warning("Nasdaq: stooq failed — CSV will show NaN.")

    # Yield curve: FRED T10Y2Y (日次更新)
    if fred:
        yc, _ = fred_get_latest(fred, "T10Y2Y", target_date)
        if yc is not None:
            yield_curve = round(yc, 4)
            logger.info(f"Yield curve (FRED T10Y2Y): {yield_curve}")
        else:
            logger.warning("FRED T10Y2Y unavailable.")

    return sp500, nasdaq, yield_curve

def get_prev_close(target_date, fred=None):
    """前日S&P500終値: FRED → stooq の順で取得"""
    prev = target_date - timedelta(days=1)
    val = get_sp500_prev_via_fred(fred, prev)
    if val is None:
        val = get_sp500_via_stooq(prev)
    return val

# ─── FMP Economic Calendar (期待値コンセンサス取得用) ─────────────────────────

def fmp_get_economic_calendar(target_date, api_key):
    """
    FMP v3 は free plan で economic_calendar が非公開のため、
    v4 のみ試行。403なら諦めてFREDデータのみで運用。
    期待値が取れない日はSurpriseをNaNで記録するのみ。
    """
    if not api_key:
        return []
    date_str = target_date.strftime("%Y-%m-%d")
    # v4 (有料): 一度だけ試行
    try:
        url = (f"https://financialmodelingprep.com/api/v4/economic_calendar"
               f"?from={date_str}&to={date_str}&apikey={api_key}")
        time.sleep(0.5)
        resp = requests.get(url, timeout=20)
        if resp.status_code == 403:
            logger.info("FMP v4: 403 (free plan) — コンセンサス取得をスキップ。")
            return []
        if resp.status_code == 429:
            logger.warning("FMP v4: 429 rate limited.")
            return []
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            logger.info(f"FMP v4 events: {len(data)}")
            return data
    except Exception as e:
        logger.warning(f"FMP v4: {e}")
    return []

def fmp_find_event(fmp_events, indicator_name):
    terms = FMP_KEYWORDS.get(indicator_name, [indicator_name.lower()])
    for ev in fmp_events:
        name = str(ev.get("event", "") or ev.get("name", "")).lower()
        if any(t in name for t in terms):
            def _f(v):
                try: return float(v) if v not in (None, "", "null", "N/A") else None
                except: return None
            return (_f(ev.get("actual")),
                    _f(ev.get("estimate") or ev.get("consensus")),
                    _f(ev.get("previous")))
    return None, None, None

# ─── Investing.com (best-effort、失敗は想定内) ────────────────────────────────

def scrape_investing_calendar(target_date):
    """CI環境ではJS非実行のため通常失敗する。失敗は無音でスキップ。"""
    results = []
    try:
        time.sleep(1)
        resp = requests.get(
            "https://www.investing.com/economic-calendar/",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0"},
            timeout=15,
        )
        resp.raise_for_status()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", {"id": "economicCalendarData"})
        if not table:
            return results  # JS-rendered: expected failure in CI
        date_str = target_date.strftime("%Y/%m/%d")
        for row in table.find_all("tr", {"class": lambda c: c and "js-event-item" in c}):
            if date_str not in row.get("data-event-datetime", ""):
                continue
            def _td(cls):
                el = row.find("td", {"class": cls})
                return el.get_text(strip=True) if el else ""
            def _num(v):
                try: return float(v.replace(",", "").replace("%", "").strip())
                except: return None
            results.append({"name": _td("event"), "actual": _num(_td("actual")),
                            "forecast": _num(_td("forecast")), "previous": _num(_td("previous"))})
    except Exception:
        pass
    return results

def scrape_find_event(scraped, indicator_name):
    terms = FMP_KEYWORDS.get(indicator_name, [indicator_name.lower()])
    for item in scraped:
        if any(t in item.get("name", "").lower() for t in terms):
            return item.get("actual"), item.get("forecast"), item.get("previous")
    return None, None, None

# ─── コンセンサス取得 ─────────────────────────────────────────────────────────

def get_consensus(indicator_name, target_date, fmp_events, scraped):
    """FMP → scrape → NaN の順で期待値を返す。"""
    a, e, p = fmp_find_event(fmp_events, indicator_name)
    if e is not None or a is not None:
        logger.info(f"[{indicator_name}] Consensus←FMP: est={e}, actual={a}")
        return a, (e if e is not None else float("nan")), p, "FMP", ""

    a, e, p = scrape_find_event(scraped, indicator_name)
    if e is not None or a is not None:
        logger.info(f"[{indicator_name}] Consensus←Scrape: est={e}")
        return a, (e if e is not None else float("nan")), p, "Scraping", ""

    return None, float("nan"), None, "N/A", ""

# ─── 市場反応テキスト生成 ─────────────────────────────────────────────────────

def generate_market_reaction(surprise, sp500_today, sp500_prev):
    parts = []
    if pd.isna(surprise):
        parts.append("Surprise: N/A")
    elif surprise > 0:
        parts.append(f"Positive surprise (+{surprise:.2f})")
    elif surprise < 0:
        parts.append(f"Negative surprise ({surprise:.2f})")
    else:
        parts.append("In-line")

    if sp500_prev and not pd.isna(sp500_today) and sp500_today:
        chg = (sp500_today - sp500_prev) / sp500_prev * 100
        parts.append(f"S&P {'up' if chg>=0 else 'down'} {abs(chg):.2f}%")
    return "; ".join(parts)

# ─── Companion データ構築 ─────────────────────────────────────────────────────

def build_companion(indicator_name, fred, target_date, actual_val):
    config = INDICATOR_CONFIG.get(indicator_name, {})
    companion = {}
    ck, cf = config.get("companion_key"), config.get("companion_fred")
    if ck and cf and fred:
        v, _ = fred_get_latest(fred, cf, target_date)
        if v is not None:
            companion[ck] = v

    if indicator_name == "Average Hourly Earnings YoY" and fred:
        cpi, _ = fred_get_latest(fred, "CPIAUCSL", target_date)
        if cpi is not None and actual_val is not None and not pd.isna(actual_val):
            companion["Real Wage Diff (AHE-CPI)"] = round(actual_val - cpi, 4)

    if indicator_name == "Michigan Consumer Sentiment" and fred:
        m1, _ = fred_get_latest(fred, "MICH", target_date)
        if m1 is not None:
            companion["1Y Inflation Exp"] = m1

    return json.dumps(companion, ensure_ascii=False) if companion else "{}"

# ─── 指標1件フェッチ ──────────────────────────────────────────────────────────

def fetch_indicator(indicator_name, target_date, fred, fmp_events, scraped,
                    sp500, nasdaq, yield_curve, sp500_prev):
    row = {col: float("nan") for col in CSV_COLUMNS}
    row.update({
        "指標名": indicator_name,
        "リリース日": target_date.strftime("%Y-%m-%d"),
        "S&P500": sp500, "Nasdaq": nasdaq,
        "10Y-2Y(YieldCurve)": yield_curve,
        "更新日時": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })

    fred_id = INDICATOR_CONFIG.get(indicator_name, {}).get("fred_id", "")
    actual_val = prev_val = None
    release_date_used = target_date
    sources = []

    # ── FRED: 実際値・前回値 ──
    if fred and fred_id:
        for attempt in range(3):
            try:
                a, d = fred_get_latest(fred, fred_id, target_date)
                if a is not None:
                    actual_val = a
                    if d: release_date_used = d
                    prev_val = fred_get_previous(fred, fred_id, release_date_used)
                    sources.append("FRED")
                    logger.info(f"[{indicator_name}] FRED: actual={a}, prev={prev_val}, date={d}")
                break
            except Exception as e:
                logger.warning(f"[{indicator_name}] FRED attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)

    # ── Consensus (期待値) ──
    a_con, est, p_con, src, err = get_consensus(indicator_name, target_date, fmp_events, scraped)
    if src not in ("N/A", ""):
        sources.append(src)
    if actual_val is None and a_con is not None:
        actual_val = a_con
    if prev_val is None and p_con is not None:
        prev_val = p_con

    row["リリース日"]        = release_date_used.strftime("%Y-%m-%d")
    row["実際値"]            = actual_val if actual_val is not None else float("nan")
    row["期待値(Consensus)"] = est
    row["前回値"]            = prev_val if prev_val is not None else float("nan")

    if actual_val is not None and not pd.isna(est):
        row["Surprise(実際-期待)"] = round(actual_val - float(est), 4)

    if fred and fred_id and actual_val is not None:
        yoy = fred_get_yoy(fred, fred_id, release_date_used, actual_val)
        if yoy is not None:
            row["YoY変化(%)"] = yoy

    row["付随データ"]         = build_companion(indicator_name, fred, target_date, actual_val)
    row["市場反応(自動生成)"] = generate_market_reaction(
        row.get("Surprise(実際-期待)", float("nan")), sp500, sp500_prev)
    row["データソース"]       = ", ".join(sources) if sources else "N/A"
    return row

# ─── メインオーケストレーター ────────────────────────────────────────────────

def run(target_date, test_mode=False):
    logger.info(f"=== Starting run for {target_date} (test={test_mode}) ===")
    fred    = get_fred_client()
    fmp_key = get_fmp_api_key()

    # 市場データ取得 (yfinance不使用)
    sp500_prev           = get_prev_close(target_date, fred)
    sp500, nasdaq, yield_curve = get_market_data(target_date, fred)

    logger.info(f"Market: S&P500={sp500}, Nasdaq={nasdaq}, YieldCurve={yield_curve}, SP500_prev={sp500_prev}")

    # FMP カレンダー (有料プランのみ有効; 失敗時は空リスト)
    fmp_events = fmp_get_economic_calendar(target_date, fmp_key) if fmp_key else []

    # スクレイピング (best-effort)
    scraped = scrape_investing_calendar(target_date) if not test_mode else []
    if scraped:
        logger.info(f"Scraped events: {len(scraped)}")

    # ── 発表済み指標の特定 ──
    released = []

    # FMP / scrape 結果から
    for ind in INDICATOR_CONFIG:
        a_f, e_f, _ = fmp_find_event(fmp_events, ind)
        a_s, e_s, _ = scrape_find_event(scraped, ind)
        if any(v is not None for v in (a_f, e_f, a_s, e_s)):
            released.append(ind)

    # FRED リリース日マッチング (FMP/scrapeが空の場合のメイン手段)
    if fred:
        for ind, cfg in INDICATOR_CONFIG.items():
            if ind in released:
                continue
            fid = cfg.get("fred_id", "")
            if not fid:
                continue
            _, a_date = fred_get_latest(fred, fid, target_date)
            if a_date and a_date == target_date:
                released.append(ind)
                logger.info(f"[{ind}] FRED release date match → included")

    # ── 行生成 ──
    new_rows = []

    if not released:
        logger.info("No indicators released today. Recording market data only.")
        row = {col: float("nan") for col in CSV_COLUMNS}
        row.update({
            "指標名": "No Indicators",
            "リリース日": target_date.strftime("%Y-%m-%d"),
            "S&P500": sp500, "Nasdaq": nasdaq,
            "10Y-2Y(YieldCurve)": yield_curve,
            "付随データ": "{}",
            "市場反応(自動生成)": generate_market_reaction(float("nan"), sp500, sp500_prev),
            "データソース": "FRED/stooq",
            "更新日時": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        new_rows.append(row)
    else:
        logger.info(f"Released indicators: {released}")
        for ind in released:
            try:
                row = fetch_indicator(ind, target_date, fred, fmp_events,
                                      scraped, sp500, nasdaq, yield_curve, sp500_prev)
                new_rows.append(row)
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"[{ind}]: {e}\n{traceback.format_exc()}")

    # 毎日記録: イールドカーブ行
    yc_row = {col: float("nan") for col in CSV_COLUMNS}
    yc_row.update({
        "指標名": "Yield Curve 10Y-2Y",
        "リリース日": target_date.strftime("%Y-%m-%d"),
        "実際値": yield_curve, "S&P500": sp500, "Nasdaq": nasdaq,
        "10Y-2Y(YieldCurve)": yield_curve, "付随データ": "{}",
        "市場反応(自動生成)": "Daily yield curve record",
        "データソース": "FRED T10Y2Y",
        "更新日時": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    new_rows.append(yc_row)

    # ── CSV upsert ──
    os.makedirs("data", exist_ok=True)
    new_df = pd.DataFrame(new_rows, columns=CSV_COLUMNS)

    if os.path.exists(CSV_PATH):
        try:
            existing = pd.read_csv(CSV_PATH, encoding="utf-8")
            for c in CSV_COLUMNS:
                if c not in existing.columns:
                    existing[c] = float("nan")
            combined = pd.concat([existing, new_df], ignore_index=True)
        except Exception as e:
            logger.warning(f"CSV read error: {e}. Starting fresh.")
            combined = new_df
    else:
        combined = new_df

    combined = combined.drop_duplicates(subset=["リリース日", "指標名"], keep="last")
    combined = combined.sort_values(["リリース日", "指標名"]).reset_index(drop=True)
    combined.to_csv(CSV_PATH, index=False, encoding="utf-8")
    logger.info(f"CSV updated: {CSV_PATH} ({len(combined)} total rows)")
    logger.info("=== Run complete ===")

# ─── Entry Point ─────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Economic Indicators Auto-Update v3")
    p.add_argument("--test", action="store_true", help="Test mode: skip scraping")
    p.add_argument("--date", type=str, default=None, help="Target date YYYY-MM-DD (default: yesterday)")
    args = p.parse_args()
    target = (datetime.strptime(args.date, "%Y-%m-%d").date() if args.date
              else (datetime.now() - timedelta(days=1)).date())
    run(target, test_mode=args.test)

if __name__ == "__main__":
    main()
