#!/usr/bin/env python3
"""
MACRO PULSE — Economic Indicators Auto-Update  v5.0
====================================================
変更点 (v4.0 → v4.1):
  [スケジュール自動更新]
    --update-schedule フラグ（毎週日曜に実行）:
      1. FRED Release Calendar API で今後60日分の発表予定日を取得
         対象: Initial Jobless Claims / New Residential Starts /
               Durable Goods Orders / Average Hourly Earnings YoY /
               Michigan Consumer Sentiment
      2. ISM Manufacturing PMI（製造業）は米国営業日計算で発表予定日を内部算出:
           製造業 = 毎月第1営業日（米国祝日除く）
         ※ 非製造業（サービス業）は監視対象外
      3. 未登録の行のみ 05_indicator_schedule.csv に追記（既存行は変更しない）

変更点 (v3 → v4.0):
  [発表検知]  FRED observation_date マッチング → schedule.csv 起点に変更
  [期待値]    actual_as_forecast フォールバック実装
  [再計算]    --recalc フラグ実装
  [列追加]    forecast_source 列追加
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

CSV_PATH         = "data/05_economic_history.csv"
SCHEDULE_PATH    = "data/05_indicator_schedule.csv"
FED_CONTEXT_PATH = "data/05_fed_context.csv"

FED_CONTEXT_COLUMNS = [
    "record_date", "fomc_date", "regime",
    "dominant_concern", "dominant_label",
    "ff_current", "zq_ticker", "zq_price", "zq_rate",
    "cuts_implied", "ai_reason", "updated_at",
]

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
        "fred_release_id": None,      # FRED Release Calendar 対象外
        "ism_rule": "mfg",            # 米国第1営業日算出
        "companion_key":  "Mfg Employment",
        "companion_fred": "MANEMP",
        "threshold_bull": 50.0,
        "threshold_bear": 50.0,
        "unit": "index",
    },
    "New Residential Starts": {
        "fred_id": "HOUST",
        "fred_release_id": 235,
        "companion_key":  "Mortgage Rate 30Y",
        "companion_fred": "MORTGAGE30US",
        "threshold_bull": 1400.0,
        "threshold_bear": 1200.0,
        "unit": "千件",
    },
    "Durable Goods Orders": {
        "fred_id": "DGORDER",
        "fred_release_id": 110,
        "companion_key":  "Durable Ex-Transport",
        "companion_fred": "ADXTNO",
        "threshold_bull": 0.0,
        "threshold_bear": 0.0,
        "unit": "%MoM",
    },
    "Initial Jobless Claims": {
        "fred_id": "ICSA",
        "fred_release_id": 321,
        "companion_key":  "4W Moving Avg",
        "companion_fred": "IC4WSA",
        "threshold_bull": 250000.0,
        "threshold_bear": 300000.0,
        "unit": "件",
    },
    "Average Hourly Earnings YoY": {
        "fred_id": "AHETPI",
        "fred_release_id": 50,
        "companion_key":  "CPI (CPIAUCSL)",
        "companion_fred": "CPIAUCSL",
        "threshold_bull": 0.0,
        "threshold_bear": 0.0,
        "unit": "%YoY",
    },
    "Michigan Consumer Sentiment": {
        "fred_id": "UMCSENT",
        "fred_release_id": 426,      # 426=速報(Preliminary) / 152=確報(Final)
        "fred_release_id_alt": 152,  # 確報もフォールバックで取得
        "companion_key":  "Michigan 1Y Inflation Exp",
        "companion_fred": "MICH",
        "threshold_bull": 80.0,
        "threshold_bear": 65.0,
        "unit": "index",
    },
}

# ─────────────────────────────────────────────────────────────────
#  米国祝日計算
# ─────────────────────────────────────────────────────────────────

def us_holidays(year: int) -> set:
    """
    米国の主要連邦祝日を返す（ISM 発表日計算に使用）。
    対象: 元日・MLK Day・大統領の日・メモリアルデー・独立記念日・
          レイバーデー・コロンバスデー・退役軍人の日・感謝祭・クリスマス
    """
    from datetime import date as d

    def nth_weekday(year, month, weekday, n):
        """第n weekday（0=月曜）を返す"""
        first = d(year, month, 1)
        delta = (weekday - first.weekday()) % 7
        return first + timedelta(days=delta + (n - 1) * 7)

    def last_weekday(year, month, weekday):
        """最終 weekday を返す"""
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        last = d(year, month, last_day)
        delta = (last.weekday() - weekday) % 7
        return last - timedelta(days=delta)

    holidays = set()

    # 元日 (1/1、土なら前金、日なら翌月)
    ny = d(year, 1, 1)
    if ny.weekday() == 5:   ny = d(year, 12, 31)   # 前年土曜→前金
    elif ny.weekday() == 6: ny = d(year, 1, 2)
    holidays.add(ny)

    holidays.add(nth_weekday(year, 1, 0, 3))   # MLK Day (1月第3月曜)
    holidays.add(nth_weekday(year, 2, 0, 3))   # Presidents Day (2月第3月曜)
    holidays.add(last_weekday(year, 5, 0))     # Memorial Day (5月最終月曜)

    # 独立記念日 (7/4)
    jul4 = d(year, 7, 4)
    if jul4.weekday() == 5:   jul4 = d(year, 7, 3)
    elif jul4.weekday() == 6: jul4 = d(year, 7, 5)
    holidays.add(jul4)

    holidays.add(nth_weekday(year, 9, 0, 1))   # Labor Day (9月第1月曜)
    holidays.add(nth_weekday(year, 10, 0, 2))  # Columbus Day (10月第2月曜)

    # 退役軍人の日 (11/11)
    nov11 = d(year, 11, 11)
    if nov11.weekday() == 5:   nov11 = d(year, 11, 10)
    elif nov11.weekday() == 6: nov11 = d(year, 11, 12)
    holidays.add(nov11)

    holidays.add(nth_weekday(year, 11, 3, 4))  # Thanksgiving (11月第4木曜)

    # クリスマス (12/25)
    xmas = d(year, 12, 25)
    if xmas.weekday() == 5:   xmas = d(year, 12, 24)
    elif xmas.weekday() == 6: xmas = d(year, 12, 26)
    holidays.add(xmas)

    return holidays


def nth_us_business_day(year: int, month: int, n: int) -> date:
    """
    指定年月の第n米国営業日（月〜金かつ祝日除く）を返す。
    n=1 → 第1営業日、n=3 → 第3営業日
    """
    holidays = us_holidays(year) | us_holidays(year - 1) | us_holidays(year + 1)
    count = 0
    d = date(year, month, 1)
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    while d <= date(year, month, last_day):
        if d.weekday() < 5 and d not in holidays:  # 月〜金 かつ 祝日でない
            count += 1
            if count == n:
                return d
        d += timedelta(days=1)
    raise ValueError(f"{year}-{month:02d} の第{n}営業日が見つかりません")


# ─────────────────────────────────────────────────────────────────
#  ISM 発表予定日の内部算出
# ─────────────────────────────────────────────────────────────────

def ism_release_dates(months_ahead: int = 3) -> list[tuple[str, date]]:
    """
    ISM 製造業（第1営業日）の今後 months_ahead ヶ月分の発表予定日リストを返す。
    非製造業（サービス業）は監視対象外。

    Returns: [(指標名, release_date), ...]
    """
    today = date.today()
    results = []
    for offset in range(months_ahead + 1):
        year  = today.year + (today.month - 1 + offset) // 12
        month = (today.month - 1 + offset) % 12 + 1
        try:
            mfg_date = nth_us_business_day(year, month, 1)  # 製造業: 第1営業日
            if mfg_date >= today:
                results.append(("ISM Manufacturing PMI", mfg_date))
        except ValueError as e:
            logger.warning(f"ISM date calc error: {e}")
    return results


# ─────────────────────────────────────────────────────────────────
#  FRED Release Calendar
# ─────────────────────────────────────────────────────────────────

def fred_release_dates(fred_api_key: str, days_ahead: int = 90) -> dict[str, list[date]]:
    """
    FRED Release Calendar API で今後 days_ahead 日分の発表予定日を取得。
    504 等のサーバーエラー時は最大3回リトライ（1秒・2秒・4秒待機）。
    Returns: {指標名: [date, ...]}
    """
    today    = date.today()
    end_date = today + timedelta(days=days_ahead)
    results  = {}

    for ind_name, cfg in INDICATOR_CONFIG.items():
        release_id = cfg.get("fred_release_id")
        if not release_id:
            continue  # ISM 系はスキップ（別途算出）

        # release_id_alt が設定されている場合は両方取得してマージ
        release_ids = [release_id]
        if cfg.get("fred_release_id_alt"):
            release_ids.append(cfg["fred_release_id_alt"])

        all_dates = []
        for rid in release_ids:
            url = (
                f"https://api.stlouisfed.org/fred/release/dates"
                f"?release_id={rid}"
                f"&realtime_start={today.strftime('%Y-%m-%d')}"
                f"&realtime_end={end_date.strftime('%Y-%m-%d')}"
                f"&include_release_dates_with_no_data=true"
                f"&api_key={fred_api_key}"
                f"&file_type=json"
            )
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    r = requests.get(url, timeout=20)
                    r.raise_for_status()
                    data  = r.json()
                    dates = [
                        datetime.strptime(d["date"], "%Y-%m-%d").date()
                        for d in data.get("release_dates", [])
                        if datetime.strptime(d["date"], "%Y-%m-%d").date() >= today
                    ]
                    all_dates.extend(dates)
                    time.sleep(0.3)
                    break
                except Exception as e:
                    wait = 2 ** attempt
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"[FRED Release] {ind_name} (id={rid}) "
                            f"attempt {attempt + 1}/{max_retries}: {e} → retry in {wait}s"
                        )
                        time.sleep(wait)
                    else:
                        logger.warning(
                            f"[FRED Release] {ind_name} (id={rid}) "
                            f"failed after {max_retries} attempts: {e}"
                        )

        # 重複排除・ソート
        unique_dates = sorted(set(all_dates))
        results[ind_name] = unique_dates
        logger.info(f"[FRED Release] {ind_name}: {[str(d) for d in unique_dates]}")


# ─────────────────────────────────────────────────────────────────
#  スケジュール CSV 自動更新
# ─────────────────────────────────────────────────────────────────

def update_schedule(fred_api_key: str, days_ahead: int = 90):
    """
    毎週日曜に実行。
    1. FRED Release Calendar で FRED 収録指標の発表予定日を取得
    2. ISM 製造業・非製造業は営業日計算で算出
    3. 未登録の行のみ schedule.csv に追記（既存行は一切変更しない）
    """
    ensure_schedule_csv()
    df = load_schedule()

    # 既登録済みのキー集合: (指標名, 発表予定日)
    registered = set(zip(df["指標名"], df["発表予定日"]))

    new_rows = []

    # ── FRED Release Calendar ──────────────────────────────────
    fred_dates = fred_release_dates(fred_api_key, days_ahead)
    for ind_name, dates in fred_dates.items():
        cfg = INDICATOR_CONFIG.get(ind_name, {})
        for release_date in dates:
            date_str = release_date.strftime("%Y-%m-%d")
            if (ind_name, date_str) in registered:
                continue  # 既登録済みはスキップ
            new_rows.append({
                "指標名":    ind_name,
                "発表予定日": date_str,
                "fred_id":  cfg.get("fred_id", ""),
                "閾値_強気": cfg.get("threshold_bull", ""),
                "閾値_弱気": cfg.get("threshold_bear", ""),
                "単位":      cfg.get("unit", ""),
                "actual":   "",
                "備考":      "FRED Release Calendar 自動取得",
            })
            logger.info(f"[Schedule+] {ind_name}: {date_str} (FRED)")

    # ── ISM 製造業・非製造業（営業日計算） ──────────────────────
    for ind_name, release_date in ism_release_dates(months_ahead=3):
        cfg      = INDICATOR_CONFIG.get(ind_name, {})
        date_str = release_date.strftime("%Y-%m-%d")
        rule     = cfg.get("ism_rule", "")
        note     = "製造業: 米国第1営業日 自動算出"
        if (ind_name, date_str) in registered:
            continue
        new_rows.append({
            "指標名":    ind_name,
            "発表予定日": date_str,
            "fred_id":  "",
            "閾値_強気": cfg.get("threshold_bull", ""),
            "閾値_弱気": cfg.get("threshold_bear", ""),
            "単位":      cfg.get("unit", ""),
            "actual":   "",
            "備考":      note,
        })
        logger.info(f"[Schedule+] {ind_name}: {date_str} ({note})")

    if not new_rows:
        logger.info("Schedule up to date. No new rows added.")
        return

    # 追記して保存（既存行 + 新規行、日付順にソート）
    new_df = pd.DataFrame(new_rows, columns=SCHEDULE_COLUMNS)
    frames = [f for f in [df, new_df] if not f.empty]
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=SCHEDULE_COLUMNS)
    combined = combined.sort_values(["発表予定日", "指標名"]).reset_index(drop=True)
    combined.to_csv(SCHEDULE_PATH, index=False, encoding="utf-8")
    logger.info(f"Schedule updated: {len(new_rows)} rows added → {SCHEDULE_PATH}")


def ensure_schedule_csv():
    """schedule.csv が存在しなければテンプレートを生成する。"""
    os.makedirs("data", exist_ok=True)
    if os.path.exists(SCHEDULE_PATH):
        return
    rows = []
    for name, cfg in INDICATOR_CONFIG.items():
        rows.append({
            "指標名":    name,
            "発表予定日": "",
            "fred_id":  cfg.get("fred_id", ""),
            "閾値_強気": cfg.get("threshold_bull", ""),
            "閾値_弱気": cfg.get("threshold_bear", ""),
            "単位":      cfg.get("unit", ""),
            "actual":   "",
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


# ─────────────────────────────────────────────────────────────────
#  ZQ先物 (FF Funds Futures) — 12ヶ月先限月から期待FF金利を算出
# ─────────────────────────────────────────────────────────────────

ZQ_MONTH_CODE = {1:'F',2:'G',3:'H',4:'J',5:'K',6:'M',
                 7:'N',8:'Q',9:'U',10:'V',11:'X',12:'Z'}

def get_zq_futures(target_date: date, fred=None):
    """
    1年後の市場期待FF金利を算出し (ticker, price, implied_rate) を返す。

    算出方法:
      FRED T1YFF（1年物国債利回り − FF金利スプレッド）を使用。
        implied_rate = ff_current + T1YFF
      ※ T1YFF > 0 なら市場は1年後に金利上昇を織り込み（TIGHTENING方向）
        T1YFF < 0 なら市場は1年後に金利低下を織り込み（EASING方向）

    ticker欄には "FRED:T1YFF" を記録。
    price欄には T1YFF の値（スプレッド）を記録。
    失敗時は (None, None, None)
    """
    if fred is None:
        logger.warning("T1YFF: fred client unavailable")
        return None, None, None

    # T1YFF: 1年物国債利回り − FF実効金利
    t1yff, obs_date = fred_latest(fred, "T1YFF", target_date, lookback=30)
    if t1yff is None:
        logger.warning("T1YFF: could not retrieve from FRED")
        return None, None, None

    # FF現在値
    ff_current = get_ff_current(fred)
    if ff_current is None:
        logger.warning("T1YFF: FF current rate unavailable, cannot compute implied rate")
        return None, None, None

    implied_rate = round(ff_current + t1yff, 4)
    logger.info(
        f"T1YFF (FRED): {t1yff:+.4f}% (obs={obs_date}) | "
        f"FF current: {ff_current}% → implied 1Y FF: {implied_rate}%"
    )
    # cuts_implied: (ff_current - implied_rate) / 0.25
    cuts = round((ff_current - implied_rate) / 0.25, 2)
    logger.info(f"Implied cuts in 12M: {cuts:+.2f} 回 (25bp each)")

    # price欄 = T1YFFスプレッド値（ZQ価格の代替として記録）
    return "FRED:T1YFF", round(t1yff, 4), implied_rate


def get_ff_current(fred):
    """現在のFF金利誘導目標中心値をFREDから取得。"""
    if fred is None:
        return None
    # DFEDTARU = FF誘導目標上限, DFEDTARL = 下限
    v_hi, _ = fred_latest(fred, "DFEDTARU", date.today(), lookback=30)
    v_lo, _ = fred_latest(fred, "DFEDTARL", date.today(), lookback=30)
    if v_hi is not None and v_lo is not None:
        center = round((v_hi + v_lo) / 2, 4)
        logger.info(f"FF rate: {v_lo}~{v_hi}% → center {center}%")
        return center
    # フォールバック: FEDFUNDS（実効値）
    v, _ = fred_latest(fred, "FEDFUNDS", date.today(), lookback=45)
    if v is not None:
        logger.info(f"FF rate (FEDFUNDS): {v}%")
        return round(v, 4)
    return None


# ─────────────────────────────────────────────────────────────────
#  Gemini FOMC分析
# ─────────────────────────────────────────────────────────────────

def fetch_latest_fomc_statement():
    """
    FRBサイトから最新FOMC声明テキストを取得。失敗時は (None, None)。

    FRBの声明URLの正規パス:
      /newsevents/pressreleases/monetary{YYYYMMDD}a.htm
    カレンダーページで発見できない場合は既知FOMC日程から直接アクセス。
    """
    import re

    # ── Step1: カレンダーページから正規パスを走査 ─────────────────
    cal_url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    found_url = None
    fomc_date = None

    try:
        r = requests.get(cal_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        r.raise_for_status()
        html = r.text

        # 正規パスを優先順に探す
        pats = [
            r'href="(/newsevents/pressreleases/monetary(\d{8})a\d?\.htm)"',
            r'href="(/monetarypolicy/(\d{8})a\d?\.htm)"',
            r'href="(/monetarypolicy/monetary(\d{8})a\d?\.htm)"',
        ]
        candidates = []
        for pat in pats:
            for path, dt in re.findall(pat, html):
                candidates.append((dt, path))

        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            best_dt, best_path = candidates[0]
            found_url = "https://www.federalreserve.gov" + best_path
            fomc_date = datetime.strptime(best_dt, "%Y%m%d").strftime("%Y-%m-%d")
            logger.info(f"FOMC statement URL found: {found_url}")

    except Exception as e:
        logger.warning(f"FOMC calendar fetch: {e}")

    # ── Step2: 見つからない場合は既知FOMC日程から直接アクセス ─────
    if not found_url:
        logger.info("FOMC URL not found via scraping. Trying known schedule.")
        known_fomc_dates = [
            "20260318", "20260129",
            "20251218", "20251107", "20250918", "20250730",
        ]
        from datetime import date as date_cls
        today_str = date_cls.today().strftime("%Y%m%d")
        past = sorted([d for d in known_fomc_dates if d <= today_str], reverse=True)
        for best_dt in past:
            url_candidates = [
                f"https://www.federalreserve.gov/newsevents/pressreleases/monetary{best_dt}a.htm",
                f"https://www.federalreserve.gov/monetarypolicy/{best_dt}a1.htm",
            ]
            for u in url_candidates:
                try:
                    r = requests.get(u, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
                    if r.status_code == 200:
                        found_url = u
                        fomc_date = datetime.strptime(best_dt, "%Y%m%d").strftime("%Y-%m-%d")
                        logger.info(f"FOMC statement URL (known schedule): {found_url}")
                        break
                except Exception as e:
                    logger.warning(f"FOMC known URL [{u}]: {e}")
            if found_url:
                break

    if not found_url:
        logger.warning("FOMC statement: all URL strategies failed.")
        return None, None

    # ── Step3: 声明本文取得 ──────────────────────────────────────
    try:
        r2 = requests.get(found_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        r2.raise_for_status()

        text = re.sub(r'<[^>]+>', ' ', r2.text)
        text = re.sub(r'\s+', ' ', text).strip()

        start = -1
        for marker in [
            "Recent indicators", "Recent economic indicators",
            "The Federal Open Market Committee",
            "Information received since",
            "Labor market conditions", "Economic activity",
        ]:
            idx = text.find(marker)
            if idx != -1:
                start = idx
                break

        stmt_text = text[start:start+3000] if start != -1 else text[500:3500]
        logger.info(f"FOMC statement fetched: {fomc_date} ({len(stmt_text)} chars)")
        return fomc_date, stmt_text

    except Exception as e:
        logger.warning(f"FOMC statement body fetch failed: {e}")
        return None, None




def analyze_fomc_with_gemini(fomc_date: str, stmt_text: str, ff_current: float,
                              zq_rate: float, cuts_implied: float) -> dict:
    """
    Gemini APIでFOMC声明を分析し dominant_concern / regime / reason を返す。
    Returns dict with keys: regime, dominant_concern, dominant_label, ai_reason
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        logger.warning("GEMINI_API_KEY not set. Skipping AI analysis.")
        return _fallback_regime(ff_current, zq_rate, cuts_implied)

    prompt = f"""You are a Federal Reserve policy analyst. Analyze the following FOMC statement and market data.

FOMC Statement ({fomc_date}):
{stmt_text}

Market Context:
- Current FF Rate: {ff_current}%
- 12-month ahead FF futures implied rate: {zq_rate}%
- Market-implied rate changes in 12M: {cuts_implied:+.1f} cuts (25bp each)

Based on this analysis, determine:

1. REGIME: Is the Fed in EASING (cutting rates), TIGHTENING (raising rates), or BALANCED (on hold) mode?
2. DOMINANT_CONCERN: What is the Fed's primary concern right now?
   - INFLATION_FOCUS: Inflation is the primary concern
   - EMPLOYMENT_FOCUS: Labor market/employment is the primary concern  
   - BALANCED: Both are equally weighted

Respond ONLY in this exact JSON format (no markdown, no extra text):
{{"regime":"EASING","dominant_concern":"EMPLOYMENT_FOCUS","dominant_label":"雇用重視","ai_reason":"日本語で100字以内で判断理由を記載。声明の具体的な文言に言及すること。"}}"""

    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
        payload = {"contents": [{"parts": [{"text": prompt}]}],
                   "generationConfig": {"temperature": 0.1, "maxOutputTokens": 300}}
        r = requests.post(url, json=payload,
                          headers={"Content-Type": "application/json"}, timeout=30)
        r.raise_for_status()
        data = r.json()
        raw = data["candidates"][0]["content"]["parts"][0]["text"].strip()
        # JSON抽出
        import re
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            result = json.loads(m.group())
            logger.info(f"Gemini analysis: {result}")
            return result
        logger.warning(f"Gemini response parse failed: {raw}")
    except Exception as e:
        logger.warning(f"Gemini API error: {e}")

    return _fallback_regime(ff_current, zq_rate, cuts_implied)


def _fallback_regime(ff_current, zq_rate, cuts_implied):
    """Gemini失敗時のルールベースフォールバック。"""
    if cuts_implied is None:
        return {"regime":"BALANCED","dominant_concern":"BALANCED",
                "dominant_label":"両睨み","ai_reason":"データ取得失敗のためルールベース判定。"}
    if cuts_implied >= 1.0:
        return {"regime":"EASING","dominant_concern":"EMPLOYMENT_FOCUS",
                "dominant_label":"雇用重視",
                "ai_reason":f"ZQ先物が{cuts_implied:.1f}回の利下げを織り込み。EASING局面と判定（AI分析なし）。"}
    elif cuts_implied <= -1.0:
        return {"regime":"TIGHTENING","dominant_concern":"INFLATION_FOCUS",
                "dominant_label":"インフレ警戒",
                "ai_reason":f"ZQ先物が{abs(cuts_implied):.1f}回の利上げを織り込み。TIGHTENING局面と判定（AI分析なし）。"}
    else:
        return {"regime":"BALANCED","dominant_concern":"BALANCED",
                "dominant_label":"両睨み",
                "ai_reason":f"ZQ先物の織り込みが{cuts_implied:+.1f}回でBALANCED局面と判定（AI分析なし）。"}


# ─────────────────────────────────────────────────────────────────
#  fed_context.csv 更新
# ─────────────────────────────────────────────────────────────────

def update_fed_context(target_date: date, fred):
    """
    毎週日曜（update-scheduleジョブ）に呼び出す。
    ZQ先物・FF金利・Gemini FOMC分析を実行し05_fed_context.csvに記録。
    """
    logger.info("=== Updating Fed Context ===")

    # 既存CSV読み込み
    if os.path.exists(FED_CONTEXT_PATH):
        ctx_df = pd.read_csv(FED_CONTEXT_PATH, dtype=str)
    else:
        ctx_df = pd.DataFrame(columns=FED_CONTEXT_COLUMNS)

    # T1YFF ベース期待FF金利（fredを渡す）
    zq_ticker, zq_price, zq_rate = get_zq_futures(target_date, fred)

    # FF金利（get_zq_futures内で既に取得済みだがCSV記録用に再取得）
    ff_current = get_ff_current(fred)
    if ff_current is None:
        logger.warning("FF rate unavailable, using 4.375 as fallback")
        ff_current = 4.375

    # 利下げ回数計算（implied_rate = zq_rate が取得できた場合のみ）
    cuts_implied = None
    if zq_rate is not None and ff_current is not None:
        cuts_implied = round((ff_current - zq_rate) / 0.25, 2)
        logger.info(f"Cuts implied: {cuts_implied:+.2f} 回 (FF={ff_current}% → 1Y expected={zq_rate}%)")

    # FOMC声明取得 & Gemini分析（月1回：当月まだ分析していなければ実行）
    record_month = target_date.strftime("%Y-%m")
    already_analyzed = (
        not ctx_df.empty and
        "record_date" in ctx_df.columns and
        ctx_df["record_date"].str.startswith(record_month).any()
    )

    if already_analyzed:
        logger.info(f"Fed context already recorded for {record_month}. ZQ/FF update only.")
        # ZQ・FF金利だけ更新（最新行を更新）
        last_idx = ctx_df[ctx_df["record_date"].str.startswith(record_month)].index[-1]
        ctx_df.loc[last_idx, "zq_ticker"]  = zq_ticker or ""
        ctx_df.loc[last_idx, "zq_price"]   = str(zq_price or "")
        ctx_df.loc[last_idx, "zq_rate"]    = str(zq_rate or "")
        ctx_df.loc[last_idx, "ff_current"] = str(ff_current)
        ctx_df.loc[last_idx, "cuts_implied"] = str(cuts_implied or "")
        ctx_df.loc[last_idx, "updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    else:
        # Gemini FOMC分析実行
        fomc_date, stmt_text = fetch_latest_fomc_statement()
        if stmt_text:
            analysis = analyze_fomc_with_gemini(
                fomc_date or target_date.strftime("%Y-%m-%d"),
                stmt_text, ff_current, zq_rate or ff_current, cuts_implied or 0)
        else:
            logger.warning("FOMC statement unavailable. Using fallback.")
            analysis = _fallback_regime(ff_current, zq_rate, cuts_implied)
            fomc_date = target_date.strftime("%Y-%m-%d")

        new_row = {
            "record_date":      target_date.strftime("%Y-%m-%d"),
            "fomc_date":        fomc_date or "",
            "regime":           analysis.get("regime", "BALANCED"),
            "dominant_concern": analysis.get("dominant_concern", "BALANCED"),
            "dominant_label":   analysis.get("dominant_label", "両睨み"),
            "ff_current":       str(ff_current),
            "zq_ticker":        zq_ticker or "",
            "zq_price":         str(zq_price or ""),
            "zq_rate":          str(zq_rate or ""),
            "cuts_implied":     str(cuts_implied or ""),
            "ai_reason":        analysis.get("ai_reason", ""),
            "updated_at":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        new_df = pd.DataFrame([new_row])
        ctx_df = pd.concat([df for df in [ctx_df, new_df] if not df.empty], ignore_index=True)
        logger.info(f"Fed context new record: {new_row['regime']} / {new_row['dominant_concern']}")

    os.makedirs("data", exist_ok=True)
    ctx_df.to_csv(FED_CONTEXT_PATH, index=False, encoding="utf-8")
    logger.info(f"Fed context saved: {FED_CONTEXT_PATH} ({len(ctx_df)} rows)")


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

def run(target_date: date, test_mode: bool = False, do_recalc: bool = False,
        do_update_schedule: bool = False):
    logger.info(f"=== MACRO PULSE v5.0 | {target_date} | test={test_mode} | recalc={do_recalc} | update_schedule={do_update_schedule} ===")

    ensure_schedule_csv()
    fred     = get_fred()
    schedule = load_schedule()
    existing = load_csv()

    # ── スケジュール自動更新モード ──────────────────────────────
    if do_update_schedule:
        logger.info("=== UPDATE SCHEDULE MODE ===")
        fred_api_key = os.environ.get("FRED_API_KEY", "")
        if not fred_api_key:
            logger.error("FRED_API_KEY not set. Cannot update schedule.")
            sys.exit(1)
        update_schedule(fred_api_key)
        # Fed Context (ZQ先物 + Gemini FOMC分析) を更新
        update_fed_context(target_date, fred)
        logger.info("=== Schedule + Fed Context update complete ===")
        return

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

    # ── CSV upsert（同日付+同指標名は上書き、新規は追記） ───────
    new_df = pd.DataFrame(new_rows, columns=CSV_COLUMNS)
    if existing.empty:
        combined = new_df
    else:
        # (リリース日, 指標名) をキーに既存の重複行を除去してから結合
        key_new = set(zip(new_df["リリース日"], new_df["指標名"]))
        existing_filtered = existing[
            ~existing.apply(
                lambda r: (r["リリース日"], r["指標名"]) in key_new, axis=1
            )
        ]
        combined = pd.concat([existing_filtered, new_df], ignore_index=True)
    # リリース日昇順でソート
    combined = combined.sort_values("リリース日", kind="stable").reset_index(drop=True)
    save_csv(combined)
    logger.info("=== Run complete ===")

# ─────────────────────────────────────────────────────────────────
#  Entry Point
# ─────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="MACRO PULSE Economic Indicators v4.1")
    p.add_argument("--test",            action="store_true", help="Test mode")
    p.add_argument("--recalc",          action="store_true",
                   help="Recalculate Surprise for all rows where forecast was updated")
    p.add_argument("--update-schedule", action="store_true",
                   help="Auto-update indicator_schedule.csv via FRED Release Calendar + ISM rule")
    p.add_argument("--date", type=str, default=None,
                   help="Target date YYYY-MM-DD (default: yesterday)")
    args = p.parse_args()
    target = (datetime.strptime(args.date, "%Y-%m-%d").date()
              if args.date else (datetime.now() - timedelta(days=1)).date())
    run(target, test_mode=args.test, do_recalc=args.recalc,
        do_update_schedule=args.update_schedule)


if __name__ == "__main__":
    main()
