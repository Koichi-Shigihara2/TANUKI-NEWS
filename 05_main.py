#!/usr/bin/env python3
"""
MACRO PULSE — Economic Indicators Auto-Update  v6.0
====================================================
変更点 (v5.0 → v6.0):
  [スキーマ刷新]
    - 05_economic_history.csv → 05_events.csv に移行
    - イベント単位での記録（event_id = {indicator_slug}_{release_date}）
    - 金融環境スナップショット（regime, ff_rate, yc_10y2y, hy_spread, vix, cuts_implied）を同時保存
    - S&P500 t0/t1/t5/t10/t20 と変化率を後から自動補完

  [監視指標 12本体制]
    手入力:  ISM製造業PMI, ISM非製造業PMI
    自動取得(FRED): Conference Board LEI → OECD CLI (USALOLITONOSTSAM) で代替
    FRED自動: NFP, 失業保険4週MA, ミシガン1Y/5Yインフレ期待, CB消費者信頼感,
              住宅建築許可, 10Y-2Yカーブ, HYスプレッド, VIX

  [Discord リマインダー]
    --remind フラグ: 当日発表予定の手入力指標を Discord に通知

  [市場反応自動補完]
    --fill-returns フラグ: sp500_t1/t5/t10/t20 と ret_* を後から補完

  [後方互換]
    --update-schedule, --recalc は引き続き動作
"""

import os, sys, time, json, logging, argparse, traceback, re
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

# ─────────────────────────────────────────────────────────────────
#  パス定数
# ─────────────────────────────────────────────────────────────────
EVENTS_PATH      = "data/05_events.csv"
SCHEDULE_PATH    = "data/05_indicator_schedule.csv"
FED_CONTEXT_PATH = "data/05_fed_context.csv"

# ─────────────────────────────────────────────────────────────────
#  カラム定義
# ─────────────────────────────────────────────────────────────────
EVENTS_COLUMNS = [
    "event_id", "indicator", "release_date",
    "actual", "consensus", "surprise", "surprise_pct",
    "regime", "ff_rate", "yc_10y2y", "hy_spread", "vix", "cuts_implied",
    "sp500_t0", "sp500_t1", "sp500_t5", "sp500_t10", "sp500_t20",
    "ret_t1", "ret_t5", "ret_t10", "ret_t20",
    "forecast_source", "data_source", "analysis", "updated_at",
]

SCHEDULE_COLUMNS = [
    "indicator", "release_date", "fred_id", "input_method", "consensus", "actual", "status",
]

FED_CONTEXT_COLUMNS = [
    "record_date", "fomc_date", "regime",
    "dominant_concern", "dominant_label",
    "ff_current", "zq_ticker", "zq_price", "zq_rate",
    "cuts_implied", "ai_reason", "updated_at",
]

# ─────────────────────────────────────────────────────────────────
#  指標マスタ（v6.0 確定12指標）
# ─────────────────────────────────────────────────────────────────
INDICATOR_CONFIG = {
    # ── 手入力指標 ──────────────────────────────────────────────
    "ISM Manufacturing PMI": {
        "fred_id": "",
        "input_method": "manual",
        "fred_release_id": None,
        "ism_rule": "mfg",
        "slug": "ism_mfg_pmi",
        "threshold_bull": 50.0,
        "threshold_bear": 50.0,
        "unit": "index",
        "discord_remind": True,
    },
    "ISM Non-Manufacturing PMI": {
        "fred_id": "",
        "input_method": "manual",
        "fred_release_id": None,
        "ism_rule": "svc",
        "slug": "ism_svc_pmi",
        "threshold_bull": 50.0,
        "threshold_bear": 50.0,
        "unit": "index",
        "discord_remind": True,
    },
    "Conference Board LEI": {
        "fred_id": "USALOLITONOSTSAM",   # OECD CLI Normalized (FRED free API)
        "input_method": "FRED",
        "fred_release_id": None,          # リリースカレンダー不要（月次自動）
        "slug": "cb_lei",
        "threshold_bull": 100.1,          # OECD CLI: 100超=拡張、100未満=縮小
        "threshold_bear": 99.5,
        "unit": "index",                  # 正規化指数（100基準）
        "discord_remind": False,          # FRED自動取得のためリマインド不要
    },
    # ── FRED自動取得 ─────────────────────────────────────────────
    "NFP": {
        "fred_id": "PAYEMS",
        "input_method": "FRED",
        "fred_release_id": 50,
        "slug": "nfp",
        "threshold_bull": 200000,
        "threshold_bear": 100000,
        "unit": "千人",
        "discord_remind": False,
    },
    "Initial Claims 4W MA": {
        "fred_id": "IC4WSA",
        "input_method": "FRED",
        "fred_release_id": 321,
        "slug": "ic4wsa",
        "threshold_bull": 250000,
        "threshold_bear": 300000,
        "unit": "件",
        "discord_remind": False,
    },
    "Michigan Inflation 1Y": {
        "fred_id": "MICH",
        "input_method": "FRED",
        "fred_release_id": None,
        "michigan_rule": True,
        "slug": "mich_1y",
        "threshold_bull": 2.5,
        "threshold_bear": 4.0,
        "unit": "%",
        "discord_remind": False,
    },
    "Michigan Inflation 5Y": {
        "fred_id": "T5YIE",          # 5-Year Breakeven Inflation Rate（市場ベース代替）
        "input_method": "FRED",
        "fred_release_id": None,
        "michigan_rule": True,
        "slug": "mich_5y",
        "threshold_bull": 2.5,
        "threshold_bear": 3.5,
        "unit": "%",
        "discord_remind": False,
    },
    "Michigan Consumer Sentiment": {
        "fred_id": "UMCSENT",
        "input_method": "FRED",
        "fred_release_id": None,
        "michigan_rule": True,
        "slug": "mich_sent",
        "threshold_bull": 90.0,
        "threshold_bear": 70.0,
        "unit": "index",
        "discord_remind": False,
    },
    "Building Permits": {
        "fred_id": "PERMIT",
        "input_method": "FRED",
        "fred_release_id": None,   # FRED Release Calendar が空のためルールベース算出
        "permit_rule": True,       # 毎月第3週火曜（Housing Starts と同日発表）
        "slug": "permit",
        "threshold_bull": 1400.0,
        "threshold_bear": 1200.0,
        "unit": "千件",
        "discord_remind": False,
    },
    # ── デイリー指標（毎日自動記録）────────────────────────────
    "Yield Curve 10Y-2Y": {
        "fred_id": "T10Y2Y",
        "input_method": "FRED",
        "fred_release_id": None,
        "slug": "yc_10y2y",
        "unit": "%",
        "daily": True,
        "discord_remind": False,
    },
    "HY Spread": {
        "fred_id": "BAMLH0A0HYM2",
        "input_method": "FRED",
        "fred_release_id": None,
        "slug": "hy_spread",
        "unit": "%",
        "daily": True,
        "discord_remind": False,
    },
    "VIX": {
        "fred_id": "VIXCLS",
        "input_method": "FRED",
        "fred_release_id": None,
        "slug": "vix",
        "unit": "index",
        "daily": True,
        "discord_remind": False,
    },
}

# ─────────────────────────────────────────────────────────────────
#  event_id 生成
# ─────────────────────────────────────────────────────────────────

def make_event_id(indicator: str, release_date) -> str:
    slug = INDICATOR_CONFIG.get(indicator, {}).get("slug", "")
    if not slug:
        slug = re.sub(r"[^a-z0-9]+", "_", indicator.lower()).strip("_")
    if isinstance(release_date, date):
        date_str = release_date.strftime("%Y-%m-%d")
    else:
        date_str = str(release_date)
    return f"{slug}_{date_str}"

# ─────────────────────────────────────────────────────────────────
#  米国祝日・営業日計算
# ─────────────────────────────────────────────────────────────────

def nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta + (n - 1) * 7)


def us_holidays(year: int) -> set:
    import calendar

    def last_weekday(y, m, wd):
        last_day = calendar.monthrange(y, m)[1]
        last = date(y, m, last_day)
        delta = (last.weekday() - wd) % 7
        return last - timedelta(days=delta)

    holidays = set()
    ny = date(year, 1, 1)
    if ny.weekday() == 5:   ny = date(year, 12, 31)
    elif ny.weekday() == 6: ny = date(year, 1, 2)
    holidays.add(ny)
    holidays.add(nth_weekday(year, 1, 0, 3))
    holidays.add(nth_weekday(year, 2, 0, 3))
    holidays.add(last_weekday(year, 5, 0))
    jul4 = date(year, 7, 4)
    if jul4.weekday() == 5:   jul4 = date(year, 7, 3)
    elif jul4.weekday() == 6: jul4 = date(year, 7, 5)
    holidays.add(jul4)
    holidays.add(nth_weekday(year, 9, 0, 1))
    holidays.add(nth_weekday(year, 10, 0, 2))
    nov11 = date(year, 11, 11)
    if nov11.weekday() == 5:   nov11 = date(year, 11, 10)
    elif nov11.weekday() == 6: nov11 = date(year, 11, 12)
    holidays.add(nov11)
    holidays.add(nth_weekday(year, 11, 3, 4))
    xmas = date(year, 12, 25)
    if xmas.weekday() == 5:   xmas = date(year, 12, 24)
    elif xmas.weekday() == 6: xmas = date(year, 12, 26)
    holidays.add(xmas)
    return holidays


def nth_us_business_day(year: int, month: int, n: int) -> date:
    import calendar
    holidays = us_holidays(year) | us_holidays(year - 1) | us_holidays(year + 1)
    count = 0
    d = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    while d <= date(year, month, last_day):
        if d.weekday() < 5 and d not in holidays:
            count += 1
            if count == n:
                return d
        d += timedelta(days=1)
    raise ValueError(f"{year}-{month:02d} の第{n}営業日が見つかりません")


def us_business_days_add(start: date, n: int) -> date:
    """start から n 営業日後の日付を返す"""
    holidays = us_holidays(start.year) | us_holidays(start.year + 1)
    count = 0
    d = start + timedelta(days=1)
    while True:
        if d.weekday() < 5 and d not in holidays:
            count += 1
            if count == n:
                return d
        d += timedelta(days=1)

# ─────────────────────────────────────────────────────────────────
#  ISM 発表予定日算出
# ─────────────────────────────────────────────────────────────────

def ism_release_dates(months_ahead: int = 3) -> list[tuple[str, date]]:
    today = date.today()
    results = []
    for offset in range(months_ahead + 1):
        year  = today.year + (today.month - 1 + offset) // 12
        month = (today.month - 1 + offset) % 12 + 1
        try:
            mfg_date = nth_us_business_day(year, month, 1)   # 製造業: 第1営業日
            svc_date = nth_us_business_day(year, month, 3)   # 非製造業: 第3営業日
            if mfg_date >= today:
                results.append(("ISM Manufacturing PMI", mfg_date))
            if svc_date >= today:
                results.append(("ISM Non-Manufacturing PMI", svc_date))
        except ValueError as e:
            logger.warning(f"ISM date calc error: {e}")
    return results


def michigan_consumer_sentiment_release_dates(months_ahead: int = 3) -> list[tuple[str, date]]:
    """
    Michigan Consumer Sentiment（ミシガン大学消費者信頼感指数）の発表予定日。
    発表スケジュール: 毎月第2金曜日（速報値）
    Returns: [("Michigan Consumer Sentiment", release_date), ...]
    """
    import calendar
    today = date.today()
    results = []
    for offset in range(months_ahead + 1):
        year  = today.year + (today.month - 1 + offset) // 12
        month = (today.month - 1 + offset) % 12 + 1
        try:
            # 第2金曜日を算出（weekday=4が金曜）
            first_day = date(year, month, 1)
            first_friday = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
            second_friday = first_friday + timedelta(weeks=1)
            if second_friday >= today:
                results.append(("Michigan Consumer Sentiment", second_friday))
        except Exception as e:
            logger.warning(f"Michigan Consumer Sentiment date calc error: {e}")
    return results


def cb_lei_release_dates(months_ahead: int = 3) -> list[tuple[str, date]]:
    """
    OECD CLI (USALOLITONOSTSAM) の発表予定日。
    OECD CLIは毎月第2週月曜日前後に発表（前々月分データ）。
    FRED自動取得のため、スケジュールは概算でよい。
    Returns: [("Conference Board LEI", release_date), ...]
    """
    today = date.today()
    results = []
    for offset in range(months_ahead + 1):
        year  = today.year + (today.month - 1 + offset) // 12
        month = (today.month - 1 + offset) % 12 + 1
        try:
            release_date = nth_weekday(year, month, 0, 2)  # 0=月曜, 第2週
            if release_date >= today:
                results.append(("Conference Board LEI", release_date))
        except Exception as e:
            logger.warning(f"OECD CLI date calc error: {e}")
    return results


def building_permit_release_dates(months_ahead: int = 3) -> list[tuple[str, date]]:
    """
    Building Permits（住宅建築許可）の発表予定日をルールベースで算出。

    発表スケジュール:
      毎月第3週火曜日（Housing Starts と同日）
      ※ 前月分データを当月第3週火曜に発表

    Returns: [("Building Permits", release_date), ...]
    """
    today = date.today()
    results = []
    for offset in range(months_ahead + 1):
        year  = today.year + (today.month - 1 + offset) // 12
        month = (today.month - 1 + offset) % 12 + 1
        try:
            # 第3週火曜 = weekday=1（火曜）の第3週
            release_date = nth_weekday(year, month, 1, 3)
            if release_date >= today:
                results.append(("Building Permits", release_date))
        except Exception as e:
            logger.warning(f"Building Permits date calc error: {e}")
    return results


def michigan_release_dates(months_ahead: int = 3) -> list[tuple[str, date]]:
    today = date.today()
    results = []
    for offset in range(months_ahead + 1):
        year  = today.year + (today.month - 1 + offset) // 12
        month = (today.month - 1 + offset) % 12 + 1
        try:
            prelim = nth_weekday(year, month, 4, 2)
            final  = nth_weekday(year, month, 4, 4)
            for nm, rd in [("Michigan Inflation 1Y", prelim), ("Michigan Inflation 5Y", prelim),
                            ("Michigan Inflation 1Y", final),  ("Michigan Inflation 5Y", final)]:
                if rd >= today:
                    results.append((nm, rd))
        except Exception as e:
            logger.warning(f"Michigan date calc error: {e}")
    return results

# ─────────────────────────────────────────────────────────────────
#  FRED Release Calendar
# ─────────────────────────────────────────────────────────────────

def fred_release_dates(fred_api_key: str, days_ahead: int = 90) -> dict[str, list[date]]:
    today    = date.today()
    end_date = today + timedelta(days=days_ahead)
    results  = {}
    for ind_name, cfg in INDICATOR_CONFIG.items():
        release_id = cfg.get("fred_release_id")
        if not release_id:
            continue
        all_dates = []
        url = (
            f"https://api.stlouisfed.org/fred/release/dates"
            f"?release_id={release_id}"
            f"&realtime_start={today.strftime('%Y-%m-%d')}"
            f"&realtime_end={end_date.strftime('%Y-%m-%d')}"
            f"&include_release_dates_with_no_data=true"
            f"&api_key={fred_api_key}"
            f"&file_type=json"
        )
        for attempt in range(3):
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
                if attempt < 2:
                    logger.warning(f"[FRED Release] {ind_name} attempt {attempt+1}: {e} → retry {wait}s")
                    time.sleep(wait)
                else:
                    logger.warning(f"[FRED Release] {ind_name} failed: {e}")
        results[ind_name] = sorted(set(all_dates))
        logger.info(f"[FRED Release] {ind_name}: {[str(d) for d in results[ind_name]]}")
    return results

# ─────────────────────────────────────────────────────────────────
#  スケジュール CSV（v6.0 スキーマ）
# ─────────────────────────────────────────────────────────────────

def ensure_schedule_csv():
    os.makedirs("data", exist_ok=True)
    if os.path.exists(SCHEDULE_PATH):
        return
    pd.DataFrame(columns=SCHEDULE_COLUMNS).to_csv(SCHEDULE_PATH, index=False, encoding="utf-8")
    logger.info(f"Created schedule: {SCHEDULE_PATH}")


def load_schedule() -> pd.DataFrame:
    if not os.path.exists(SCHEDULE_PATH):
        return pd.DataFrame(columns=SCHEDULE_COLUMNS)
    df = pd.read_csv(SCHEDULE_PATH, encoding="utf-8", dtype=str).fillna("")
    # 後方互換: 旧スキーマ（指標名/発表予定日）を新スキーマに変換
    if "指標名" in df.columns and "indicator" not in df.columns:
        df = df.rename(columns={"指標名": "indicator", "発表予定日": "release_date"})
    for col in SCHEDULE_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def update_schedule(fred_api_key: str, days_ahead: int = 90):
    ensure_schedule_csv()
    df = load_schedule()
    registered = set(zip(df["indicator"], df["release_date"]))
    new_rows = []

    # FRED Release Calendar
    for ind_name, dates in fred_release_dates(fred_api_key, days_ahead).items():
        cfg = INDICATOR_CONFIG.get(ind_name, {})
        for rd in dates:
            date_str = rd.strftime("%Y-%m-%d")
            if (ind_name, date_str) in registered:
                continue
            new_rows.append({
                "indicator":    ind_name,
                "release_date": date_str,
                "fred_id":      cfg.get("fred_id", ""),
                "input_method": cfg.get("input_method", "FRED"),
                "consensus":    "",
                "actual":       "",
                "status":       "scheduled",
            })

    # ISM 製造業・非製造業
    for ind_name, rd in ism_release_dates(months_ahead=3):
        date_str = rd.strftime("%Y-%m-%d")
        if (ind_name, date_str) in registered:
            continue
        new_rows.append({
            "indicator":    ind_name,
            "release_date": date_str,
            "fred_id":      "",
            "input_method": "manual",
            "consensus":    "",
            "actual":       "",
            "status":       "scheduled",
        })

    # Michigan
    for ind_name, rd in michigan_release_dates(months_ahead=3):
        date_str = rd.strftime("%Y-%m-%d")
        if (ind_name, date_str) in registered:
            continue
        new_rows.append({
            "indicator":    ind_name,
            "release_date": date_str,
            "fred_id":      INDICATOR_CONFIG.get(ind_name, {}).get("fred_id", ""),
            "input_method": "FRED",
            "consensus":    "",
            "actual":       "",
            "status":       "scheduled",
        })

    # Building Permits（第3週火曜ルール）
    for ind_name, rd in building_permit_release_dates(months_ahead=3):
        date_str = rd.strftime("%Y-%m-%d")
        if (ind_name, date_str) in registered:
            continue
        new_rows.append({
            "indicator":    ind_name,
            "release_date": date_str,
            "fred_id":      INDICATOR_CONFIG.get(ind_name, {}).get("fred_id", ""),
            "input_method": "FRED",
            "consensus":    "",
            "actual":       "",
            "status":       "scheduled",
        })
        logger.info(f"[Schedule+] {ind_name}: {date_str} (第3週火曜 ルールベース算出)")

    # Michigan Consumer Sentiment（毎月第2金曜ルール）
    for ind_name, rd in michigan_consumer_sentiment_release_dates(months_ahead=3):
        date_str = rd.strftime("%Y-%m-%d")
        if (ind_name, date_str) in registered:
            continue
        new_rows.append({
            "indicator":    ind_name,
            "release_date": date_str,
            "fred_id":      INDICATOR_CONFIG.get(ind_name, {}).get("fred_id", ""),
            "input_method": "FRED",
            "consensus":    "",
            "actual":       "",
            "status":       "scheduled",
        })
        logger.info(f"[Schedule+] {ind_name}: {date_str} (第2金曜 ルールベース算出)")

    # Conference Board LEI（毎月第3木曜ルール）
    for ind_name, rd in cb_lei_release_dates(months_ahead=3):
        date_str = rd.strftime("%Y-%m-%d")
        if (ind_name, date_str) in registered:
            continue
        new_rows.append({
            "indicator":    ind_name,
            "release_date": date_str,
            "fred_id":      INDICATOR_CONFIG.get(ind_name, {}).get("fred_id", ""),
            "input_method": "manual",
            "consensus":    "",
            "actual":       "",
            "status":       "scheduled",
        })
        logger.info(f"[Schedule+] {ind_name}: {date_str} (第3木曜 ルールベース算出)")

    if not new_rows:
        logger.info("Schedule up to date.")
        return

    new_df = pd.DataFrame(new_rows, columns=SCHEDULE_COLUMNS)
    combined = pd.concat([df, new_df], ignore_index=True)
    combined = combined.sort_values(["release_date", "indicator"]).reset_index(drop=True)
    combined.to_csv(SCHEDULE_PATH, index=False, encoding="utf-8")
    logger.info(f"Schedule updated: +{len(new_rows)} rows")

# ─────────────────────────────────────────────────────────────────
#  Discord リマインダー
# ─────────────────────────────────────────────────────────────────

def send_discord(message: str):
    webhook_url = os.environ.get("DISCORD_WEB_HOOK", "")
    if not webhook_url:
        logger.warning("DISCORD_WEB_HOOK not set. Skip notification.")
        return
    try:
        r = requests.post(webhook_url, json={"content": message}, timeout=10)
        r.raise_for_status()
        logger.info("Discord notification sent.")
    except Exception as e:
        logger.warning(f"Discord notification failed: {e}")


def remind_manual_indicators(target_date: date):
    """
    当日発表予定の手入力指標を Discord に通知。
    --remind フラグで実行（GitHub Actions 朝7時 JST に呼び出す）。
    """
    schedule = load_schedule()
    date_str = target_date.strftime("%Y-%m-%d")
    today_rows = schedule[
        (schedule["release_date"] == date_str) &
        (schedule["input_method"] == "manual")
    ]

    if today_rows.empty:
        logger.info(f"No manual indicators today ({date_str}).")
        return

    lines = [f"📊 **MACRO PULSE — 手入力リマインダー** ({date_str})"]
    for _, row in today_rows.iterrows():
        ind  = row["indicator"]
        cons = row["consensus"]
        cons_str = f"  コンセンサス: {cons}" if cons else "  コンセンサス: 未設定"
        lines.append(f"• **{ind}**\n{cons_str}")

    lines.append("\n→ `data/05_indicator_schedule.csv` の `actual` 列に値を入力してください。")
    send_discord("\n".join(lines))
    logger.info(f"Reminded {len(today_rows)} manual indicators.")


def remind_missing_actuals(target_date: date):
    """
    毎週日曜: 過去30日以内で actual が空の手入力指標をアラート。
    """
    schedule = load_schedule()
    cutoff = (target_date - timedelta(days=30)).strftime("%Y-%m-%d")
    today_str = target_date.strftime("%Y-%m-%d")
    missing = schedule[
        (schedule["release_date"] >= cutoff) &
        (schedule["release_date"] <= today_str) &
        (schedule["input_method"] == "manual") &
        (schedule["actual"].str.strip() == "")
    ]

    if missing.empty:
        logger.info("No missing actuals.")
        return

    lines = [f"⚠️ **MACRO PULSE — 未入力アラート** ({today_str})"]
    for _, row in missing.iterrows():
        lines.append(f"• **{row['indicator']}** ({row['release_date']}) — actual 未入力")
    lines.append("\n→ `data/05_indicator_schedule.csv` を更新してください。")
    send_discord("\n".join(lines))
    logger.info(f"Missing actuals alert: {len(missing)} rows")

# ─────────────────────────────────────────────────────────────────
#  FRED クライアント
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


def fred_latest(fred, series_id: str, target_date: date, lookback: int = 60):
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


def get_ff_current(fred):
    if fred is None:
        return None
    v_hi, _ = fred_latest(fred, "DFEDTARU", date.today(), lookback=30)
    v_lo, _ = fred_latest(fred, "DFEDTARL", date.today(), lookback=30)
    if v_hi is not None and v_lo is not None:
        return round((v_hi + v_lo) / 2, 4)
    v, _ = fred_latest(fred, "FEDFUNDS", date.today(), lookback=45)
    return round(v, 4) if v is not None else None


def get_zq_futures(target_date: date, fred=None):
    if fred is None:
        return None, None, None
    t1yff, _ = fred_latest(fred, "T1YFF", target_date, lookback=30)
    if t1yff is None:
        return None, None, None
    ff_current = get_ff_current(fred)
    if ff_current is None:
        return None, None, None
    implied_rate = round(ff_current + t1yff, 4)
    return "FRED:T1YFF", round(t1yff, 4), implied_rate

# ─────────────────────────────────────────────────────────────────
#  金融環境スナップショット
# ─────────────────────────────────────────────────────────────────

def get_financial_context(target_date: date, fred) -> dict:
    """
    イベント記録時点の金融環境を取得。
    regime は fed_context.csv の最新値を使用。
    """
    ctx = {
        "regime": "BALANCED",
        "ff_rate": None,
        "yc_10y2y": None,
        "hy_spread": None,
        "vix": None,
        "cuts_implied": None,
    }

    # fed_context.csv から最新 regime・cuts_implied を読み込み
    if os.path.exists(FED_CONTEXT_PATH):
        try:
            fc = pd.read_csv(FED_CONTEXT_PATH, dtype=str).fillna("")
            if not fc.empty:
                last = fc.iloc[-1]
                ctx["regime"]       = last.get("regime", "BALANCED")
                ctx["ff_rate"]      = _safe_float(last.get("ff_current"))
                ctx["cuts_implied"] = _safe_float(last.get("cuts_implied"))
        except Exception as e:
            logger.warning(f"fed_context read: {e}")

    if fred:
        yc, _ = fred_latest(fred, "T10Y2Y", target_date)
        hy, _ = fred_latest(fred, "BAMLH0A0HYM2", target_date)
        vx, _ = fred_latest(fred, "VIXCLS", target_date)
        if yc is not None: ctx["yc_10y2y"]  = round(yc, 4)
        if hy is not None: ctx["hy_spread"]  = round(hy, 4)
        if vx is not None: ctx["vix"]        = round(vx, 2)
        ff = get_ff_current(fred)
        if ff is not None: ctx["ff_rate"]    = ff

    return ctx


def _safe_float(v):
    try:
        return float(v) if v not in (None, "", "nan") else None
    except (ValueError, TypeError):
        return None

# ─────────────────────────────────────────────────────────────────
#  S&P500 取得
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


def get_sp500(target_date: date, fred=None):
    """指定日のS&P500終値を返す"""
    if fred:
        v, _ = fred_latest(fred, "SP500", target_date, lookback=10)
        if v:
            return v
    return _stooq("%5Espx", target_date)

# ─────────────────────────────────────────────────────────────────
#  events.csv I/O
# ─────────────────────────────────────────────────────────────────

def load_events() -> pd.DataFrame:
    if not os.path.exists(EVENTS_PATH):
        return pd.DataFrame(columns=EVENTS_COLUMNS)
    try:
        df = pd.read_csv(EVENTS_PATH, encoding="utf-8", dtype=str).fillna("")
        for c in EVENTS_COLUMNS:
            if c not in df.columns:
                df[c] = ""
        return df
    except Exception as e:
        logger.warning(f"events.csv read error: {e}")
        return pd.DataFrame(columns=EVENTS_COLUMNS)


def save_events(df: pd.DataFrame):
    os.makedirs("data", exist_ok=True)
    df = df.drop_duplicates(subset=["event_id"], keep="last")
    df = df.sort_values(["release_date", "indicator"]).reset_index(drop=True)
    df.to_csv(EVENTS_PATH, index=False, encoding="utf-8")
    logger.info(f"events.csv saved: {EVENTS_PATH} ({len(df)} rows)")

# ─────────────────────────────────────────────────────────────────
#  期待値解決
# ─────────────────────────────────────────────────────────────────

def resolve_forecast(indicator: str, release_date_str: str, actual_val,
                     schedule: pd.DataFrame, events: pd.DataFrame):
    """
    優先順位:
      1. schedule.csv の consensus 列
      2. events.csv に既存の consensus
      3. actual_as_forecast フォールバック
    Returns: (forecast_val, forecast_source, surprise, surprise_pct)
    """
    # schedule から取得
    mask = (schedule["indicator"] == indicator) & (schedule["release_date"] == release_date_str)
    hits = schedule[mask]
    if not hits.empty:
        cons_str = hits.iloc[-1].get("consensus", "")
        if cons_str and cons_str.strip():
            try:
                fv = float(cons_str)
                src = "user"
                surp = round(actual_val - fv, 4) if actual_val is not None else None
                surp_pct = round(surp / abs(fv) * 100, 4) if (surp is not None and fv != 0) else None
                return fv, src, surp, surp_pct
            except (ValueError, TypeError):
                pass

    # events.csv に既存 consensus
    if not events.empty:
        ev_mask = (events["indicator"] == indicator) & (events["release_date"] == release_date_str)
        ev_hits = events[ev_mask]
        if not ev_hits.empty:
            cons_str = ev_hits.iloc[-1].get("consensus", "")
            if cons_str and cons_str.strip():
                try:
                    fv = float(cons_str)
                    src = str(ev_hits.iloc[-1].get("forecast_source", "stored") or "stored")
                    surp = round(actual_val - fv, 4) if actual_val is not None else None
                    surp_pct = round(surp / abs(fv) * 100, 4) if (surp is not None and fv != 0) else None
                    return fv, src, surp, surp_pct
                except (ValueError, TypeError):
                    pass

    # フォールバック
    if actual_val is not None:
        return actual_val, "actual_as_forecast", 0.0, 0.0
    return None, "none", None, None

# ─────────────────────────────────────────────────────────────────
#  指標フェッチ → event row 生成
# ─────────────────────────────────────────────────────────────────

def fetch_event_row(indicator: str, target_date: date, fred,
                    fin_ctx: dict, schedule: pd.DataFrame,
                    events: pd.DataFrame,
                    override_actual=None) -> dict:
    cfg      = INDICATOR_CONFIG.get(indicator, {})
    fred_id  = cfg.get("fred_id", "")
    date_str = target_date.strftime("%Y-%m-%d")
    event_id = make_event_id(indicator, target_date)

    row = {col: "" for col in EVENTS_COLUMNS}
    row.update({
        "event_id":    event_id,
        "indicator":   indicator,
        "release_date": date_str,
        "regime":      fin_ctx.get("regime", ""),
        "ff_rate":     _fmt(fin_ctx.get("ff_rate")),
        "yc_10y2y":    _fmt(fin_ctx.get("yc_10y2y")),
        "hy_spread":   _fmt(fin_ctx.get("hy_spread")),
        "vix":         _fmt(fin_ctx.get("vix")),
        "cuts_implied": _fmt(fin_ctx.get("cuts_implied")),
        "updated_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })

    actual_val = override_actual

    # FRED から実績値取得
    if fred and fred_id and actual_val is None:
        for attempt in range(3):
            try:
                a, d = fred_latest(fred, fred_id, target_date)
                if a is not None:
                    actual_val = a
                    if d:
                        row["release_date"] = d.strftime("%Y-%m-%d")
                        row["event_id"]     = make_event_id(indicator, d)
                break
            except Exception as e:
                logger.warning(f"[{indicator}] FRED attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)

    row["actual"] = _fmt(actual_val)

    # 期待値・サプライズ
    fv, src, surp, surp_pct = resolve_forecast(
        indicator, row["release_date"], actual_val, schedule, events)
    row["consensus"]     = _fmt(fv)
    row["surprise"]      = _fmt(surp)
    row["surprise_pct"]  = _fmt(surp_pct)
    row["forecast_source"] = src or ""
    row["data_source"]   = "FRED" if (fred_id and override_actual is None) else \
                           ("manual" if override_actual is not None else "N/A")

    return row


def _fmt(v) -> str:
    if v is None or v == "" :
        return ""
    try:
        f = float(v)
        if f != f:  # nan
            return ""
        return str(v)
    except (ValueError, TypeError):
        return str(v)

# ─────────────────────────────────────────────────────────────────
#  S&P500 変化率の後補完 (--fill-returns)
# ─────────────────────────────────────────────────────────────────

def _load_sp500_cache(fred, from_date: str, to_date: str) -> pd.Series:
    """
    S&P500終値を指定期間まとめて取得してキャッシュ返却。
    FRED SP500 → 失敗時 stooq にフォールバック。
    Returns: pd.Series（index=date, value=終値）
    """
    logger.info(f"S&P500 一括取得中 ({from_date} 〜 {to_date})...")
    if fred:
        try:
            s = fred.get_series("SP500", observation_start=from_date, observation_end=to_date)
            if s is not None and not s.empty:
                s = s.dropna()
                # タイムゾーンをtz-naiveに正規化（比較エラー対策）
                if hasattr(s.index, 'tz') and s.index.tz is not None:
                    s.index = s.index.tz_localize(None)
                logger.info(f"S&P500 (FRED): {len(s)} obs")
                return s
        except Exception as e:
            logger.warning(f"S&P500 FRED: {e} → stooq fallback")

    # stooq fallback: 一括取得
    try:
        d1 = from_date.replace("-", "")
        d2 = to_date.replace("-", "")
        url = f"https://stooq.com/q/d/l/?s=%5Espx&d1={d1}&d2={d2}&i=d"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        r.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(r.text))
        df.columns = [c.strip() for c in df.columns]
        df["Date"] = pd.to_datetime(df["Date"])
        s = df.set_index("Date")["Close"].dropna()
        logger.info(f"S&P500 (stooq): {len(s)} obs")
        return s
    except Exception as e:
        logger.warning(f"S&P500 stooq: {e}")
        return pd.Series(dtype=float)


def _lookup_sp500(cache: pd.Series, target_date: date):
    """キャッシュから target_date 以前の最新終値を返す"""
    if cache.empty:
        return None
    td = pd.Timestamp(target_date)
    # FREDのSP500はUTC付きDatetimeIndexの場合があるためtz-naiveに正規化
    idx = cache.index
    if hasattr(idx, 'tz') and idx.tz is not None:
        idx = idx.tz_localize(None)
        cache = pd.Series(cache.values, index=idx)
    s = cache[cache.index <= td]
    if s.empty:
        return None
    return round(float(s.iloc[-1]), 2)


def fill_returns(fred=None):
    """
    events.csv の sp500_t0〜t20 と ret_* を一括補完。
    S&P500を全期間まとめて1回取得してキャッシュし、行ごとのAPI呼び出しをゼロにする。
    """
    events = load_events()
    if events.empty:
        logger.info("No events to fill.")
        return

    today = date.today()

    # デイリー指標はsp0のみ補完するが、needから除外して期間計算を正確にする
    DAILY_INDS_SET = {'Yield Curve 10Y-2Y', 'HY Spread', 'VIX', 'Michigan Inflation 5Y'}
    # 補完が必要な行に絞る（デイリー指標は除外）
    need = events[
        (events["release_date"] != "") &
        (~events["indicator"].isin(DAILY_INDS_SET)) &
        (
            (events["sp500_t0"] == "") |
            (events["sp500_t1"] == "") |
            (events["sp500_t5"] == "") |
            (events["sp500_t10"] == "") |
            (events["sp500_t20"] == "")
        )
    ]
    if need.empty:
        logger.info("fill-returns: nothing to update.")
        return

    # 対象期間を算出（t20補完のため最大+30日余裕）
    # min_dateは7日前倒し（元旦・週末等でS&P500休場の場合に前日終値を取得するため）
    raw_min = pd.to_datetime(need["release_date"].min()).date()
    min_date = (raw_min - timedelta(days=7)).strftime("%Y-%m-%d")
    max_rd   = pd.to_datetime(need["release_date"].max()).date()
    max_date = min(today, max_rd + timedelta(days=45)).strftime("%Y-%m-%d")
    logger.info(f"fill-returns: {len(need)} rows need update ({need['release_date'].min()} 〜 {need['release_date'].max()})")

    # S&P500 を一括キャッシュ
    sp_cache = _load_sp500_cache(fred, min_date, max_date)
    if sp_cache.empty:
        logger.error("S&P500 cache empty. Cannot fill returns.")
        return

    updated = 0
    skip_no_sp0 = 0
    skip_future = 0
    skip_no_spn = 0
    first_rows_logged = 0
    DAILY_INDS = {'Yield Curve 10Y-2Y', 'HY Spread', 'VIX', 'Michigan Inflation 5Y'}

    for idx, row in need.iterrows():
        try:
            rd = datetime.strptime(row["release_date"], "%Y-%m-%d").date()
        except Exception as e:
            logger.warning(f"fill-returns: release_date parse error idx={idx} val={repr(row['release_date'])}: {e}")
            continue

        # t0: 発表日当日または直前の終値
        if not events.at[idx, "sp500_t0"]:
            sp0 = _lookup_sp500(sp_cache, rd)
            if sp0:
                events.at[idx, "sp500_t0"] = str(sp0)
                updated += 1
            else:
                skip_no_sp0 += 1
                if skip_no_sp0 <= 3:
                    logger.warning(f"fill-returns: sp0 not found for {row['indicator']} {rd} (cache range: {sp_cache.index.min()} 〜 {sp_cache.index.max()})")
                continue

        t0_str = events.at[idx, "sp500_t0"]
        if not t0_str:
            continue
        try:
            t0_val = float(t0_str)
        except (ValueError, TypeError):
            continue

        # デイリー指標はt1〜t20補完不要（コンテキスト用途のためsp0のみ）
        if row.get("indicator", "") in DAILY_INDS:
            continue

        # t1/t5/t10/t20
        for n, col_sp, col_ret in [
            (1,  "sp500_t1",  "ret_t1"),
            (5,  "sp500_t5",  "ret_t5"),
            (10, "sp500_t10", "ret_t10"),
            (20, "sp500_t20", "ret_t20"),
        ]:
            if events.at[idx, col_sp]:
                continue
            target_n = us_business_days_add(rd, n)
            if target_n > today:  # todayを含まない（当日終値は翌朝確定）
                skip_future += 1
                continue
            sp_n = _lookup_sp500(sp_cache, target_n)
            if sp_n is None:
                skip_no_spn += 1
                continue
            ret_n = round((sp_n - t0_val) / t0_val * 100, 4)
            events.at[idx, col_sp]  = str(sp_n)
            events.at[idx, col_ret] = str(ret_n)
            updated += 1

    logger.info(f"fill-returns stats: skip_no_sp0={skip_no_sp0} skip_future={skip_future} skip_no_spn={skip_no_spn}")

    if updated:
        save_events(events)
        logger.info(f"fill-returns: {updated} cells updated.")
    else:
        logger.info("fill-returns: nothing to update.")

# ─────────────────────────────────────────────────────────────────
#  --recalc（サプライズ再計算）
# ─────────────────────────────────────────────────────────────────

def recalc(events: pd.DataFrame) -> pd.DataFrame:
    updated = 0
    for idx, row in events.iterrows():
        try:
            actual   = float(row["actual"])
            forecast = float(row["consensus"])
        except (ValueError, TypeError):
            continue
        src     = str(row.get("forecast_source", "") or "")
        new_sur = round(actual - forecast, 4)
        new_pct = round(new_sur / abs(forecast) * 100, 4) if forecast != 0 else 0.0
        old_sur = row.get("surprise", "")

        if src == "actual_as_forecast" and forecast != actual:
            events.at[idx, "surprise"]       = str(new_sur)
            events.at[idx, "surprise_pct"]   = str(new_pct)
            events.at[idx, "forecast_source"] = "user_retroactive"
            events.at[idx, "updated_at"]     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            updated += 1
            logger.info(f"[RECALC] {row['event_id']}: {old_sur} → {new_sur}")
        elif src in ("user", "user_retroactive"):
            old_str = str(old_sur)
            if old_str != str(new_sur):
                events.at[idx, "surprise"]     = str(new_sur)
                events.at[idx, "surprise_pct"] = str(new_pct)
                events.at[idx, "updated_at"]   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                updated += 1
    logger.info(f"Recalc done: {updated} rows updated.")
    return events

# ─────────────────────────────────────────────────────────────────
#  fed_context.csv 更新（v5から継承）
# ─────────────────────────────────────────────────────────────────

def fetch_latest_fomc_statement():
    cal_url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    found_url = None
    fomc_date = None
    try:
        r = requests.get(cal_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        r.raise_for_status()
        html = r.text
        pats = [
            r'href="(/newsevents/pressreleases/monetary(\d{8})a\d?\.htm)"',
            r'href="(/monetarypolicy/(\d{8})a\d?\.htm)"',
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
    except Exception as e:
        logger.warning(f"FOMC calendar fetch: {e}")

    if not found_url:
        known = ["20260318","20260129","20251218","20251107","20250918","20250730"]
        today_str = date.today().strftime("%Y%m%d")
        for best_dt in sorted([d for d in known if d <= today_str], reverse=True):
            u = f"https://www.federalreserve.gov/newsevents/pressreleases/monetary{best_dt}a.htm"
            try:
                r = requests.get(u, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
                if r.status_code == 200:
                    found_url = u
                    fomc_date = datetime.strptime(best_dt, "%Y%m%d").strftime("%Y-%m-%d")
                    break
            except Exception:
                pass

    if not found_url:
        return None, None

    try:
        r2 = requests.get(found_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        r2.raise_for_status()
        text = re.sub(r'<[^>]+>', ' ', r2.text)
        text = re.sub(r'\s+', ' ', text).strip()
        start = -1
        for marker in ["Recent indicators","The Federal Open Market Committee","Information received since"]:
            idx = text.find(marker)
            if idx != -1:
                start = idx
                break
        stmt_text = text[start:start+3000] if start != -1 else text[500:3500]
        return fomc_date, stmt_text
    except Exception as e:
        logger.warning(f"FOMC statement body: {e}")
        return None, None


def _fallback_regime(ff_current, zq_rate, cuts_implied):
    if cuts_implied is None:
        return {"regime":"BALANCED","dominant_concern":"BALANCED","dominant_label":"両睨み","ai_reason":"データ取得失敗のためルールベース判定。"}
    if cuts_implied >= 1.0:
        return {"regime":"EASING","dominant_concern":"EMPLOYMENT_FOCUS","dominant_label":"雇用重視","ai_reason":f"ZQ先物が{cuts_implied:.1f}回の利下げを織り込み。EASING局面と判定（AI分析なし）。"}
    elif cuts_implied <= -1.0:
        return {"regime":"TIGHTENING","dominant_concern":"INFLATION_FOCUS","dominant_label":"インフレ警戒","ai_reason":f"ZQ先物が{abs(cuts_implied):.1f}回の利上げを織り込み。TIGHTENING局面と判定（AI分析なし）。"}
    else:
        return {"regime":"BALANCED","dominant_concern":"BALANCED","dominant_label":"両睨み","ai_reason":f"ZQ先物の織り込みが{cuts_implied:+.1f}回でBALANCED局面と判定（AI分析なし）。"}


def analyze_fomc_with_gemini(fomc_date, stmt_text, ff_current, zq_rate, cuts_implied):
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return _fallback_regime(ff_current, zq_rate, cuts_implied)
    prompt = f"""You are a Federal Reserve policy analyst. Analyze the following FOMC statement and market data.

FOMC Statement ({fomc_date}):
{stmt_text}

Market Context:
- Current FF Rate: {ff_current}%
- 12-month ahead FF futures implied rate: {zq_rate}%
- Market-implied rate changes in 12M: {cuts_implied:+.1f} cuts (25bp each)

Respond ONLY in this exact JSON format (no markdown, no extra text):
{{"regime":"EASING","dominant_concern":"EMPLOYMENT_FOCUS","dominant_label":"雇用重視","ai_reason":"日本語で100字以内で判断理由を記載。"}}"""
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
        payload = {"contents":[{"parts":[{"text":prompt}]}],"generationConfig":{"temperature":0.1,"maxOutputTokens":300}}
        for attempt in range(3):
            r = requests.post(url, json=payload, headers={"Content-Type":"application/json"}, timeout=30)
            if r.status_code == 429:
                wait = 15 * (2 ** attempt)
                if attempt < 2:
                    time.sleep(wait)
                    continue
                return _fallback_regime(ff_current, zq_rate, cuts_implied)
            r.raise_for_status()
            break
        data = r.json()
        raw = data["candidates"][0]["content"]["parts"][0]["text"].strip()
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            return json.loads(m.group())
    except Exception as e:
        logger.warning(f"Gemini API error: {e}")
    return _fallback_regime(ff_current, zq_rate, cuts_implied)


def update_fed_context(target_date: date, fred):
    logger.info("=== Updating Fed Context ===")
    if os.path.exists(FED_CONTEXT_PATH):
        ctx_df = pd.read_csv(FED_CONTEXT_PATH, dtype=str)
    else:
        ctx_df = pd.DataFrame(columns=FED_CONTEXT_COLUMNS)

    zq_ticker, zq_price, zq_rate = get_zq_futures(target_date, fred)
    ff_current = get_ff_current(fred)
    if ff_current is None:
        ff_current = 4.375

    cuts_implied = None
    if zq_rate is not None and ff_current is not None:
        cuts_implied = round((ff_current - zq_rate) / 0.25, 2)

    record_month = target_date.strftime("%Y-%m")
    already = (not ctx_df.empty and "record_date" in ctx_df.columns and
               ctx_df["record_date"].str.startswith(record_month).any())

    if already:
        last_idx = ctx_df[ctx_df["record_date"].str.startswith(record_month)].index[-1]
        ctx_df.loc[last_idx, "zq_ticker"]    = zq_ticker or ""
        ctx_df.loc[last_idx, "zq_price"]     = str(zq_price or "")
        ctx_df.loc[last_idx, "zq_rate"]      = str(zq_rate or "")
        ctx_df.loc[last_idx, "ff_current"]   = str(ff_current)
        ctx_df.loc[last_idx, "cuts_implied"] = str(cuts_implied or "")
        ctx_df.loc[last_idx, "updated_at"]   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    else:
        fomc_date, stmt_text = fetch_latest_fomc_statement()
        if stmt_text:
            analysis = analyze_fomc_with_gemini(
                fomc_date or target_date.strftime("%Y-%m-%d"),
                stmt_text, ff_current, zq_rate or ff_current, cuts_implied or 0)
        else:
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
        ctx_df = pd.concat([ctx_df, pd.DataFrame([new_row])], ignore_index=True)

    os.makedirs("data", exist_ok=True)
    ctx_df.to_csv(FED_CONTEXT_PATH, index=False, encoding="utf-8")
    logger.info(f"Fed context saved: {FED_CONTEXT_PATH}")

# ─────────────────────────────────────────────────────────────────
#  メインオーケストレーター
# ─────────────────────────────────────────────────────────────────

def run(target_date: date, test_mode: bool = False, do_recalc: bool = False,
        do_update_schedule: bool = False, do_remind: bool = False,
        do_fill_returns: bool = False):
    logger.info(f"=== MACRO PULSE v6.0 | {target_date} | recalc={do_recalc} | "
                f"update_schedule={do_update_schedule} | remind={do_remind} | "
                f"fill_returns={do_fill_returns} ===")

    ensure_schedule_csv()
    fred     = get_fred()
    schedule = load_schedule()
    events   = load_events()

    # ── Discord リマインダーモード ──────────────────────────────
    if do_remind:
        remind_manual_indicators(target_date)
        return

    # ── スケジュール更新モード ──────────────────────────────────
    if do_update_schedule:
        logger.info("=== UPDATE SCHEDULE MODE ===")
        fred_api_key = os.environ.get("FRED_API_KEY", "")
        if not fred_api_key:
            logger.error("FRED_API_KEY not set.")
            sys.exit(1)
        update_schedule(fred_api_key)
        update_fed_context(target_date, fred)
        remind_missing_actuals(target_date)
        logger.info("=== Schedule + Fed Context update complete ===")
        return

    # ── 再計算モード ────────────────────────────────────────────
    if do_recalc:
        logger.info("=== RECALC MODE ===")
        updated = recalc(events)
        save_events(updated)
        return

    # ── 変化率補完モード ────────────────────────────────────────
    if do_fill_returns:
        logger.info("=== FILL RETURNS MODE ===")
        fill_returns(fred)
        return

    # ── 通常実行：当日発表指標を処理 ───────────────────────────
    fin_ctx  = get_financial_context(target_date, fred)
    sp500_t0 = get_sp500(target_date, fred)
    logger.info(f"Financial context: {fin_ctx}")
    logger.info(f"S&P500 t0: {sp500_t0}")

    date_str  = target_date.strftime("%Y-%m-%d")
    scheduled = schedule[schedule["release_date"] == date_str].to_dict("records")
    logger.info(f"Scheduled today: {[r['indicator'] for r in scheduled]}")

    new_rows = []

    # スケジュール済み指標
    for sched in scheduled:
        ind = sched["indicator"]
        if INDICATOR_CONFIG.get(ind, {}).get("daily"):
            continue  # デイリー指標はまとめて後で記録

        override = None
        raw = str(sched.get("actual", "")).strip()
        if raw and raw.lower() not in ("", "nan"):
            try:
                override = float(raw)
            except ValueError:
                pass

        try:
            row = fetch_event_row(ind, target_date, fred, fin_ctx, schedule, events, override)
            row["sp500_t0"] = str(sp500_t0) if sp500_t0 else ""
            new_rows.append(row)
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"[{ind}]: {e}\n{traceback.format_exc()}")

    # デイリー指標（YC / HY / VIX）
    for ind_name in ["Yield Curve 10Y-2Y", "HY Spread", "VIX"]:
        try:
            row = fetch_event_row(ind_name, target_date, fred, fin_ctx, schedule, events)
            row["sp500_t0"] = str(sp500_t0) if sp500_t0 else ""
            new_rows.append(row)
        except Exception as e:
            logger.error(f"[{ind_name}]: {e}")

    if not new_rows:
        logger.info("No rows to add.")
        return

    new_df = pd.DataFrame(new_rows, columns=EVENTS_COLUMNS)
    key_new = set(new_df["event_id"])
    existing_filtered = events[~events["event_id"].isin(key_new)]
    combined = pd.concat([existing_filtered, new_df], ignore_index=True)
    save_events(combined)
    logger.info("=== Run complete ===")

# ─────────────────────────────────────────────────────────────────
#  Entry Point
# ─────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="MACRO PULSE v6.0")
    p.add_argument("--test",            action="store_true")
    p.add_argument("--recalc",          action="store_true", help="Recalculate surprises")
    p.add_argument("--update-schedule", action="store_true", help="Update schedule + fed context")
    p.add_argument("--remind",          action="store_true", help="Send Discord reminders for today's manual indicators")
    p.add_argument("--fill-returns",    action="store_true", help="Backfill S&P500 t+N returns")
    p.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: yesterday)")
    args = p.parse_args()
    target = (datetime.strptime(args.date, "%Y-%m-%d").date()
              if args.date else (datetime.now() - timedelta(days=1)).date())
    run(target,
        test_mode=args.test,
        do_recalc=args.recalc,
        do_update_schedule=args.update_schedule,
        do_remind=args.remind,
        do_fill_returns=args.fill_returns)


if __name__ == "__main__":
    main()
