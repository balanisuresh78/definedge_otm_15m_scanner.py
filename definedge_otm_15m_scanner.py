import os
import re
import io
import json
import time
import zipfile
import smtplib
import pyotp
import requests
import pytz
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, time as dtime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from tabulate import tabulate

# ==============================================================================
# 1. CREDENTIALS & CONFIGURATION (GITHUB ACTIONS SECRETS + FALLBACKS)
# ==============================================================================
DEFINEDGE_API_TOKEN = os.environ.get("DEFINEDGE_API_TOKEN", "").strip()
DEFINEDGE_API_SECRET = os.environ.get("DEFINEDGE_API_SECRET", "").strip()
DEFINEDGE_TOTP_SECRET = os.environ.get("DEFINEDGE_TOTP_SECRET", "").strip()

SENDER_EMAIL = (
    os.environ.get("SENDER_EMAIL")
    or os.environ.get("EMAIL_SENDER")
    or os.environ.get("EMAIL_USER")
    or "balanisuresh@gmail.com"
).strip()

RECEIVER_EMAIL = (
    os.environ.get("RECEIVER_EMAIL")
    or os.environ.get("EMAIL_RECEIVER")
    or SENDER_EMAIL
    or "balanisuresh@gmail.com"
).strip()

APP_PASSWORD = (
    os.environ.get("APP_PASSWORD")
    or os.environ.get("EMAIL_PASSWORD")
    or os.environ.get("GMAIL_APP_PASSWORD")
    or os.environ.get("EMAIL_PASS")
    or ""
).strip()

IST = pytz.timezone("Asia/Kolkata")
MEMORY_FILE = "otm_15m_memory.json"
BASKET_VERSION = "v4_official_daily_close"

ATR_PERIOD = 2
ATR_MULT_THRESHOLD = 1.5
VOL_MULT_THRESHOLD = 1.5
ATR_METHOD = "SMA"  # "SMA" or "RMA"
YESTERDAY_TREND_MODE = "CLOSE_VS_PREV_CLOSE"

LOOKBACK_DAYS_15M = 7
LOOKBACK_DAYS_100D_SLOT = 160

SCAN_CHECKPOINTS_IST = [
    dtime(9, 30, 12), dtime(9, 45, 12), dtime(10, 0, 12), dtime(10, 15, 12),
    dtime(10, 30, 12), dtime(10, 45, 12), dtime(11, 0, 12), dtime(11, 15, 12),
    dtime(11, 30, 12), dtime(11, 45, 12), dtime(12, 0, 12), dtime(12, 15, 12),
    dtime(12, 30, 12), dtime(12, 45, 12), dtime(13, 0, 12), dtime(13, 15, 12),
    dtime(13, 30, 12), dtime(13, 45, 12), dtime(14, 0, 12), dtime(14, 15, 12),
    dtime(14, 30, 12), dtime(14, 45, 12), dtime(15, 0, 12), dtime(15, 15, 12),
    dtime(15, 30, 12)
]

high_beta_50_stocks = [
    "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "BANKBARODA", "CANBK", "CHOLAFIN",
    "FEDERALBNK", "ICICIBANK", "INDUSINDBK", "PNB", "SBIN", "SHRIRAMFIN",
    "ANGELONE", "BSE", "CDSL", "MCX",
    "BDL", "BEL", "BHEL", "HAL", "IRFC", "MAZDOCK", "PFC", "RECLTD",
    "ABB", "CGPOWER", "CUMMINSIND", "DIXON", "KAYNES", "POLYCAB", "SIEMENS",
    "ADANIENT", "ADANIPORTS", "HINDALCO", "HINDCOPPER", "JINDALSTEL", "TATASTEEL", "VEDL",
    "BAJAJ-AUTO", "M&M", "MOTHERSON", "TATAMOTORS", "TRENT", "ZOMATO",
    "COFORGE", "DLF", "GODREJPROP", "OBEROIRLTY", "OFSS", "PERSISTENT"
]

SESSION_CACHE = {"key": "", "date": ""}

# ==============================================================================
# 2. JSON MEMORY PERSISTENCE
# ==============================================================================
def load_memory():
    default_mem = {
        "basket_date": "",
        "basket_version": "",
        "last_scanned_15m_bar": "",
        "und_slot_100d_cache": {},
        "options": []
    }
    if os.path.exists(MEMORY_FILE):
        try:
            with open(MEMORY_FILE, "r") as f:
                data = json.load(f)
                default_mem.update(data)
        except Exception as e:
            print(f"⚠️ Memory read warning: {e}")
    return default_mem

def save_memory(mem):
    try:
        with open(MEMORY_FILE, "w") as f:
            json.dump(mem, f, indent=2)
    except Exception as e:
        print(f"⚠️ Memory write warning: {e}")

# ==============================================================================
# 3. DEFINEDGE AUTHENTICATION & HISTORICAL DATA ENGINE (DAILY + 1M)
# ==============================================================================
def get_definedge_session_key(force_refresh=False):
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    if not force_refresh and SESSION_CACHE["key"] and SESSION_CACHE["date"] == today_str:
        return SESSION_CACHE["key"]

    for attempt in range(1, 4):
        try:
            url_step1 = f"https://signin.definedgesecurities.com/auth/realms/debroking/dsbpkc/login/{DEFINEDGE_API_TOKEN}"
            r1 = requests.get(url_step1, headers={"api_secret": DEFINEDGE_API_SECRET}, timeout=15)
            r1.raise_for_status()
            otp_token = r1.json().get("otp_token")

            totp_code = pyotp.TOTP(DEFINEDGE_TOTP_SECRET).now()
            url_step2 = "https://signin.definedgesecurities.com/auth/realms/debroking/dsbpkc/token"
            r2 = requests.post(url_step2, json={"otp_token": otp_token, "otp": totp_code}, timeout=15)
            r2.raise_for_status()
            session_key = r2.json().get("api_session_key")

            if session_key:
                SESSION_CACHE["key"] = session_key
                SESSION_CACHE["date"] = today_str
                print("🔑 Definedge session authenticated.")
                return session_key
        except Exception as e:
            print(f"⚠️ Definedge login attempt {attempt}/3 failed: {e}")
            time.sleep(4)
    raise RuntimeError("Could not authenticate with Definedge API.")

def fetch_definedge_day_df(segment, token, start_dt, end_dt):
    from_str = start_dt.strftime("%d%m%Y%H%M")
    to_str = end_dt.strftime("%d%m%Y%H%M")
    url = f"https://data.definedgesecurities.com/sds/history/{segment}/{token}/day/{from_str}/{to_str}"

    for attempt in range(1, 3):
        session_key = get_definedge_session_key(force_refresh=(attempt > 1))
        try:
            resp = requests.get(url, headers={"Authorization": session_key}, timeout=20)
            if resp.status_code == 401:
                continue
            if resp.status_code != 200 or not resp.text.strip():
                return pd.DataFrame()

            df = pd.read_csv(
                io.StringIO(resp.text.strip()),
                header=None,
                names=["datetime", "open", "high", "low", "close", "volume", "oi"]
            )
            if df.empty:
                return pd.DataFrame()

            raw_d = df["datetime"].astype(str).str.strip().str.slice(0, 8)
            df["date"] = pd.to_datetime(raw_d, format="%d%m%Y", errors="coerce").dt.date
            df = df.dropna(subset=["date"]).sort_values("date")

            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["open", "close"]).drop_duplicates(subset=["date"], keep="last")
            return df
        except Exception as e:
            if attempt == 2:
                print(f"⚠️ Daily data fetch error ({segment}/{token}): {e}")
            time.sleep(2)
    return pd.DataFrame()

def fetch_definedge_1m_df(segment, token, start_dt, end_dt):
    from_str = start_dt.strftime("%d%m%Y%H%M")
    to_str = end_dt.strftime("%d%m%Y%H%M")
    url = f"https://data.definedgesecurities.com/sds/history/{segment}/{token}/minute/{from_str}/{to_str}"

    for attempt in range(1, 3):
        session_key = get_definedge_session_key(force_refresh=(attempt > 1))
        try:
            resp = requests.get(url, headers={"Authorization": session_key}, timeout=20)
            if resp.status_code == 401:
                continue
            if resp.status_code != 200 or not resp.text.strip():
                return pd.DataFrame()

            df = pd.read_csv(
                io.StringIO(resp.text.strip()),
                header=None,
                names=["datetime", "open", "high", "low", "close", "volume", "oi"]
            )
            if df.empty:
                return pd.DataFrame()

            df["datetime"] = pd.to_datetime(df["datetime"].astype(str), format="%d%m%Y%H%M", errors="coerce")
            df = df.dropna(subset=["datetime"])
            df["datetime"] = df["datetime"].dt.tz_localize(IST)
            df = df.set_index("datetime").sort_index()

            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            return df.dropna(subset=["open", "close"])
        except Exception as e:
            if attempt == 2:
                print(f"⚠️ Historical 1m data fetch error ({segment}/{token}): {e}")
            time.sleep(2)
    return pd.DataFrame()

# ==============================================================================
# 4. MASTER SYMBOL PARSING & OFFICIAL DAILY CANDLE BASKET BUILDER
# ==============================================================================
def extract_strike_and_type_from_row(row, underlying, trading_symbol):
    opt_type = str(row[8]).strip().upper() if 8 in row.index and pd.notna(row[8]) else ""
    strike = None

    if 9 in row.index and pd.notna(row[9]):
        try:
            raw_strike = float(row[9])
            price_prec = int(row[10]) if (10 in row.index and pd.notna(row[10])) else 2
            multiplier = float(row[11]) if (11 in row.index and pd.notna(row[11]) and float(row[11]) > 0) else 1.0
            calc_strike = raw_strike / (multiplier * (10 ** price_prec))
            if calc_strike > 0:
                strike = round(calc_strike, 2)
        except Exception:
            strike = None

    if strike is None or opt_type not in ("CE", "PE"):
        p_cp = r"^" + re.escape(underlying) + r"\d{2}[A-Z]{3}\d{2}(C|P)(\d+(?:\.\d+)?)$"
        m1 = re.match(p_cp, trading_symbol)
        if m1:
            opt_type = "CE" if m1.group(1) == "C" else "PE"
            strike = float(m1.group(2))
        else:
            p_cepe = r"^" + re.escape(underlying) + r"\d{2}[A-Z]{3}\d{2}(\d+(?:\.\d+)?)(CE|PE)$"
            m2 = re.match(p_cepe, trading_symbol)
            if m2:
                strike = float(m2.group(1))
                opt_type = m2.group(2)

    if strike is not None and opt_type in ("CE", "PE"):
        return round(float(strike), 2), opt_type
    return None, None

def load_definedge_master_data():
    today = datetime.now(IST).date()
    stock_set = set(high_beta_50_stocks)

    url = "https://app.definedgesecurities.com/public/allmaster.zip"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pd.read_csv(f, header=None, dtype=str, low_memory=False)

    nse_eq = df[
        (df[0] == "NSE")
        & (df[2].str.strip().isin(stock_set))
        & (df[4].str.strip() == "EQ")
    ].copy()
    eq_token_map = {row[2].strip(): row[1].strip() for _, row in nse_eq.iterrows()}

    nfo = df[(df[0] == "NFO") & (df[2].str.strip().isin(stock_set))].copy()
    nfo[1] = nfo[1].str.strip()
    nfo[2] = nfo[2].str.strip()
    nfo[3] = nfo[3].str.strip()
    nfo[4] = nfo[4].str.strip()

    for exp_col in [5, 8, 6, 7]:
        if exp_col in nfo.columns:
            parsed = pd.to_datetime(nfo[exp_col], format="%d%m%Y", errors="coerce")
            if parsed.notna().sum() > 0:
                nfo["expiry"] = parsed.dt.date
                break

    nfo = nfo[nfo["expiry"].notna() & (nfo["expiry"] >= today)]

    fut_df = nfo[nfo[4] == "FUTSTK"].sort_values("expiry").groupby(2, as_index=False).first()
    fut_map = {
        row[2]: {"token": row[1], "trading_symbol": row[3], "expiry": row["expiry"]}
        for _, row in fut_df.iterrows()
    }

    opt_df = nfo[nfo[4] == "OPTSTK"].copy()
    min_opt_exp = opt_df.groupby(2)["expiry"].transform("min")
    opt_df = opt_df[opt_df["expiry"] == min_opt_exp]

    options_by_stock = {s: [] for s in stock_set}
    for _, row in opt_df.iterrows():
        und = row[2]
        trd_sym = row[3]
        strike, opt_type = extract_strike_and_type_from_row(row, und, trd_sym)
        if strike is not None and opt_type in ("CE", "PE"):
            options_by_stock[und].append({
                "underlying": und,
                "token": row[1],
                "trading_symbol": trd_sym,
                "strike": strike,
                "opt_type": opt_type,
                "expiry": str(row["expiry"])
            })

    return eq_token_map, fut_map, options_by_stock

def get_yesterday_daily_stats(eq_token, fut_token, today_date, now_ist):
    start_dt_day = (now_ist - timedelta(days=20)).replace(hour=9, minute=15, second=0, microsecond=0)

    df_fut_day = fetch_definedge_day_df("NFO", fut_token, start_dt_day, now_ist)
    df_fut_prev = df_fut_day[df_fut_day["date"] < today_date] if not df_fut_day.empty else pd.DataFrame()

    df_eq_day = fetch_definedge_day_df("NSE", eq_token, start_dt_day, now_ist) if eq_token else pd.DataFrame()
    df_eq_prev = df_eq_day[df_eq_day["date"] < today_date] if not df_eq_day.empty else pd.DataFrame()

    trend_df = df_eq_prev if len(df_eq_prev) >= 2 else df_fut_prev

    if trend_df.empty:
        df_1m = fetch_definedge_1m_df("NFO", fut_token, start_dt_day, now_ist)
        df_1m_prev = df_1m[df_1m.index.date < today_date].between_time("09:15", "15:29")
        if df_1m_prev.empty:
            return "FLAT", 0.0, 0.0
        trend_df = df_1m_prev.groupby(df_1m_prev.index.date).agg(
            open=("open", "first"),
            close=("close", "last")
        ).dropna()

    if trend_df.empty:
        return "FLAT", 0.0, 0.0

    yest_open = float(trend_df["open"].iloc[-1])
    yest_close = float(trend_df["close"].iloc[-1])

    if YESTERDAY_TREND_MODE.upper() == "CLOSE_VS_OPEN" or len(trend_df) < 2:
        ref_base = yest_open
    else:
        ref_base = float(trend_df["close"].iloc[-2])

    if not df_fut_prev.empty:
        strike_ref_price = float(df_fut_prev["close"].iloc[-1])
    else:
        strike_ref_price = yest_close

    if ref_base <= 0 or strike_ref_price <= 0:
        return "FLAT", 0.0, 0.0

    pct_chg = round(((yest_close - ref_base) / ref_base) * 100.0, 2)
    if yest_close < ref_base:
        return "DOWN", pct_chg, strike_ref_price
    elif yest_close > ref_base:
        return "UP", pct_chg, strike_ref_price
    return "FLAT", 0.0, strike_ref_price

def build_basket_task(now_ist):
    print("\n🎯 Building Filtered OTM Option Basket via Official Daily Candles (Down Yest -> CE | Up Yest -> PE)...")
    get_definedge_session_key()
    eq_token_map, fut_map, options_by_stock = load_definedge_master_data()

    today_date = now_ist.date()
    basket = []
    ce_count, pe_count = 0, 0

    for symbol in high_beta_50_stocks:
        fut_info = fut_map.get(symbol)
        opt_chain = options_by_stock.get(symbol, [])
        eq_token = eq_token_map.get(symbol, "")
        if not fut_info or not opt_chain:
            continue

        try:
            yest_dir, yest_pct, ref_price = get_yesterday_daily_stats(
                eq_token, fut_info["token"], today_date, now_ist
            )
            if yest_dir not in ("DOWN", "UP") or ref_price <= 0:
                continue

            selected_opt = None
            if yest_dir == "DOWN":
                ce_candidates = sorted(
                    [o for o in opt_chain if o["opt_type"] == "CE" and o["strike"] > ref_price],
                    key=lambda x: x["strike"]
                )
                if ce_candidates:
                    selected_opt = dict(ce_candidates[0])
                    ce_count += 1
            elif yest_dir == "UP":
                pe_candidates = sorted(
                    [o for o in opt_chain if o["opt_type"] == "PE" and o["strike"] < ref_price],
                    key=lambda x: x["strike"],
                    reverse=True
                )
                if pe_candidates:
                    selected_opt = dict(pe_candidates[0])
                    pe_count += 1

            if selected_opt:
                selected_opt["ref_fut_price"] = round(ref_price, 2)
                selected_opt["yest_dir"] = yest_dir
                selected_opt["yest_pct"] = yest_pct
                selected_opt["eq_token"] = eq_token
                selected_opt["fut_token"] = fut_info["token"]
                basket.append(selected_opt)
        except Exception as e:
            print(f"  ⚠️ Error selecting OTM option for {symbol}: {e}")

    print(f"🎯 Basket Ready: {len(basket)} OTM Contracts ({ce_count} CEs + {pe_count} PEs).")
    return basket

# ==============================================================================
# 5. 15m RESAMPLING, ATR(2), OPT VOL & INVERTED UNDERLYING SLOT VOL
# ==============================================================================
def resample_to_15m(df_1m, now_ist):
    if df_1m.empty:
        return pd.DataFrame()
    df = df_1m.copy().between_time("09:15", "15:29")
    if df.empty:
        return pd.DataFrame()

    resampled_chunks = []
    for _, group in df.groupby(df.index.date):
        bars = group.resample(
            "15min",
            origin="start_day",
            offset="9h15min",
            label="left",
            closed="left"
        ).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }).dropna(subset=["open", "close"])

        bars = bars[bars.index.time <= dtime(15, 15)]
        resampled_chunks.append(bars)

    if not resampled_chunks:
        return pd.DataFrame()

    df_15m = pd.concat(resampled_chunks).sort_index()
    today_date = now_ist.date()

    if (df_15m.index.date == today_date).any():
        completed_mask = [
            (ts.date() < today_date) or (now_ist >= ts + timedelta(minutes=15))
            for ts in df_15m.index
        ]
        df_15m = df_15m[completed_mask]

    return df_15m

def calculate_atr(df, period=2):
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)

    if ATR_METHOD.upper() == "RMA":
        return tr.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    return tr.rolling(window=period, min_periods=period).mean()

def evaluate_15m_option_breakout(df_15m):
    if len(df_15m) < ATR_PERIOD + 2:
        return None

    df = df_15m.copy()
    df["atr2"] = calculate_atr(df, ATR_PERIOD)

    cur_bar = df.iloc[-1]
    prev_bar = df.iloc[-2]

    cur_atr = float(cur_bar["atr2"])
    prev_atr = float(prev_bar["atr2"])
    cur_vol = float(cur_bar["volume"])
    prev_vol = float(prev_bar["volume"])

    if (
        pd.isna(cur_atr) or pd.isna(prev_atr) or prev_atr <= 0
        or pd.isna(cur_vol) or pd.isna(prev_vol) or prev_vol <= 0
    ):
        return None

    if not (
        float(cur_bar["close"]) > float(prev_bar["close"])
        and float(cur_bar["close"]) > float(prev_bar["high"])
        and cur_atr > (prev_atr * ATR_MULT_THRESHOLD)
        and cur_vol > (prev_vol * VOL_MULT_THRESHOLD)
    ):
        return None

    signal_ts = cur_bar.name + timedelta(minutes=15)

    return {
        "bar_ts": cur_bar.name,
        "bar_time": cur_bar.name.strftime("%d-%b %H:%M"),
        "bar_iso": cur_bar.name.strftime("%Y-%m-%d %H:%M"),
        "slot_hm": cur_bar.name.strftime("%H:%M"),
        "signal_hm": signal_ts.strftime("%H:%M"),
        "close": round(float(cur_bar["close"]), 2),
        "atr_ratio": round(cur_atr / prev_atr, 2),
        "vol": int(cur_vol),
        "vol_ratio": round(cur_vol / prev_vol, 2)
    }

def get_underlying_respective_slot_vol_ratio(opt, bar_ts, now_ist, memory):
    symbol = opt["underlying"]
    eq_token = opt.get("eq_token", "")
    fut_token = opt.get("fut_token", "")
    slot_hm = bar_ts.strftime("%H:%M")
    target_date = bar_ts.date()
    slot_cache = memory.setdefault("und_slot_100d_cache", {})

    if symbol not in slot_cache or not slot_cache.get(symbol):
        start_dt_100d = (now_ist - timedelta(days=LOOKBACK_DAYS_100D_SLOT)).replace(hour=9, minute=15, second=0, microsecond=0)
        if eq_token:
            df_hist_1m = fetch_definedge_1m_df("NSE", eq_token, start_dt_100d, now_ist)
        else:
            df_hist_1m = fetch_definedge_1m_df("NFO", fut_token, start_dt_100d, now_ist)

        df_hist_15m = resample_to_15m(df_hist_1m, now_ist)
        slot_avgs = {}
        if not df_hist_15m.empty:
            df_prior = df_hist_15m[df_hist_15m.index.date < target_date].copy()
            df_prior = df_prior[df_prior["volume"] > 0]
            if not df_prior.empty:
                df_prior["slot_hm"] = df_prior.index.strftime("%H:%M")
                for s_hm, grp in df_prior.groupby("slot_hm"):
                    last_100 = grp["volume"].tail(100)
                    if not last_100.empty:
                        slot_avgs[s_hm] = int(round(float(last_100.mean())))
        slot_cache[symbol] = slot_avgs

    avg_100d_slot_vol = int(slot_cache.get(symbol, {}).get(slot_hm, 0))

    start_today = IST.localize(datetime.combine(target_date, dtime(9, 15)))
    if eq_token:
        df_today_1m = fetch_definedge_1m_df("NSE", eq_token, start_today, now_ist)
    else:
        df_today_1m = fetch_definedge_1m_df("NFO", fut_token, start_today, now_ist)

    df_today_15m = resample_to_15m(df_today_1m, now_ist)
    today_slot_vol = 0
    if not df_today_15m.empty:
        matching_bars = df_today_15m[
            (df_today_15m.index.date == target_date)
            & (df_today_15m.index.strftime("%H:%M") == slot_hm)
        ]
        if not matching_bars.empty:
            today_slot_vol = int(round(float(matching_bars["volume"].iloc[-1])))

    if avg_100d_slot_vol > 0 and today_slot_vol > 0:
        ratio = round(float(today_slot_vol) / float(avg_100d_slot_vol), 2)
    else:
        ratio = 0.0

    return ratio, avg_100d_slot_vol, today_slot_vol

# ==============================================================================
# 6. 15px LARGE-FONT HTML EMAIL SENDER (SORTED BY UND SLOT VOL RATIO)
# ==============================================================================
def send_otm_15m_email_alert(triggered_list, bar_label, total_tracked):
    now_str = datetime.now(IST).strftime("%d-%b %H:%M:%S")
    subject = f"⚡ 15m OTM Options Breakout ({len(triggered_list)} Matches) - [{bar_label}] ({now_str})"

    sort_key = lambda x: (x.get("und_vol_ratio", 0.0), x.get("atr_ratio", 0.0))
    ce_matches = sorted([m for m in triggered_list if m["opt_type"] == "CE"], key=sort_key, reverse=True)
    pe_matches = sorted([m for m in triggered_list if m["opt_type"] == "PE"], key=sort_key, reverse=True)
    ordered_matches = ce_matches + pe_matches

    ce_top5 = ", ".join([m["trading_symbol"] for m in ce_matches[:5]]) or "None"
    pe_top5 = ", ".join([m["trading_symbol"] for m in pe_matches[:5]]) or "None"
    ce_all = ", ".join([m["trading_symbol"] for m in ce_matches]) or "None"
    pe_all = ", ".join([m["trading_symbol"] for m in pe_matches]) or "None"

    headers = [
        "Stock (Yest %)",
        "Option Symbol",
        "15m Slot (Sig)",
        "Close",
        "ATR Ratio",
        "Opt Vol Ratio",
        "Und Slot Vol Ratio (Today/100dAvg)"
    ]

    table_rows = [
        [
            f"{m['underlying']} ({m.get('yest_pct', 0):+.1f}%)",
            m["trading_symbol"],
            f"{m['slot_hm']} ({m.get('signal_hm', '')})",
            f"{m['close']:.2f}",
            f"{m['atr_ratio']:.2f}x",
            f"{m['vol_ratio']:.2f}x ({m['vol']:,})",
            f"{m.get('und_vol_ratio', 0):.2f}x ({m.get('und_today_slot_vol', 0):,}/{m.get('und_avg100_slot_vol', 0):,})"
        ]
        for m in ordered_matches
    ]

    plain_body = tabulate(table_rows, headers=headers, tablefmt="simple")
    print(f"\n📊 {subject}\n{plain_body}\n")

    if not SENDER_EMAIL or not RECEIVER_EMAIL or not APP_PASSWORD:
        print(
            f"⚠️ Email credentials check -> SENDER: {'SET' if SENDER_EMAIL else 'MISSING'} | "
            f"RECEIVER: {'SET' if RECEIVER_EMAIL else 'MISSING'} | "
            f"APP_PASSWORD: {'SET' if APP_PASSWORD else 'MISSING (Check workflow .yml env section)'}. Skipping email dispatch."
        )
        return

    for attempt in range(1, 4):
        try:
            th_html = "".join(
                f'<th style="background-color:#1e293b;color:#ffffff;padding:10px 14px;border:1px solid #cbd5e1;text-align:left;white-space:nowrap;font-size:15px;font-weight:600;">{h}</th>'
                for h in headers
            )
            tr_list = []
            for idx, r in enumerate(table_rows):
                bg = "#f8fafc" if idx % 2 == 1 else "#ffffff"
                tds = "".join(
                    f'<td style="padding:9px 14px;border:1px solid #cbd5e1;white-space:nowrap;font-size:15px;color:#0f172a;text-align:{"right" if i >= 3 else "left"};">{cell}</td>'
                    for i, cell in enumerate(r)
                )
                tr_list.append(f'<tr style="background-color:{bg};">{tds}</tr>')

            html_body = f"""
            <html>
            <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; font-size: 15px; color: #0f172a;">
                <p style="margin:0 0 8px 0;font-size:16px;"><strong>⚡ 15m OTM Options Breakout Alert (Ranked by Und Slot Vol Ratio)</strong></p>
                <p style="margin:0 0 14px 0; color:#475569;font-size:15px;">
                    <strong>15m Bar:</strong> {bar_label} &nbsp;|&nbsp;
                    <strong>Basket Size:</strong> {total_tracked} Filtered OTM Options
                </p>
                <p style="margin:0 0 4px 0;"><strong>📋 OTM CALLS (CE - Ranked by Und Slot Vol Ratio) - Top 5:</strong></p>
                <pre style="background:#f1f5f9;padding:10px;border-radius:4px;margin:0 0 12px 0;white-space:pre-wrap;font-size:14px;">{ce_top5}</pre>
                <p style="margin:0 0 4px 0;"><strong>📋 OTM PUTS (PE - Ranked by Und Slot Vol Ratio) - Top 5:</strong></p>
                <pre style="background:#f1f5f9;padding:10px;border-radius:4px;margin:0 0 12px 0;white-space:pre-wrap;font-size:14px;">{pe_top5}</pre>
                <p style="margin:0 0 14px 0;font-size:14px;color:#334155;">
                    <strong>All CE ({len(ce_matches)}):</strong> {ce_all}<br>
                    <strong>All PE ({len(pe_matches)}):</strong> {pe_all}
                </p>
                <div style="overflow-x:auto;">
                    <table style="border-collapse:collapse;font-family:'SFMono-Regular',Consolas,monospace;">
                        <thead><tr>{th_html}</tr></thead>
                        <tbody>{"".join(tr_list)}</tbody>
                    </table>
                </div>
            </body>
            </html>
            """

            msg = MIMEMultipart("alternative")
            msg["From"] = SENDER_EMAIL
            msg["To"] = RECEIVER_EMAIL
            msg["Subject"] = subject
            msg.attach(MIMEText(plain_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
                server.login(SENDER_EMAIL, APP_PASSWORD)
                server.send_message(msg)

            print(f"✅ Alert Email sent successfully to {RECEIVER_EMAIL} for {len(triggered_list)} contract(s)!")
            return
        except Exception as e:
            print(f"⚠️ Email attempt {attempt}/3 failed: {e}")
            time.sleep(3)

# ==============================================================================
# 7. CORE SCANNER & MARKET-CLOCK SCHEDULER
# ==============================================================================
def get_expected_15m_bar_label(now_ist):
    t = now_ist.time()
    if t < dtime(9, 30):
        return None
    capped_dt = now_ist.replace(second=0, microsecond=0)
    if t >= dtime(15, 30):
        bar_start = now_ist.replace(hour=15, minute=15, second=0, microsecond=0)
    else:
        mins_since_open = (capped_dt.hour * 60 + capped_dt.minute) - (9 * 60 + 15)
        completed_bars = mins_since_open // 15
        bar_start_mins = (9 * 60 + 15) + (completed_bars - 1) * 15
        bar_start = now_ist.replace(hour=bar_start_mins // 60, minute=bar_start_mins % 60, second=0, microsecond=0)
    return bar_start.strftime("%Y-%m-%d %H:%M")

def run_scanner(force_scan=False):
    now_ist = datetime.now(IST)
    today_str = now_ist.strftime("%Y-%m-%d")

    memory = load_memory()
    if memory.get("basket_date") != today_str:
        memory["und_slot_100d_cache"] = {}

    need_new_basket = (
        memory.get("basket_date") != today_str
        or memory.get("basket_version") != BASKET_VERSION
        or not memory.get("options")
    )

    if need_new_basket:
        new_basket = build_basket_task(now_ist)
        if new_basket:
            memory["basket_date"] = today_str
            memory["basket_version"] = BASKET_VERSION
            memory["options"] = new_basket
            save_memory(memory)

    options_list = memory.get("options", [])
    if not options_list:
        print("⚠️ No OTM Option contracts found in basket.")
        return

    expected_bar_label = get_expected_15m_bar_label(now_ist)
    if not force_scan and expected_bar_label is not None:
        if memory.get("last_scanned_15m_bar") == expected_bar_label:
            print(f"ℹ️ Bar [{expected_bar_label}] already scanned. Skipping duplicate check.")
            return

    print(f"\n🔎 Scanning {len(options_list)} Filtered OTM Options (Target Bar: {expected_bar_label or 'LATEST'})...")
    start_dt_15m = (now_ist - timedelta(days=LOOKBACK_DAYS_15M)).replace(hour=9, minute=15, second=0, microsecond=0)

    triggered_alerts = []
    latest_bar_seen = expected_bar_label or ""

    for opt in options_list:
        trd_sym = opt["trading_symbol"]
        try:
            df_1m = fetch_definedge_1m_df("NFO", opt["token"], start_dt_15m, now_ist)
            if df_1m.empty:
                continue

            df_15m = resample_to_15m(df_1m, now_ist)
            if df_15m.empty:
                continue

            res = evaluate_15m_option_breakout(df_15m)
            latest_bar_seen = df_15m.index[-1].strftime("%Y-%m-%d %H:%M")

            if res:
                # Strict Stale-Bar Guard (Matches Dhan Cloud 1-to-1)
                if expected_bar_label and res["bar_iso"] != expected_bar_label:
                    continue

                bar_ts = res.pop("bar_ts")
                und_ratio, avg100_slot, today_slot = get_underlying_respective_slot_vol_ratio(opt, bar_ts, now_ist, memory)

                alert_item = {
                    **opt,
                    **res,
                    "und_vol_ratio": und_ratio,
                    "und_avg100_slot_vol": avg100_slot,
                    "und_today_slot_vol": today_slot
                }
                triggered_alerts.append(alert_item)
                print(
                    f"  🔥 MATCH: {trd_sym} ({opt['opt_type']} | Slot {res['slot_hm']} -> Sig {res['signal_hm']} | "
                    f"Und Slot Vol [Today/100dAvg]: {und_ratio:.2f}x | "
                    f"ATR Ratio: {res['atr_ratio']}x | Opt Vol: {res['vol_ratio']}x)"
                )
        except Exception as e:
            print(f"  ⚠️ Error scanning {trd_sym}: {e}")

    if expected_bar_label:
        memory["last_scanned_15m_bar"] = expected_bar_label
    elif latest_bar_seen:
        memory["last_scanned_15m_bar"] = latest_bar_seen
    save_memory(memory)

    if triggered_alerts:
        send_otm_15m_email_alert(triggered_alerts, expected_bar_label or latest_bar_seen, len(options_list))
    else:
        print(f"ℹ️ No 15m OTM Options matched on bar [{expected_bar_label or latest_bar_seen}].")

def heartbeat_sleep(target_dt):
    while True:
        now_ist = datetime.now(IST)
        rem = (target_dt - now_ist).total_seconds()
        if rem <= 0:
            break
        step = min(30.0, rem)
        time.sleep(step)

def stay_awake_market_clock():
    now_ist = datetime.now(IST)
    session_mode = os.environ.get("SESSION_MODE", "AUTO").upper()
    print(f"🕒 Starting 15m OTM Scanner Clock at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')} (Mode: {session_mode})")

    is_weekday = now_ist.weekday() < 5
    in_market_window = is_weekday and (dtime(8, 55) <= now_ist.time() <= dtime(15, 35))

    if not in_market_window:
        print("⚡ Running immediate single-pass scan (outside live schedule window)...")
        run_scanner(force_scan=True)
        return

    if session_mode == "MORNING":
        session_cutoff = dtime(12, 32)
    else:
        session_cutoff = dtime(15, 32)

    memory = load_memory()
    today_str = now_ist.strftime("%Y-%m-%d")
    if (
        memory.get("basket_date") != today_str
        or memory.get("basket_version") != BASKET_VERSION
        or not memory.get("options")
    ):
        print("🌅 Pre-warming today's Official Daily Close OTM Option basket...")
        new_basket = build_basket_task(now_ist)
        if new_basket:
            memory["basket_date"] = today_str
            memory["basket_version"] = BASKET_VERSION
            memory["options"] = new_basket
            save_memory(memory)

    while True:
        now_ist = datetime.now(IST)
        if now_ist.time() > session_cutoff:
            print(f"⏹️ Reached session cutoff ({session_cutoff}). Exiting cleanly.")
            break

        today_date = now_ist.date()
        future_points = [
            IST.localize(datetime.combine(today_date, cp))
            for cp in SCAN_CHECKPOINTS_IST
            if cp > now_ist.time() and cp <= session_cutoff
        ]

        if not future_points:
            print(f"✅ All checkpoints for {session_mode} completed ({now_ist.strftime('%H:%M:%S IST')}).")
            break

        next_dt = future_points[0]
        sleep_secs = (next_dt - now_ist).total_seconds()
        print(f"💤 Sleeping {round(sleep_secs / 60, 1)} min until next checkpoint: {next_dt.strftime('%H:%M:%S IST')}...")
        heartbeat_sleep(next_dt)

        now_ist = datetime.now(IST)
        print(f"\n🔔 Checkpoint reached at {now_ist.strftime('%H:%M:%S IST')}. Running 15m OTM scanner...")
        try:
            run_scanner(force_scan=False)
        except Exception as e:
            print(f"⚠️ Error during checkpoint scan: {e}")

if __name__ == "__main__":
    stay_awake_market_clock()
