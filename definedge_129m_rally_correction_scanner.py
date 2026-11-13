import os
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
from concurrent.futures import ThreadPoolExecutor
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
MEMORY_FILE = "rally_corr_129m_memory.json"

# ==============================================================================
# 129-MINUTE IMPULSE RALLY + SLOW TIME-CORRECTION PARAMETERS
# ==============================================================================
DATA_SEGMENT = "NSE"                  # "NSE" (Continuous Spot Cash) or "NFO" (Current Month Futures)
ATR_FAST_PERIOD = 2                   # ATR(2) for Wide-Range Bar thrust check
ATR_SLOW_PERIOD = 100                 # ATR(100) baseline for small-body & rally height
ATR_METHOD = "SMA"                    # "SMA" or "RMA"

# Phase 1: Impulse Rally Rules
WRB_ATR2_MULT = 1.5                   # At least 1 rally bar must have ATR(2) >= 1.5x Prev ATR(2)
MIN_WRB_COUNT_IN_RALLY = 1            # Minimum wide-range bullish bars in the rally
MAX_DOWN_BARS_IN_RALLY = 3            # Max 1, 2, or 3 small-body corrective down bars allowed in rally
SMALL_DOWN_BODY_ATR100_MAX = 0.67     # Down bar body inside rally must be <= 0.67 * ATR(100)
SMALL_DOWN_BODY_VS_UP_MAX = 0.55      # Down bar body inside rally must be <= 55% of max bullish rally body
MIN_RALLY_BARS = 1                    # Minimum bars in the impulsive rally
MAX_RALLY_BARS = 12                   # Maximum bars in the impulsive rally
MIN_RALLY_RANGE_ATR100 = 1.5          # Total rally height (Peak High - Start Low) >= 1.5x ATR(100)

# Phase 2: Slow Correction Rules (Time Ratio = Correction Bars / Rally Bars > 1.0)
MAX_CORRECTION_BARS = 35              # Max bars to scan for an active correction
MAX_RETRACEMENT_OF_RALLY = 0.85       # Correction Low must stay strictly above Rally Start Low (<= 85% retracement)

# Lookback (~95 calendar days = ~195 bars of 129m data for accurate ATR(100))
LOOKBACK_DAYS_1M = 95

# 3 Daily 129m Candle Close Checkpoints:
# Bar 1: 09:15 - 11:24 | Bar 2: 11:24 - 13:33 | Bar 3: 13:33 - 15:30
SCAN_CHECKPOINTS_IST = [
    dtime(11, 24, 15),
    dtime(13, 33, 15),
    dtime(15, 30, 15)
]

# ==============================================================================
# HIGH-BETA NSE F&O UNIVERSE (102 STOCKS INCLUDING KALYANJWL & PAYTM)
# ==============================================================================
high_beta_stocks = [
    "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "BANKBARODA", "CANBK", "CHOLAFIN",
    "FEDERALBNK", "HDFCBANK", "ICICIBANK", "IDFCFIRSTB", "INDUSINDBK", "KOTAKBANK",
    "PNB", "RBLBANK", "SBIN", "SHRIRAMFIN", "AUBANK", "BANDHANBNK",
    "ABCAPITAL", "LICHSGFIN", "LTF", "MANAPPURAM",
    "ANGELONE", "BSE", "CDSL", "MCX", "CAMS", "HDFCAMC", "JIOFIN", "MUTHOOTFIN", "PAYTM",
    "BDL", "BEL", "BHEL", "HAL", "IRFC", "MAZDOCK", "PFC", "RECLTD",
    "TATAPOWER", "JSWENERGY", "NTPC", "POWERGRID", "COALINDIA", "ONGC",
    "BPCL", "HINDPETRO", "IOC", "GAIL", "IREDA", "RVNL",
    "ABB", "CGPOWER", "CUMMINSIND", "DIXON", "KAYNES", "POLYCAB", "SIEMENS",
    "HAVELLS", "VOLTAS", "BHARATFORG", "ASTRAL",
    "ADANIENT", "ADANIPORTS", "ADANIGREEN", "ADANIENSOL", "HINDALCO",
    "JINDALSTEL", "JSWSTEEL", "TATASTEEL", "VEDL", "NMDC", "NATIONALUM",
    "BAJAJ-AUTO", "M&M", "MARUTI", "MOTHERSON", "TMPV", "TVSMOTOR",
    "EICHERMOT", "ASHOKLEY", "TRENT", "ETERNAL", "NYKAA", "KALYANJWL",
    "BHARTIARTL", "INDIGO", "INDUSTOWER",
    "COFORGE", "PERSISTENT", "OFSS", "INFY", "TCS", "HCLTECH", "TECHM", "KPITTECH",
    "DLF", "GODREJPROP", "OBEROIRLTY", "LT", "DIVISLAB", "LAURUSLABS"
]

SESSION_CACHE = {"key": "", "date": ""}

# ==============================================================================
# 2. JSON MEMORY PERSISTENCE
# ==============================================================================
def load_memory():
    default_mem = {"last_scanned_129m_bar": ""}
    if os.path.exists(MEMORY_FILE):
        try:
            with open(MEMORY_FILE, "r") as f:
                default_mem.update(json.load(f))
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
# 3. DEFINEDGE AUTHENTICATION & 1-MINUTE HISTORICAL ENGINE (LEADING-0 SAFE)
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

def load_definedge_tokens():
    today = datetime.now(IST).date()
    stock_set = set(high_beta_stocks)

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
    eq_map = {row[2].strip(): row[1].strip() for _, row in nse_eq.iterrows()}

    nfo = df[
        (df[0] == "NFO")
        & (df[2].str.strip().isin(stock_set))
        & (df[4].str.strip() == "FUTSTK")
    ].copy()
    nfo[1] = nfo[1].str.strip()
    nfo[2] = nfo[2].str.strip()
    nfo[3] = nfo[3].str.strip()

    for exp_col in [5, 8, 6, 7]:
        if exp_col in nfo.columns:
            parsed = pd.to_datetime(nfo[exp_col].str.strip().str.zfill(8), format="%d%m%Y", errors="coerce")
            if parsed.notna().sum() > 0:
                nfo["expiry"] = parsed.dt.date
                break

    nfo = nfo[nfo["expiry"].notna() & (nfo["expiry"] >= today)]
    fut_df = nfo.sort_values("expiry").groupby(2, as_index=False).first()
    fut_map = {row[2]: row[1] for _, row in fut_df.iterrows()}

    return eq_map, fut_map

def fetch_definedge_1m_df(segment, token, start_dt, end_dt):
    from_str = start_dt.strftime("%d%m%Y%H%M")
    to_str = end_dt.strftime("%d%m%Y%H%M")
    url = f"https://data.definedgesecurities.com/sds/history/{segment}/{token}/minute/{from_str}/{to_str}"

    for attempt in range(1, 3):
        session_key = get_definedge_session_key(force_refresh=(attempt > 1))
        try:
            resp = requests.get(url, headers={"Authorization": session_key}, timeout=25)
            if resp.status_code == 401:
                continue
            if resp.status_code != 200 or not resp.text.strip():
                return pd.DataFrame()

            df = pd.read_csv(
                io.StringIO(resp.text.strip()),
                header=None,
                names=["datetime", "open", "high", "low", "close", "volume", "oi"],
                dtype=str
            )
            if df.empty:
                return pd.DataFrame()

            raw_dt = df["datetime"].astype(str).str.strip().str.zfill(12)
            df["datetime"] = pd.to_datetime(raw_dt, format="%d%m%Y%H%M", errors="coerce")
            df = df.dropna(subset=["datetime"])
            df["datetime"] = df["datetime"].dt.tz_localize(IST)
            df = df.set_index("datetime").sort_index()

            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            return df.dropna(subset=["open", "high", "low", "close"])
        except Exception as e:
            if attempt == 2:
                print(f"⚠️ 1m fetch error ({segment}/{token}): {e}")
            time.sleep(2)
    return pd.DataFrame()

# ==============================================================================
# 4. 129-MINUTE RESAMPLING & INDICATORS (ATR 2, ATR 100, STOCH 5,3,3)
# ==============================================================================
def get_bar_close_dt(bar_start_ts):
    t = bar_start_ts.time()
    if t == dtime(9, 15):
        return bar_start_ts + timedelta(minutes=129)  # Closes at 11:24
    elif t == dtime(11, 24):
        return bar_start_ts + timedelta(minutes=129)  # Closes at 13:33
    else:
        return bar_start_ts.replace(hour=15, minute=30, second=0, microsecond=0)  # Closes at 15:30

def resample_to_129m(df_1m, now_ist):
    if df_1m.empty:
        return pd.DataFrame()
    df = df_1m.copy().between_time("09:15", "15:29")
    if df.empty:
        return pd.DataFrame()

    chunks = []
    for _, group in df.groupby(df.index.date):
        bars = group.resample(
            "129min",
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

        bars = bars[bars.index.time <= dtime(13, 33)]
        chunks.append(bars)

    if not chunks:
        return pd.DataFrame()

    df_129m = pd.concat(chunks).sort_index()
    today_date = now_ist.date()

    if (df_129m.index.date == today_date).any():
        completed_mask = [
            (ts.date() < today_date) or (now_ist >= get_bar_close_dt(ts))
            for ts in df_129m.index
        ]
        df_129m = df_129m[completed_mask]

    return df_129m

def compute_indicators_129m(df_129m):
    df = df_129m.copy()
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs()
    ], axis=1).max(axis=1)

    if ATR_METHOD.upper() == "RMA":
        df["atr2"] = tr.ewm(alpha=1.0 / ATR_FAST_PERIOD, min_periods=ATR_FAST_PERIOD, adjust=False).mean()
        df["atr100"] = tr.ewm(alpha=1.0 / ATR_SLOW_PERIOD, min_periods=20, adjust=False).mean()
    else:
        df["atr2"] = tr.rolling(window=ATR_FAST_PERIOD, min_periods=ATR_FAST_PERIOD).mean()
        df["atr100"] = tr.rolling(window=ATR_SLOW_PERIOD, min_periods=20).mean()

    df["prev_atr2"] = df["atr2"].shift(1)
    df["atr2_ratio"] = np.where(df["prev_atr2"] > 0, df["atr2"] / df["prev_atr2"], 0.0)
    df["body"] = (df["close"] - df["open"]).abs()

    # Stochastic (5, 3, 3)
    low_5 = df["low"].rolling(5, min_periods=5).min()
    high_5 = df["high"].rolling(5, min_periods=5).max()
    fast_k = np.where((high_5 - low_5) > 0, ((df["close"] - low_5) / (high_5 - low_5)) * 100.0, 50.0)
    df["stoch_k"] = pd.Series(fast_k, index=df.index).rolling(3, min_periods=3).mean()
    df["stoch_d"] = df["stoch_k"].rolling(3, min_periods=3).mean()

    # Bullish Wide-Range Bar (WRB): Bullish candle + ATR(2) >= 1.5x Prev ATR(2)
    df["is_bull_wrb"] = (
        (df["close"] > df["open"])
        & (df["close"] > prev_close)
        & (df["atr2_ratio"] >= WRB_ATR2_MULT)
    )
    return df

# ==============================================================================
# 5. PATTERN DETECTOR: WRB IMPULSE RALLY + SLOWER TIME-CORRECTION ABOVE BASE
# ==============================================================================
def check_rally_and_correction_window(df, start_idx, peak_idx, end_corr_idx):
    """
    Validates whether [start_idx .. peak_idx] is a valid WRB Impulse Rally
    and [peak_idx + 1 .. end_corr_idx] is a valid Slow Time-Correction:
      1. Rally has >= 1 Bullish WRB (ATR(2) >= 1.5x previous bar ATR(2)).
      2. Rally has 0, 1, 2, or max 3 small-body corrective down bars.
      3. Correction consumes strictly MORE time than the rally (corr_bars > rally_bars).
      4. Correction stays strictly above the starting point of the rally.
    """
    rally_bars = peak_idx - start_idx + 1
    corr_bars = end_corr_idx - peak_idx

    if rally_bars < MIN_RALLY_BARS or rally_bars > MAX_RALLY_BARS:
        return None
    if corr_bars <= rally_bars or corr_bars > MAX_CORRECTION_BARS:
        return None

    rally_slice = df.iloc[start_idx : peak_idx + 1]
    corr_slice = df.iloc[peak_idx + 1 : end_corr_idx + 1]

    # Starting bar of the rally must be a bullish bar and the lowest point of the rally
    first_bar = rally_slice.iloc[0]
    if float(first_bar["close"]) <= float(first_bar["open"]):
        return None

    rally_start_low = float(first_bar["low"])
    if float(rally_slice["low"].min()) < rally_start_low:
        return None

    # Peak bar must be the highest point of the rally
    rally_peak_high = float(rally_slice.iloc[-1]["high"])
    if float(rally_slice["high"].max()) > rally_peak_high:
        return None

    rally_range = rally_peak_high - rally_start_low
    ref_atr100 = float(df["atr100"].iloc[end_corr_idx] or 0.0)
    if ref_atr100 <= 0 or rally_range < (MIN_RALLY_RANGE_ATR100 * ref_atr100):
        return None

    # Rule 1: At least 1 Bullish Wide-Range Bar (ATR(2) >= 1.5x Prev ATR(2)) inside the rally
    wrb_bars = rally_slice[rally_slice["is_bull_wrb"]]
    if len(wrb_bars) < MIN_WRB_COUNT_IN_RALLY:
        return None

    max_wrb_atr_ratio = round(float(wrb_bars["atr2_ratio"].max()), 2)

    # Rule 2: 0, 1, 2, or max 3 Small-Body Corrective Down Bars within the rally
    down_mask = rally_slice["close"] < rally_slice["open"]
    down_bars = rally_slice[down_mask]
    up_bars = rally_slice[~down_mask]

    if len(down_bars) > MAX_DOWN_BARS_IN_RALLY or up_bars.empty:
        return None

    max_up_body = float(up_bars["body"].max())
    if not down_bars.empty:
        for _, d_row in down_bars.iterrows():
            d_body = float(d_row["body"])
            d_atr100 = float(d_row["atr100"] or ref_atr100)
            if d_body > (SMALL_DOWN_BODY_ATR100_MAX * d_atr100) or d_body > (SMALL_DOWN_BODY_VS_UP_MAX * max_up_body):
                return None

    # Rule 3: Correction stays strictly above Rally Start Low & has not broken above Rally Peak
    corr_low = float(corr_slice["low"].min())
    corr_high = float(corr_slice["high"].max())
    corr_max_close = float(corr_slice["close"].max())

    if corr_low <= rally_start_low:
        return None

    if corr_max_close > rally_peak_high or corr_high > (rally_peak_high + 0.05 * rally_range):
        return None

    retracement_pct = round(((rally_peak_high - corr_low) / rally_range) * 100.0, 1)
    if retracement_pct < 10.0 or retracement_pct > (MAX_RETRACEMENT_OF_RALLY * 100.0):
        return None

    time_ratio = round(float(corr_bars) / float(rally_bars), 2)

    return {
        "rally_bars": int(rally_bars),
        "corr_bars": int(corr_bars),
        "time_ratio": time_ratio,
        "wrb_count": int(len(wrb_bars)),
        "down_bars_in_rally": int(len(down_bars)),
        "max_wrb_atr2_ratio": max_wrb_atr_ratio,
        "rally_start_low": round(rally_start_low, 2),
        "rally_peak_high": round(rally_peak_high, 2),
        "corr_low": round(corr_low, 2),
        "retracement_pct": retracement_pct,
        "rally_gain_pct": round(((rally_peak_high - rally_start_low) / rally_start_low) * 100.0, 1)
    }

def evaluate_129m_rally_slow_correction(df_129m):
    if len(df_129m) < 35:
        return None

    df = compute_indicators_129m(df_129m)
    cur_idx = len(df) - 1
    cur_bar = df.iloc[cur_idx]

    best_match = None
    min_peak_idx = max(10, cur_idx - MAX_CORRECTION_BARS)

    for peak_idx in range(cur_idx - 2, min_peak_idx - 1, -1):
        min_start_idx = max(2, peak_idx - MAX_RALLY_BARS + 1)
        # Anchor to the earliest valid start_idx (full rally base) for each candidate peak_idx
        full_rally_for_peak = None
        for start_idx in range(min_start_idx, peak_idx + 1):
            res = check_rally_and_correction_window(df, start_idx, peak_idx, cur_idx)
            if res:
                full_rally_for_peak = res
                break

        if full_rally_for_peak:
            score = (full_rally_for_peak["time_ratio"], full_rally_for_peak["max_wrb_atr2_ratio"])
            if best_match is None or score > (best_match["time_ratio"], best_match["max_wrb_atr2_ratio"]):
                best_match = full_rally_for_peak

    if best_match is None:
        return None

    return {
        "bar_iso": cur_bar.name.strftime("%Y-%m-%d %H:%M"),
        "slot_hm": cur_bar.name.strftime("%d-%b %H:%M"),
        "close": round(float(cur_bar["close"]), 2),
        "cur_atr2_ratio": round(float(cur_bar["atr2_ratio"]), 2),
        "stoch_k": round(float(cur_bar["stoch_k"] or 0.0), 1),
        **best_match
    }

# ==============================================================================
# 6. 15px HTML EMAIL ALERT SENDER (SORTED BY MAX TIME RATIO AT TOP)
# ==============================================================================
def send_129m_pattern_email(triggered_list, bar_label, total_scanned):
    now_str = datetime.now(IST).strftime("%d-%b %H:%M:%S")
    subject = f"🔥 129m WRB Rally + Slow Correction ({len(triggered_list)} Matches) - [{bar_label}] ({now_str})"

    # Sort with MAX Time Ratio (Correction Time / Rally Time) at the top
    ordered = sorted(
        triggered_list,
        key=lambda x: (x["time_ratio"], x["max_wrb_atr2_ratio"]),
        reverse=True
    )

    top5_syms = ", ".join([m["symbol"] for m in ordered[:5]]) or "None"
    all_syms = ", ".join([m["symbol"] for m in ordered]) or "None"

    headers = [
        "Stock",
        "Time Ratio (Corr/Rally)",
        "Corr Bars",
        "Rally Bars (WRB | Red)",
        "Max Rally ATR(2) Ratio",
        "Close",
        "Rally Base -> Peak",
        "Corr Low (Retr %)",
        "Stoch K (5,3,3)"
    ]

    table_rows = [
        [
            m["symbol"],
            f"{m['time_ratio']:.2f}x",
            f"{m['corr_bars']} bars",
            f"{m['rally_bars']}b ({m['wrb_count']} WRB | {m['down_bars_in_rally']} Red)",
            f"{m['max_wrb_atr2_ratio']:.2f}x",
            f"{m['close']:.2f}",
            f"{m['rally_start_low']:.1f} -> {m['rally_peak_high']:.1f} (+{m['rally_gain_pct']:.1f}%)",
            f"{m['corr_low']:.1f} ({m['retracement_pct']:.0f}%)",
            f"{m['stoch_k']:.1f}"
        ]
        for m in ordered
    ]

    plain_body = tabulate(table_rows, headers=headers, tablefmt="simple")
    print(f"\n📊 {subject}\n{plain_body}\n")

    if not SENDER_EMAIL or not RECEIVER_EMAIL or not APP_PASSWORD:
        print("⚠️ Email credentials missing. Skipping email dispatch.")
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
                    f'<td style="padding:9px 14px;border:1px solid #cbd5e1;white-space:nowrap;font-size:15px;color:#0f172a;text-align:{"right" if i in (1, 2, 4, 5, 8) else "left"};font-weight:{"600" if i in (0, 1) else "400"};">{cell}</td>'
                    for i, cell in enumerate(r)
                )
                tr_list.append(f'<tr style="background-color:{bg};">{tds}</tr>')

            html_body = f"""
            <html>
            <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; font-size: 15px; color: #0f172a;">
                <p style="margin:0 0 8px 0;font-size:16px;"><strong>🔥 129m WRB Impulse Rally + Slow Time-Correction (Sorted by Max Time Ratio)</strong></p>
                <p style="margin:0 0 14px 0; color:#475569;font-size:15px;">
                    <strong>129m Bar:</strong> {bar_label} &nbsp;|&nbsp;
                    <strong>Universe Scanned:</strong> {total_scanned} Stocks &nbsp;|&nbsp;
                    <strong>Matches:</strong> {len(ordered)}
                </p>
                <p style="margin:0 0 4px 0;"><strong>📋 TOP 5 STOCKS (Highest Correction/Rally Time Ratio):</strong></p>
                <pre style="background:#f1f5f9;padding:10px;border-radius:4px;margin:0 0 12px 0;white-space:pre-wrap;font-size:14px;">{top5_syms}</pre>
                <p style="margin:0 0 14px 0;font-size:14px;color:#334155;">
                    <strong>All Matched Stocks ({len(ordered)}):</strong> {all_syms}
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

            print(f"✅ Alert Email sent successfully to {RECEIVER_EMAIL} for {len(ordered)} stock(s)!")
            return
        except Exception as e:
            print(f"⚠️ Email attempt {attempt}/3 failed: {e}")
            time.sleep(3)

# ==============================================================================
# 7. CORE SCANNER & 129-MINUTE MARKET CLOCK SCHEDULER
# ==============================================================================
def get_expected_129m_bar_label(now_ist):
    t = now_ist.time()
    if t < dtime(11, 24):
        return None
    elif t < dtime(13, 33):
        bar_start = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    elif t < dtime(15, 30):
        bar_start = now_ist.replace(hour=11, minute=24, second=0, microsecond=0)
    else:
        bar_start = now_ist.replace(hour=13, minute=33, second=0, microsecond=0)
    return bar_start.strftime("%Y-%m-%d %H:%M")

def run_129m_scanner(force_scan=False):
    now_ist = datetime.now(IST)
    memory = load_memory()
    expected_bar_label = get_expected_129m_bar_label(now_ist)

    if not force_scan and expected_bar_label:
        if memory.get("last_scanned_129m_bar") == expected_bar_label:
            print(f"ℹ️ 129m Bar [{expected_bar_label}] already scanned. Skipping duplicate pass.")
            return

    get_definedge_session_key()
    eq_map, fut_map = load_definedge_tokens()
    token_map = eq_map if DATA_SEGMENT.upper() == "NSE" else fut_map

    start_dt = (now_ist - timedelta(days=LOOKBACK_DAYS_1M)).replace(hour=9, minute=15, second=0, microsecond=0)
    print(f"\n🔎 Scanning {len(token_map)} High-Beta Stocks on 129m Timeframe (Target Bar: {expected_bar_label or 'LATEST'})...")

    def scan_single_stock(symbol):
        token = token_map.get(symbol)
        if not token:
            return None
        try:
            df_1m = fetch_definedge_1m_df(DATA_SEGMENT.upper(), token, start_dt, now_ist)
            if df_1m.empty:
                return None
            df_129m = resample_to_129m(df_1m, now_ist)
            if df_129m.empty:
                return None
            res = evaluate_129m_rally_slow_correction(df_129m)
            if res:
                if expected_bar_label and res["bar_iso"] != expected_bar_label and not force_scan:
                    return None
                res["symbol"] = symbol
                return res
        except Exception as e:
            print(f"  ⚠️ Error scanning {symbol}: {e}")
        return None

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(scan_single_stock, high_beta_stocks))

    triggered = sorted(
        [r for r in results if r is not None],
        key=lambda x: (x["time_ratio"], x["max_wrb_atr2_ratio"]),
        reverse=True
    )

    for m in triggered:
        print(
            f"  🔥 MATCH: {m['symbol']} | Time Ratio [Corr/Rally]: {m['time_ratio']:.2f}x "
            f"({m['corr_bars']}b / {m['rally_bars']}b) | WRB: {m['wrb_count']} (Max ATR2: {m['max_wrb_atr2_ratio']}x) | "
            f"Red in Rally: {m['down_bars_in_rally']} | Retr: {m['retracement_pct']}% | StochK: {m['stoch_k']}"
        )

    bar_label_used = expected_bar_label or (triggered[0]["bar_iso"] if triggered else "LATEST")
    if expected_bar_label:
        memory["last_scanned_129m_bar"] = expected_bar_label
        save_memory(memory)

    if triggered:
        send_129m_pattern_email(triggered, bar_label_used, len(token_map))
    else:
        print(f"ℹ️ No 129m WRB Rally + Slow Correction setups matched on bar [{bar_label_used}].")

def heartbeat_sleep(target_dt):
    while True:
        now_ist = datetime.now(IST)
        rem = (target_dt - now_ist).total_seconds()
        if rem <= 0:
            break
        time.sleep(min(30.0, rem))

def stay_awake_129m_clock():
    now_ist = datetime.now(IST)
    print(f"🕒 Starting 129m WRB Rally + Slow Correction Scanner at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")

    is_weekday = now_ist.weekday() < 5
    in_market_window = is_weekday and (dtime(9, 10) <= now_ist.time() <= dtime(15, 35))

    if not in_market_window:
        print("⚡ Running immediate single-pass 129m scan (outside live schedule window)...")
        run_129m_scanner(force_scan=True)
        return

    while True:
        now_ist = datetime.now(IST)
        if now_ist.time() > dtime(15, 32):
            print("⏹️ Market session complete. Exiting cleanly.")
            break

        today_date = now_ist.date()
        future_points = [
            IST.localize(datetime.combine(today_date, cp))
            for cp in SCAN_CHECKPOINTS_IST
            if cp > now_ist.time()
        ]

        if not future_points:
            print("✅ All 3 daily 129m checkpoints (11:24, 13:33, 15:30) completed.")
            break

        next_dt = future_points[0]
        sleep_mins = round((next_dt - now_ist).total_seconds() / 60.0, 1)
        print(f"💤 Sleeping {sleep_mins} min until next 129m checkpoint: {next_dt.strftime('%H:%M:%S IST')}...")
        heartbeat_sleep(next_dt)

        print(f"\n🔔 129m Checkpoint reached at {datetime.now(IST).strftime('%H:%M:%S IST')}. Running scan...")
        run_129m_scanner(force_scan=False)

if __name__ == "__main__":
    stay_awake_129m_clock()
