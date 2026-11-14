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
# 129-MINUTE "JUST STARTED RALLYING + SLOW TIME-CORRECTION" PARAMETERS
# ==============================================================================
DATA_SEGMENT = "NSE"                  # "NSE" (Continuous Spot Cash) or "NFO" (Near-Month Futures)
ATR_FAST_PERIOD = 2                   # ATR(2) for Wide-Range Bar thrust check
ATR_SLOW_PERIOD = 100                 # ATR(100) baseline for WRB & small-body checks
ATR_METHOD = "SMA"                    # "SMA" or "RMA"

# Phase 0: "Just Started Rallying" (Prior Base / Low Context)
PRIOR_BASE_LOOKBACK = 20              # Bars prior to rally start used to verify stock launched from a base/low
MAX_START_POS_IN_PRIOR_BASE = 0.45    # Rally Start Low must be in the bottom 45% of the prior 20-bar range
PRIOR_HIGH_CEILING_RATIO = 0.65       # Prior 15 bars before rally must not have exceeded 65% of the new rally height

# Phase 1: Impulse Rally Rules (True Full-Leg Base-to-Peak)
WRB_ATR2_PREV_MULT = 1.5              # Rally WRB must have ATR(2) >= 1.5x Previous Bar's ATR(2)
WRB_ATR2_VS_ATR100_MIN = 1.35         # Rally WRB must have ATR(2) >= 1.35x ATR(100) (spikes above ATR 100 line)
WRB_BODY_VS_ATR100_MIN = 1.0          # Rally WRB bullish body |Close - Open| >= 1.0x ATR(100)
MIN_WRB_COUNT_IN_RALLY = 1            # At least 1 wide-range bullish thrust bar in the rally
MAX_DOWN_BARS_IN_RALLY = 3            # Allow 0, 1, 2, or max 3 small-body corrective down bars inside rally
SMALL_DOWN_BODY_ATR100_MAX = 0.67     # Down bar body inside rally must be <= 0.67 * ATR(100)
SMALL_DOWN_BODY_VS_UP_MAX = 0.45      # Down bar body inside rally must be <= 45% of max bullish rally body
MIN_RALLY_BARS = 2                    # Minimum bars in the full impulse rally leg
MAX_RALLY_BARS = 10                   # Maximum bars in the full impulse rally leg
MIN_RALLY_RANGE_ATR100 = 2.8          # Total rally height (Peak High - Start Low) >= 2.8x ATR(100)

# Phase 2: Slow Time-Correction Rules (Correction Time > Rally Time)
MAX_CORRECTION_BARS = 35              # Max bars to look back for the rally peak
MIN_RETRACEMENT_OF_RALLY = 0.20       # Must pull back at least 20% from peak (actual correction, not flat top)
MAX_RETRACEMENT_OF_RALLY = 0.78       # Correction Low must hold well above Rally Start Low (<= 78% retracement)
MAX_CORR_AVG_BODY_VS_RALLY = 0.65     # Avg body in correction must be < 65% of avg body in rally (slow pullback)

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
    df["atr2_vs_100"] = np.where(df["atr100"] > 0, df["atr2"] / df["atr100"], 0.0)
    df["body"] = (df["close"] - df["open"]).abs()
    df["body_vs_100"] = np.where(df["atr100"] > 0, df["body"] / df["atr100"], 0.0)

    # Stochastic (5, 3, 3)
    low_5 = df["low"].rolling(5, min_periods=5).min()
    high_5 = df["high"].rolling(5, min_periods=5).max()
    fast_k = np.where((high_5 - low_5) > 0, ((df["close"] - low_5) / (high_5 - low_5)) * 100.0, 50.0)
    df["stoch_k"] = pd.Series(fast_k, index=df.index).rolling(3, min_periods=3).mean()
    df["stoch_d"] = df["stoch_k"].rolling(3, min_periods=3).mean()

    # True Bullish Wide-Range Bar (WRB):
    # 1. Bullish bar (Close > Open and Close > Prev Close)
    # 2. ATR(2) >= 1.5x Previous ATR(2)
    # 3. ATR(2) >= 1.35x ATR(100) (blue ATR 2 line spikes above black ATR 100 line)
    # 4. Bullish candle body >= 1.0x ATR(100)
    df["is_bull_wrb"] = (
        (df["close"] > df["open"])
        & (df["close"] > prev_close)
        & (df["atr2_ratio"] >= WRB_ATR2_PREV_MULT)
        & (df["atr2_vs_100"] >= WRB_ATR2_VS_ATR100_MIN)
        & (df["body_vs_100"] >= WRB_BODY_VS_ATR100_MIN)
    )
    return df

# ==============================================================================
# 5. PATTERN DETECTOR: FRESH IMPULSE RALLY FROM BASE + SLOWER TIME-CORRECTION
# ==============================================================================
def find_true_rally_start_idx(df, peak_idx):
    """
    Finds the true structural starting bar of the rally ending at `peak_idx`
    so the scanner NEVER slices just the top 1-2 bars of a longer move:
      - Walks backward up to MAX_RALLY_BARS from `peak_idx` to locate the lowest
        swing low (`min_low_idx`) and the first bullish launch bar (`start_idx`).
    """
    search_from = max(PRIOR_BASE_LOOKBACK, peak_idx - MAX_RALLY_BARS + 1)
    if search_from >= peak_idx:
        return None

    window = df.iloc[search_from : peak_idx + 1]
    min_low_val = float(window["low"].min())

    # Positional index of the lowest low inside [search_from .. peak_idx]
    rel_min_pos = int(np.argmin(window["low"].values))
    raw_start_idx = search_from + rel_min_pos

    # If the lowest low bar itself was a small red bar right before the first green launch bar,
    # step 1 bar forward if that next bar is bullish and holds above the swing low
    if (
        raw_start_idx < peak_idx
        and float(df["close"].iloc[raw_start_idx]) <= float(df["open"].iloc[raw_start_idx])
        and float(df["close"].iloc[raw_start_idx + 1]) > float(df["open"].iloc[raw_start_idx + 1])
    ):
        return raw_start_idx + 1, min_low_val

    return raw_start_idx, min_low_val

def validate_fresh_rally_and_slow_correction(df, peak_idx, end_corr_idx):
    """
    Validates the 3-Phase Kalyan Jewellers structure:
      Phase 0 (Prior Base): Stock was in a low base / downtrend before `start_idx` (just started rallying).
      Phase 1 (Impulse Rally): True base-to-peak leg has >= 1 WRB (ATR2 >= 1.5x prev & > ATR100)
                               and at most 1, 2, or 3 small-body corrective down bars.
      Phase 2 (Slow Correction): Consumes strictly MORE bars than the full rally (corr_bars > rally_bars),
                                 stays above the rally base, and has smaller corrective bodies.
    """
    start_info = find_true_rally_start_idx(df, peak_idx)
    if not start_info:
        return None

    start_idx, swing_low_price = start_info
    rally_bars = peak_idx - start_idx + 1
    corr_bars = end_corr_idx - peak_idx

    if rally_bars < MIN_RALLY_BARS or rally_bars > MAX_RALLY_BARS:
        return None
    if corr_bars <= rally_bars or corr_bars > MAX_CORRECTION_BARS:
        return None

    rally_slice = df.iloc[start_idx : peak_idx + 1]
    corr_slice = df.iloc[peak_idx + 1 : end_corr_idx + 1]

    # Peak bar must be the highest high of the rally and correction
    rally_peak_high = float(rally_slice.iloc[-1]["high"])
    if float(rally_slice["high"].max()) > rally_peak_high:
        return None
    if float(corr_slice["high"].max()) > rally_peak_high:
        return None

    rally_start_low = min(swing_low_price, float(rally_slice["low"].min()))
    rally_range = rally_peak_high - rally_start_low
    ref_atr100 = float(df["atr100"].iloc[end_corr_idx] or 0.0)

    if ref_atr100 <= 0 or rally_range < (MIN_RALLY_RANGE_ATR100 * ref_atr100):
        return None

    # --------------------------------------------------------------------------
    # PHASE 0: "JUST STARTED RALLYING" (MUST LAUNCH FROM A PRIOR BASE / LOW)
    # --------------------------------------------------------------------------
    prior_start = max(0, start_idx - PRIOR_BASE_LOOKBACK)
    prior_slice = df.iloc[prior_start:start_idx]
    if len(prior_slice) < 10:
        return None

    prior_high = float(prior_slice["high"].max())
    prior_low = float(prior_slice["low"].min())
    total_context_high = max(prior_high, rally_peak_high)
    total_context_low = min(prior_low, rally_start_low)

    # 1. Rally Peak must be a fresh breakout high above the prior base
    if prior_high >= rally_peak_high:
        return None

    # 2. Prior 15 bars before the rally must have been trading in the base (below 65% of the rally peak)
    recent_prior = df.iloc[max(0, start_idx - 15) : start_idx]
    if float(recent_prior["high"].max()) > (rally_start_low + PRIOR_HIGH_CEILING_RATIO * rally_range):
        return None

    # 3. Rally Start Low must be near the bottom of the prior 20-bar range (bottom 45%)
    if (total_context_high - total_context_low) > 0:
        start_pos = (rally_start_low - total_context_low) / (total_context_high - total_context_low)
        if start_pos > MAX_START_POS_IN_PRIOR_BASE:
            return None

    # --------------------------------------------------------------------------
    # PHASE 1: IMPULSE RALLY WITH WRB (ATR2 >= 1.5x PREV) & <= 3 SMALL DOWN BARS
    # --------------------------------------------------------------------------
    wrb_bars = rally_slice[rally_slice["is_bull_wrb"]]
    if len(wrb_bars) < MIN_WRB_COUNT_IN_RALLY:
        return None

    # First WRB must occur in the first half of the rally (the initial launch thrust)
    first_wrb_pos = int(np.where(rally_slice["is_bull_wrb"].values)[0][0])
    if first_wrb_pos > max(2, rally_bars // 2):
        return None

    max_wrb_atr_ratio = round(float(wrb_bars["atr2_ratio"].max()), 2)
    max_wrb_vs_100 = round(float(wrb_bars["atr2_vs_100"].max()), 2)

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
            if (
                d_body > (SMALL_DOWN_BODY_ATR100_MAX * d_atr100)
                or d_body > (SMALL_DOWN_BODY_VS_UP_MAX * max_up_body)
            ):
                return None

    # --------------------------------------------------------------------------
    # PHASE 2: SLOW TIME-CORRECTION ABOVE RALLY BASE
    # --------------------------------------------------------------------------
    corr_low = float(corr_slice["low"].min())
    cur_close = float(corr_slice["close"].iloc[-1])

    # Must hold strictly above the starting point of the rally
    if corr_low <= rally_start_low:
        return None

    # Retracement depth check (20% to 78% of the rally height)
    retracement_pct = round(((rally_peak_high - corr_low) / rally_range) * 100.0, 1)
    if retracement_pct < (MIN_RETRACEMENT_OF_RALLY * 100.0) or retracement_pct > (MAX_RETRACEMENT_OF_RALLY * 100.0):
        return None

    # Current close must still be inside the corrective zone (at least 10% below peak high)
    cur_pullback_pct = ((rally_peak_high - cur_close) / rally_range) * 100.0
    if cur_pullback_pct < 10.0 or cur_close <= rally_start_low:
        return None

    # Correction candles must have smaller average body than the rally candles
    avg_rally_body = float(rally_slice["body"].mean())
    avg_corr_body = float(corr_slice["body"].mean())
    if avg_rally_body <= 0 or (avg_corr_body / avg_rally_body) > MAX_CORR_AVG_BODY_VS_RALLY:
        return None

    time_ratio = round(float(corr_bars) / float(rally_bars), 2)

    return {
        "rally_bars": int(rally_bars),
        "corr_bars": int(corr_bars),
        "time_ratio": time_ratio,
        "wrb_count": int(len(wrb_bars)),
        "down_bars_in_rally": int(len(down_bars)),
        "max_wrb_atr2_ratio": max_wrb_atr_ratio,
        "max_wrb_vs_100": max_wrb_vs_100,
        "rally_start_low": round(rally_start_low, 2),
        "rally_peak_high": round(rally_peak_high, 2),
        "corr_low": round(corr_low, 2),
        "retracement_pct": retracement_pct,
        "rally_gain_pct": round(((rally_peak_high - rally_start_low) / rally_start_low) * 100.0, 1)
    }

def evaluate_129m_rally_slow_correction(df_129m):
    if len(df_129m) < 55:
        return None

    df = compute_indicators_129m(df_129m)
    cur_idx = len(df) - 1
    cur_bar = df.iloc[cur_idx]

    best_match = None
    min_peak_idx = max(PRIOR_BASE_LOOKBACK + MIN_RALLY_BARS, cur_idx - MAX_CORRECTION_BARS)

    for peak_idx in range(cur_idx - 3, min_peak_idx - 1, -1):
        res = validate_fresh_rally_and_slow_correction(df, peak_idx, cur_idx)
        if res:
            score = (res["time_ratio"], res["max_wrb_atr2_ratio"])
            if best_match is None or score > (best_match["time_ratio"], best_match["max_wrb_atr2_ratio"]):
                best_match = res

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
    subject = f"🔥 129m Fresh WRB Rally + Slow Correction ({len(triggered_list)} Matches) - [{bar_label}] ({now_str})"

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
        "Rally ATR(2) Thrust (vsPrev | vs100)",
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
            f"{m['max_wrb_atr2_ratio']:.2f}x | {m['max_wrb_vs_100']:.2f}x",
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
                <p style="margin:0 0 8px 0;font-size:16px;"><strong>🔥 129m Fresh WRB Impulse Rally + Slow Time-Correction (Sorted by Max Time Ratio)</strong></p>
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
            f"({m['corr_bars']}b / {m['rally_bars']}b) | WRB: {m['wrb_count']} "
            f"(ATR2 Thrust: {m['max_wrb_atr2_ratio']}x prev, {m['max_wrb_vs_100']}x ATR100) | "
            f"Red in Rally: {m['down_bars_in_rally']} | Retr: {m['retracement_pct']}% | StochK: {m['stoch_k']}"
        )

    bar_label_used = expected_bar_label or (triggered[0]["bar_iso"] if triggered else "LATEST")
    if expected_bar_label:
        memory["last_scanned_129m_bar"] = expected_bar_label
        save_memory(memory)

    if triggered:
        send_129m_pattern_email(triggered, bar_label_used, len(token_map))
    else:
        print(f"ℹ️ No 129m Fresh WRB Rally + Slow Correction setups matched on bar [{bar_label_used}].")

def heartbeat_sleep(target_dt):
    while True:
        now_ist = datetime.now(IST)
        rem = (target_dt - now_ist).total_seconds()
        if rem <= 0:
            break
        time.sleep(min(30.0, rem))

def stay_awake_129m_clock():
    now_ist = datetime.now(IST)
    print(f"🕒 Starting 129m Fresh WRB Rally + Slow Correction Scanner at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")

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
