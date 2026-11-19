import os
import sys
import io
import json
import time
import zipfile
import smtplib
import pyotp
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pytz
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, time as dtime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from tabulate import tabulate

# Force real-time unbuffered console logging in GitHub Actions
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

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
MEMORY_FILE = "mtf_129m_26m_st_memory.json"

# ==============================================================================
# 2. STRATEGY PARAMETERS (129m SUPERTREND 10,3.5 -> 26m TRIGGER)
# ==============================================================================
DATA_SEGMENT = "NFO"             # "NSE" (Continuous Spot Cash for all F&O stocks) or "NFO" (Near-Month Futures)

# Higher Timeframe (129m) Filter: Close < Supertrend(10, 3.5)
ST_PERIOD = 10
ST_MULTIPLIER = 3.5

# Lower Timeframe (26m) Trigger Conditions
ATR_PERIOD_26M = 2
ATR2_METHOD = "SMA"              # "SMA" or "RMA"
ATR2_MULT_THRESHOLD = 1.34       # ATR(2) > previous bar ATR(2) * 1.34
VOL_MULT_THRESHOLD = 1.25        # Vol > previous bar volume * 1.25

# Lookback (35 calendar days = ~24 trading days = ~72 bars of 129m & ~360 bars of 26m)
LOOKBACK_DAYS_1M = 35

# Email Dispatch Behavior:
# Sends email immediately on startup, whenever a new 129m bar closes, and whenever >= 1 stock triggers on 26m
SEND_EMAIL_ON_NEW_129M_BAR = True

# All 15 Daily 26-Minute Candle Close Checkpoints (09:15 origin):
SCAN_CHECKPOINTS_IST = [
    dtime(9, 41, 10),   # Bar 1:  09:15 - 09:41
    dtime(10, 7, 10),   # Bar 2:  09:41 - 10:07
    dtime(10, 33, 10),  # Bar 3:  10:07 - 10:33
    dtime(10, 59, 10),  # Bar 4:  10:33 - 10:59
    dtime(11, 25, 10),  # Bar 5:  10:59 - 11:25 (also captures 11:24 129m close)
    dtime(11, 51, 10),  # Bar 6:  11:25 - 11:51
    dtime(12, 17, 10),  # Bar 7:  11:51 - 12:17
    dtime(12, 43, 10),  # Bar 8:  12:17 - 12:43
    dtime(13, 9, 10),   # Bar 9:  12:43 - 13:09
    dtime(13, 35, 10),  # Bar 10: 13:09 - 13:35 (also captures 13:33 129m close)
    dtime(14, 1, 10),   # Bar 11: 13:35 - 14:01
    dtime(14, 27, 10),  # Bar 12: 14:01 - 14:27
    dtime(14, 53, 10),  # Bar 13: 14:27 - 14:53
    dtime(15, 19, 10),  # Bar 14: 14:53 - 15:19
    dtime(15, 30, 10),  # Bar 15: 15:19 - 15:30 (also captures 15:30 129m close)
]

# ==============================================================================
# COMPLETE NSE F&O UNIVERSE (ALL 228 F&O STOCKS)
# ==============================================================================
ALL_FNO_STOCKS = [
    "360ONE", "ABB", "ABCAPITAL", "ADANIENSOL", "ADANIENT", "ADANIGREEN", "ADANIPORTS",
    "ADANIPOWER", "ALKEM", "AMBER", "AMBUJACEM", "ANANDRATHI", "ANGELONE", "APLAPOLLO",
    "APOLLOHOSP", "ASHOKLEY", "ASIANPAINT", "ASTRAL", "ATHERENERG", "AUBANK", "AUROPHARMA",
    "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJAJHFL", "BAJAJHLDNG", "BAJFINANCE",
    "BANDHANBNK", "BANKBARODA", "BANKINDIA", "BDL", "BEL", "BHARATFORG", "BHARTIARTL",
    "BHEL", "BIOCON", "BLUESTARCO", "BOSCHLTD", "BPCL", "BRITANNIA", "BSE", "CAMS",
    "CANBK", "CDSL", "CGPOWER", "CHOLAFIN", "CIPLA", "COALINDIA", "COCHINSHIP", "COFORGE",
    "COLPAL", "CONCOR", "CROMPTON", "CUB", "CUMMINSIND", "DABUR", "DALBHARAT", "DATAPATTNS",
    "DELHIVERY", "DIVISLAB", "DIXON", "DLF", "DMART", "DRREDDY", "EICHERMOT", "ENRIN",
    "ETERNAL", "EXIDEIND", "FEDERALBNK", "FORCEMOT", "FORTIS", "GAIL", "GLENMARK",
    "GMRAIRPORT", "GODFRYPHLP", "GODREJCP", "GODREJPROP", "GRASIM", "GVT&D", "HAL",
    "HAVELLS", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO",
    "HINDPETRO", "HINDUNILVR", "HINDZINC", "HUDCO", "HYUNDAI", "ICICIBANK", "ICICIGI",
    "ICICIPRULI", "IDEA", "IDFCFIRSTB", "IEX", "IGL", "INDHOTEL", "INDIANB", "INDIGO",
    "INDUSINDBK", "INDUSTOWER", "INFY", "INOXWIND", "IOC", "IRCTC", "IREDA", "IRFC",
    "ITC", "JINDALSTEL", "JIOFIN", "JSWENERGY", "JSWINFRA", "JSWSTEEL", "JUBLFOOD",
    "KALYANKJIL", "KAYNES", "KEI", "KFINTECH", "KOTAKBANK", "KPITTECH", "LAURUSLABS",
    "LICHSGFIN", "LICI", "LODHA", "LT", "LTF", "LTM", "LUPIN", "M&M", "MAHABANK",
    "MANAPPURAM", "MANKIND", "MARICO", "MARUTI", "MAXHEALTH", "MAZDOCK", "MCX", "MFSL",
    "MOTHERSON", "MOTILALOFS", "MPHASIS", "MUTHOOTFIN", "NAM-INDIA", "NATIONALUM", "NAUKRI",
    "NBCC", "NESTLEIND", "NHPC", "NMDC", "NTPC", "NTPCGREEN", "NUVAMA", "NYKAA",
    "OBEROIRLTY", "OFSS", "OIL", "ONGC", "PAGEIND", "PATANJALI", "PAYTM", "PERSISTENT",
    "PETRONET", "PFC", "PGEL", "PHOENIXLTD", "PIDILITIND", "PIIND", "PNB", "PNBHOUSING",
    "POLICYBZR", "POLYCAB", "POWERGRID", "POWERINDIA", "PREMIERENE", "PRESTIGE", "RADICO",
    "RBLBANK", "RECLTD", "RELIANCE", "RVNL", "SAGILITY", "SAIL", "SBICARD", "SBILIFE",
    "SBIN", "SHREECEM", "SHRIRAMFIN", "SIEMENS", "SOLARINDS", "SONACOMS", "SRF",
    "SUNPHARMA", "SUPREMEIND", "SUZLON", "SWIGGY", "SYNGENE", "TATACONSUM", "TATAELXSI",
    "TATAPOWER", "TATASTEEL", "TATATECH", "TCS", "TECHM", "TIINDIA", "TITAGARH", "TITAN",
    "TMPV", "TORNTPHARM", "TORNTPOWER", "TRENT", "TVSMOTOR", "UJJIVANSFB", "ULTRACEMCO",
    "UNIONBANK", "UNITDSPR", "UNOMINDA", "UPL", "VBL", "VEDL", "VMM", "VOLTAS",
    "WAAREEENER", "WIPRO", "YESBANK", "ZYDUSLIFE"
]

SESSION_CACHE = {"key": "", "date": ""}

HTTP_SESSION = requests.Session()
adapter = HTTPAdapter(pool_connections=24, pool_maxsize=24, max_retries=Retry(total=2, backoff_factor=0.3))
HTTP_SESSION.mount("https://", adapter)
HTTP_SESSION.mount("http://", adapter)

# ==============================================================================
# 3. JSON MEMORY PERSISTENCE
# ==============================================================================
def load_memory():
    default_mem = {
        "last_scanned_26m_bar": "",
        "last_emailed_129m_bar": ""
    }
    if os.path.exists(MEMORY_FILE):
        try:
            with open(MEMORY_FILE, "r") as f:
                default_mem.update(json.load(f))
        except Exception as e:
            print(f"⚠️ Memory read warning: {e}", flush=True)
    return default_mem

def save_memory(mem):
    try:
        with open(MEMORY_FILE, "w") as f:
            json.dump(mem, f, indent=2)
    except Exception as e:
        print(f"⚠️ Memory write warning: {e}", flush=True)

# ==============================================================================
# 4. DEFINEDGE AUTH, F&O TOKEN LOADER & 1-MINUTE HISTORICAL ENGINE
# ==============================================================================
def get_definedge_session_key(force_refresh=False):
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    if not force_refresh and SESSION_CACHE["key"] and SESSION_CACHE["date"] == today_str:
        return SESSION_CACHE["key"]

    for attempt in range(1, 4):
        try:
            url_step1 = f"https://signin.definedgesecurities.com/auth/realms/debroking/dsbpkc/login/{DEFINEDGE_API_TOKEN}"
            r1 = HTTP_SESSION.get(url_step1, headers={"api_secret": DEFINEDGE_API_SECRET}, timeout=15)
            r1.raise_for_status()
            otp_token = r1.json().get("otp_token")

            totp_code = pyotp.TOTP(DEFINEDGE_TOTP_SECRET).now()
            url_step2 = "https://signin.definedgesecurities.com/auth/realms/debroking/dsbpkc/token"
            r2 = HTTP_SESSION.post(url_step2, json={"otp_token": otp_token, "otp": totp_code}, timeout=15)
            r2.raise_for_status()
            session_key = r2.json().get("api_session_key")

            if session_key:
                SESSION_CACHE["key"] = session_key
                SESSION_CACHE["date"] = today_str
                print("🔑 Definedge session authenticated.", flush=True)
                return session_key
        except Exception as e:
            print(f"⚠️ Definedge login attempt {attempt}/3 failed: {e}", flush=True)
            time.sleep(4)
    raise RuntimeError("Could not authenticate with Definedge API.")

def load_definedge_fno_tokens():
    today = datetime.now(IST).date()
    target_set = set(ALL_FNO_STOCKS)

    url = "https://app.definedgesecurities.com/public/allmaster.zip"
    resp = HTTP_SESSION.get(url, timeout=30)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pd.read_csv(f, header=None, dtype=str, low_memory=False)

    # Also auto-include any active FUTSTK underlying in NFO master
    nfo_fut = df[(df[0] == "NFO") & (df[4].str.strip() == "FUTSTK")].copy()
    nfo_fut[1] = nfo_fut[1].str.strip()
    nfo_fut[2] = nfo_fut[2].str.strip()

    for exp_col in [5, 8, 6, 7]:
        if exp_col in nfo_fut.columns:
            parsed = pd.to_datetime(nfo_fut[exp_col].str.strip().str.zfill(8), format="%d%m%Y", errors="coerce")
            if parsed.notna().sum() > 0:
                nfo_fut["expiry"] = parsed.dt.date
                break

    nfo_fut = nfo_fut[nfo_fut["expiry"].notna() & (nfo_fut["expiry"] >= today)]
    fut_near = nfo_fut.sort_values("expiry").groupby(2, as_index=False).first()
    fut_map = {row[2]: row[1] for _, row in fut_near.iterrows()}

    combined_symbols = target_set.union(set(fut_map.keys()))

    nse_eq = df[
        (df[0] == "NSE")
        & (df[2].str.strip().isin(combined_symbols))
        & (df[4].str.strip() == "EQ")
    ].copy()
    eq_map = {row[2].strip(): row[1].strip() for _, row in nse_eq.iterrows()}

    print(f"✅ Mapped {len(eq_map)} NSE Cash & {len(fut_map)} NFO Futures F&O stocks.", flush=True)
    return eq_map, fut_map

def fetch_definedge_1m_df(segment, token, start_dt, end_dt):
    from_str = start_dt.strftime("%d%m%Y%H%M")
    to_str = end_dt.strftime("%d%m%Y%H%M")
    url = f"https://data.definedgesecurities.com/sds/history/{segment}/{token}/minute/{from_str}/{to_str}"

    for attempt in range(1, 3):
        session_key = get_definedge_session_key(force_refresh=(attempt > 1))
        try:
            resp = HTTP_SESSION.get(url, headers={"Authorization": session_key}, timeout=20)
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
            df["volume"] = df["volume"].fillna(0)
            return df.dropna(subset=["open", "high", "low", "close"])
        except Exception:
            time.sleep(1.0)
    return pd.DataFrame()

# ==============================================================================
# 5. RESAMPLING (129m & 26m) + INDICATORS (SUPERTREND 10,3.5 & ATR 2)
# ==============================================================================
def get_129m_bar_close_dt(bar_start_ts):
    t = bar_start_ts.time()
    if t == dtime(9, 15):
        return bar_start_ts + timedelta(minutes=129)  # 11:24
    elif t == dtime(11, 24):
        return bar_start_ts + timedelta(minutes=129)  # 13:33
    else:
        return bar_start_ts.replace(hour=15, minute=30, second=0, microsecond=0)  # 15:30

def get_26m_bar_close_dt(bar_start_ts):
    t = bar_start_ts.time()
    if t >= dtime(15, 19):
        return bar_start_ts.replace(hour=15, minute=30, second=0, microsecond=0)
    return bar_start_ts + timedelta(minutes=26)

def resample_ohlcv(df_1m, now_ist, minutes):
    if df_1m.empty:
        return pd.DataFrame()
    df = df_1m.between_time("09:15", "15:29")
    if df.empty:
        return pd.DataFrame()

    rule = f"{minutes}min"
    max_start = dtime(13, 33) if minutes == 129 else dtime(15, 19)
    close_fn = get_129m_bar_close_dt if minutes == 129 else get_26m_bar_close_dt

    chunks = []
    for _, group in df.groupby(df.index.date):
        bars = group.resample(
            rule,
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

        bars = bars[bars.index.time <= max_start]
        chunks.append(bars)

    if not chunks:
        return pd.DataFrame()

    out = pd.concat(chunks).sort_index()
    today_date = now_ist.date()

    if (out.index.date == today_date).any():
        completed_mask = [
            (ts.date() < today_date) or (now_ist >= close_fn(ts))
            for ts in out.index
        ]
        out = out[completed_mask]

    return out

def compute_supertrend(df, period=10, multiplier=3.5):
    """Computes standard Wilder ATR Supertrend(period, multiplier)."""
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    n = len(df)

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(
        high - low,
        np.maximum(np.abs(high - prev_close), np.abs(low - prev_close))
    )

    # Wilder's RMA for ATR(period)
    atr = pd.Series(tr).ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean().values

    hl2 = (high + low) / 2.0
    basic_ub = hl2 + multiplier * atr
    basic_lb = hl2 - multiplier * atr

    final_ub = np.copy(basic_ub)
    final_lb = np.copy(basic_lb)
    st = np.zeros(n)
    direction = np.ones(n, dtype=int)  # 1 = Bullish (Close > ST), -1 = Bearish (Close < ST)

    for i in range(1, n):
        if basic_ub[i] < final_ub[i - 1] or close[i - 1] > final_ub[i - 1]:
            final_ub[i] = basic_ub[i]
        else:
            final_ub[i] = final_ub[i - 1]

        if basic_lb[i] > final_lb[i - 1] or close[i - 1] < final_lb[i - 1]:
            final_lb[i] = basic_lb[i]
        else:
            final_lb[i] = final_lb[i - 1]

        if direction[i - 1] == 1:
            if close[i] < final_lb[i]:
                direction[i] = -1
                st[i] = final_ub[i]
            else:
                direction[i] = 1
                st[i] = final_lb[i]
        else:
            if close[i] > final_ub[i]:
                direction[i] = 1
                st[i] = final_lb[i]
            else:
                direction[i] = -1
                st[i] = final_ub[i]

    return st, direction

def evaluate_stock_mtf(symbol, df_1m, now_ist):
    """
    1. Checks 129m Supertrend(10, 3.5): Latest completed 129m bar must close BELOW Supertrend.
    2. If shortlisted on 129m, checks 26m bar conditions:
       - Close > previous bar high
       - ATR(2) > previous bar ATR(2) * 1.34
       - Vol > previous bar volume * 1.25
    """
    df_129m = resample_ohlcv(df_1m, now_ist, minutes=129)
    if len(df_129m) < ST_PERIOD + 5:
        return None

    st_vals, st_dir = compute_supertrend(df_129m, period=ST_PERIOD, multiplier=ST_MULTIPLIER)
    last_129m_close = float(df_129m["close"].iloc[-1])
    last_129m_st = float(st_vals[-1])
    last_129m_dir = int(st_dir[-1])
    bar_129m_iso = df_129m.index[-1].strftime("%Y-%m-%d %H:%M")

    # Stage 1 Filter: 129m bar closed BELOW Supertrend(10, 3.5)
    if not (last_129m_dir == -1 and last_129m_close < last_129m_st):
        return None

    pct_below_st = round(((last_129m_st - last_129m_close) / last_129m_st) * 100.0, 2)

    # Stage 2 Check on 26m Timeframe
    df_26m = resample_ohlcv(df_1m, now_ist, minutes=26)
    triggered_26m = False
    bar_26m_iso = ""
    slot_26m_hm = ""
    c_26m = 0.0
    prev_h_26m = 0.0
    atr2_cur = 0.0
    atr2_prev = 0.0
    atr2_ratio = 0.0
    vol_cur = 0
    vol_prev = 0
    vol_ratio = 0.0

    if len(df_26m) >= 5:
        high_26 = df_26m["high"]
        low_26 = df_26m["low"]
        close_26 = df_26m["close"]
        vol_26 = df_26m["volume"]

        prev_close_26 = close_26.shift(1)
        tr_26 = pd.concat([
            high_26 - low_26,
            (high_26 - prev_close_26).abs(),
            (low_26 - prev_close_26).abs()
        ], axis=1).max(axis=1)

        if ATR2_METHOD.upper() == "RMA":
            atr2_series = tr_26.ewm(alpha=1.0 / ATR_PERIOD_26M, min_periods=ATR_PERIOD_26M, adjust=False).mean()
        else:
            atr2_series = tr_26.rolling(window=ATR_PERIOD_26M, min_periods=ATR_PERIOD_26M).mean()

        c_26m = float(close_26.iloc[-1])
        prev_h_26m = float(high_26.iloc[-2])
        atr2_cur = float(atr2_series.iloc[-1] or 0.0)
        atr2_prev = float(atr2_series.iloc[-2] or 0.0)
        vol_cur = int(float(vol_26.iloc[-1] or 0.0))
        vol_prev = int(float(vol_26.iloc[-2] or 0.0))

        atr2_ratio = round(atr2_cur / atr2_prev, 2) if atr2_prev > 0 else 0.0
        vol_ratio = round(vol_cur / vol_prev, 2) if vol_prev > 0 else 0.0

        bar_26m_iso = df_26m.index[-1].strftime("%Y-%m-%d %H:%M")
        slot_26m_hm = df_26m.index[-1].strftime("%d-%b %H:%M")

        cond_close = c_26m > prev_h_26m
        cond_atr = (atr2_prev > 0) and (atr2_cur > atr2_prev * ATR2_MULT_THRESHOLD)
        cond_vol = (vol_prev > 0) and (vol_cur > vol_prev * VOL_MULT_THRESHOLD)

        triggered_26m = bool(cond_close and cond_atr and cond_vol)

    return {
        "symbol": symbol,
        "bar_129m_iso": bar_129m_iso,
        "close_129m": round(last_129m_close, 2),
        "st_129m": round(last_129m_st, 2),
        "pct_below_st": pct_below_st,
        "triggered_26m": triggered_26m,
        "bar_26m_iso": bar_26m_iso,
        "slot_26m_hm": slot_26m_hm,
        "close_26m": round(c_26m, 2),
        "prev_high_26m": round(prev_h_26m, 2),
        "atr2_cur": round(atr2_cur, 2),
        "atr2_prev": round(atr2_prev, 2),
        "atr2_ratio": atr2_ratio,
        "vol_cur": vol_cur,
        "vol_prev": vol_prev,
        "vol_ratio": vol_ratio
    }

# ==============================================================================
# 6. 15px HTML EMAIL ALERT SENDER (26m TRIGGERED + FULL 129m ST SHORTLIST)
# ==============================================================================
def send_mtf_129m_26m_email(st_shortlist, triggered_26m, bar_26m_label, bar_129m_label, total_scanned):
    now_str = datetime.now(IST).strftime("%d-%b %H:%M:%S")
    subject = (
        f"⚡ 129m ST(10,3.5) Shortlist ({len(st_shortlist)}) | "
        f"26m Triggered ({len(triggered_26m)}) - [{bar_26m_label}] ({now_str})"
    )

    ordered_26m = sorted(triggered_26m, key=lambda x: (x["atr2_ratio"], x["vol_ratio"]), reverse=True)
    ordered_129m = sorted(st_shortlist, key=lambda x: (not x["triggered_26m"], x["pct_below_st"]))

    syms_26m = ", ".join([m["symbol"] for m in ordered_26m]) or "None"
    syms_129m = ", ".join([m["symbol"] for m in ordered_129m]) or "None"

    # Table 1: 26m Final Triggered Stocks
    headers_26m = [
        "Stock",
        "26m Close > Prev High",
        "26m ATR(2) Ratio",
        "26m Vol Ratio",
        "129m Close < ST(10,3.5)",
        "26m Bar"
    ]
    rows_26m = [
        [
            m["symbol"],
            f"{m['close_26m']:.2f} > {m['prev_high_26m']:.2f}",
            f"{m['atr2_ratio']:.2f}x ({m['atr2_cur']:.2f}/{m['atr2_prev']:.2f})",
            f"{m['vol_ratio']:.2f}x ({m['vol_cur']:,})",
            f"{m['close_129m']:.2f} < {m['st_129m']:.2f} (-{m['pct_below_st']:.2f}%)",
            m["slot_26m_hm"]
        ]
        for m in ordered_26m
    ]

    # Table 2: All 129m Supertrend (< ST) Shortlisted Stocks
    headers_129m = [
        "Stock",
        "129m Close",
        "129m ST(10,3.5)",
        "% Below ST",
        "26m Status (ATR2x | Volx)"
    ]
    rows_129m = [
        [
            m["symbol"],
            f"{m['close_129m']:.2f}",
            f"{m['st_129m']:.2f}",
            f"-{m['pct_below_st']:.2f}%",
            f"🔥 TRIGGERED ({m['atr2_ratio']:.2f}x | {m['vol_ratio']:.2f}x)" if m["triggered_26m"] else f"Watching ({m['atr2_ratio']:.2f}x | {m['vol_ratio']:.2f}x)"
        ]
        for m in ordered_129m
    ]

    plain_26m = tabulate(rows_26m, headers=headers_26m, tablefmt="simple") if rows_26m else "No stocks triggered all 26m conditions on this bar."
    plain_129m = tabulate(rows_129m, headers=headers_129m, tablefmt="simple") if rows_129m else "None"
    plain_body = (
        f"=== 26m TRIGGERED STOCKS ({len(ordered_26m)}) ===\n{plain_26m}\n\n"
        f"=== ALL 129m CLOSE < ST(10,3.5) SHORTLIST ({len(ordered_129m)}) ===\n{plain_129m}"
    )
    print(f"\n📊 {subject}\n{plain_body}\n", flush=True)

    if not SENDER_EMAIL or not RECEIVER_EMAIL or not APP_PASSWORD:
        print("⚠️ Email credentials missing. Skipping email dispatch.", flush=True)
        return

    def build_html_table(headers, rows, highlight_col_idx=None, highlight_token="TRIGGERED"):
        th_html = "".join(
            f'<th style="background-color:#1e293b;color:#ffffff;padding:9px 13px;border:1px solid #cbd5e1;text-align:left;white-space:nowrap;font-size:15px;font-weight:600;">{h}</th>'
            for h in headers
        )
        tr_list = []
        for idx, r in enumerate(rows):
            is_hi = highlight_col_idx is not None and highlight_token in str(r[highlight_col_idx])
            bg = "#f0fdf4" if is_hi else ("#f8fafc" if idx % 2 == 1 else "#ffffff")
            tds = "".join(
                f'<td style="padding:8px 13px;border:1px solid #cbd5e1;white-space:nowrap;font-size:15px;color:#0f172a;font-weight:{"600" if (i == 0 or is_hi) else "400"};">{cell}</td>'
                for i, cell in enumerate(r)
            )
            tr_list.append(f'<tr style="background-color:{bg};">{tds}</tr>')
        return f"""
        <div style="overflow-x:auto;margin-bottom:18px;">
            <table style="border-collapse:collapse;font-family:'SFMono-Regular',Consolas,monospace;">
                <thead><tr>{th_html}</tr></thead>
                <tbody>{"".join(tr_list)}</tbody>
            </table>
        </div>
        """

    table_26m_html = (
        build_html_table(headers_26m, rows_26m, highlight_col_idx=0, highlight_token="")
        if rows_26m
        else "<p style='color:#64748b;margin:0 0 16px 0;'>No stocks fulfilled all three 26m breakout conditions on this bar.</p>"
    )
    table_129m_html = (
        build_html_table(headers_129m, rows_129m, highlight_col_idx=4, highlight_token="TRIGGERED")
        if rows_129m
        else "<p style='color:#64748b;'>No F&O stocks closed below 129m Supertrend(10, 3.5).</p>"
    )

    for attempt in range(1, 4):
        try:
            html_body = f"""
            <html>
            <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; font-size: 15px; color: #0f172a;">
                <p style="margin:0 0 8px 0;font-size:16px;"><strong>⚡ 129m Supertrend(10, 3.5) Bearish Shortlist &rarr; 26m Breakout Trigger</strong></p>
                <p style="margin:0 0 14px 0; color:#475569;font-size:15px;">
                    <strong>26m Bar:</strong> {bar_26m_label} &nbsp;|&nbsp;
                    <strong>129m Bar:</strong> {bar_129m_label} &nbsp;|&nbsp;
                    <strong>F&amp;O Scanned:</strong> {total_scanned} &nbsp;|&nbsp;
                    <strong>129m &lt; ST:</strong> {len(ordered_129m)} &nbsp;|&nbsp;
                    <strong>26m Triggered:</strong> {len(ordered_26m)}
                </p>

                <p style="margin:0 0 4px 0;"><strong>🔥 26m FINAL TRIGGERED STOCKS ({len(ordered_26m)}):</strong></p>
                <pre style="background:#f0fdf4;border:1px solid #bbf7d0;padding:10px;border-radius:4px;margin:0 0 12px 0;white-space:pre-wrap;font-size:14px;">{syms_26m}</pre>
                {table_26m_html}

                <p style="margin:0 0 4px 0;"><strong>📋 ALL 129m CLOSE &lt; SUPERTREND(10, 3.5) SHORTLISTED STOCKS ({len(ordered_129m)}):</strong></p>
                <pre style="background:#f1f5f9;padding:10px;border-radius:4px;margin:0 0 12px 0;white-space:pre-wrap;font-size:14px;">{syms_129m}</pre>
                {table_129m_html}
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

            print(f"✅ Email sent to {RECEIVER_EMAIL} (129m Shortlist: {len(ordered_129m)}, 26m Triggered: {len(ordered_26m)})!", flush=True)
            return
        except Exception as e:
            print(f"⚠️ Email attempt {attempt}/3 failed: {e}", flush=True)
            time.sleep(3)

# ==============================================================================
# 7. CORE SCANNER & 26m / 129m MARKET CLOCK SCHEDULER
# ==============================================================================
def get_expected_26m_bar_label(now_ist):
    t = now_ist.time()
    if t < dtime(9, 41):
        return None
    if t >= dtime(15, 30):
        bar_start = now_ist.replace(hour=15, minute=19, second=0, microsecond=0)
    else:
        mins_since_open = (now_ist.hour * 60 + now_ist.minute) - (9 * 60 + 15)
        completed_bars = mins_since_open // 26
        bar_start_mins = (9 * 60 + 15) + (completed_bars - 1) * 26
        bar_start = now_ist.replace(hour=bar_start_mins // 60, minute=bar_start_mins % 60, second=0, microsecond=0)
    return bar_start.strftime("%Y-%m-%d %H:%M")

def run_mtf_scanner(force_scan=False):
    now_ist = datetime.now(IST)
    memory = load_memory()
    expected_26m = get_expected_26m_bar_label(now_ist)

    if not force_scan and expected_26m is not None:
        if memory.get("last_scanned_26m_bar") == expected_26m:
            print(f"ℹ️ 26m Bar [{expected_26m}] already scanned. Skipping duplicate pass.", flush=True)
            return

    get_definedge_session_key()
    eq_map, fut_map = load_definedge_fno_tokens()
    token_map = eq_map if DATA_SEGMENT.upper() == "NSE" else fut_map
    symbols_list = sorted(token_map.keys())

    start_dt = (now_ist - timedelta(days=LOOKBACK_DAYS_1M)).replace(hour=9, minute=15, second=0, microsecond=0)
    print(f"\n🔎 Scanning {len(symbols_list)} F&O Stocks (129m ST(10,3.5) -> 26m Trigger) [Target 26m: {expected_26m or 'LATEST'}]...", flush=True)

    def scan_single_stock(symbol):
        token = token_map.get(symbol)
        if not token:
            return None
        try:
            df_1m = fetch_definedge_1m_df(DATA_SEGMENT.upper(), token, start_dt, now_ist)
            if df_1m.empty:
                return None
            return evaluate_stock_mtf(symbol, df_1m, now_ist)
        except Exception as e:
            print(f"  ⚠️ Error scanning {symbol}: {e}", flush=True)
            return None

    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(scan_single_stock, symbols_list))

    st_shortlist = [r for r in results if r is not None]
    triggered_26m = [r for r in st_shortlist if r["triggered_26m"]]

    bar_26m_used = expected_26m or (st_shortlist[0]["bar_26m_iso"] if st_shortlist else "LATEST")
    bar_129m_used = st_shortlist[0]["bar_129m_iso"] if st_shortlist else "LATEST"

    print(f"📌 Stage 1 (129m Close < ST 10,3.5): {len(st_shortlist)}/{len(symbols_list)} F&O stocks shortlisted.", flush=True)
    for m in triggered_26m:
        print(
            f"  🔥 26m TRIGGER: {m['symbol']} | Close {m['close_26m']} > PrevH {m['prev_high_26m']} | "
            f"ATR2: {m['atr2_ratio']}x | Vol: {m['vol_ratio']}x | 129m: {m['close_129m']} < ST {m['st_129m']}",
            flush=True
        )

    is_new_129m_bar = bool(bar_129m_used != "LATEST" and memory.get("last_emailed_129m_bar") != bar_129m_used)

    if expected_26m:
        memory["last_scanned_26m_bar"] = expected_26m
    if is_new_129m_bar and SEND_EMAIL_ON_NEW_129M_BAR:
        memory["last_emailed_129m_bar"] = bar_129m_used
    save_memory(memory)

    should_email = bool(triggered_26m or force_scan or (is_new_129m_bar and SEND_EMAIL_ON_NEW_129M_BAR))
    if should_email:
        send_mtf_129m_26m_email(st_shortlist, triggered_26m, bar_26m_used, bar_129m_used, len(symbols_list))
    else:
        print(f"ℹ️ 129m Shortlist ({len(st_shortlist)} stocks) unchanged; 0 new 26m triggers on [{bar_26m_used}].", flush=True)

def heartbeat_sleep(target_dt):
    while True:
        now_ist = datetime.now(IST)
        rem = (target_dt - now_ist).total_seconds()
        if rem <= 0:
            break
        time.sleep(min(30.0, rem))

def stay_awake_mtf_clock():
    now_ist = datetime.now(IST)
    session_mode = os.environ.get("SESSION_MODE", "AUTO").upper()
    print(f"🕒 Starting 129m ST(10,3.5) -> 26m Breakout Scanner at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')} (Mode: {session_mode})", flush=True)

    is_weekday = now_ist.weekday() < 5
    in_market_window = is_weekday and (dtime(9, 0) <= now_ist.time() <= dtime(15, 32))

    if not in_market_window:
        print("⚡ Running immediate single-pass scan (outside live market window)...", flush=True)
        run_mtf_scanner(force_scan=True)
        return

    session_cutoff = dtime(12, 45) if session_mode == "MORNING" else dtime(15, 32)

    # Immediate startup pass so you get the current 129m shortlist + 26m triggers right away
    if now_ist.time() >= dtime(9, 41, 10):
        print("🚀 Running immediate startup pass...", flush=True)
        run_mtf_scanner(force_scan=True)

    while True:
        now_ist = datetime.now(IST)
        if now_ist.time() > session_cutoff:
            print(f"⏹️ Reached session cutoff ({session_cutoff}). Exiting cleanly.", flush=True)
            break

        today_date = now_ist.date()
        future_points = [
            IST.localize(datetime.combine(today_date, cp))
            for cp in SCAN_CHECKPOINTS_IST
            if cp > now_ist.time() and cp <= session_cutoff
        ]

        if not future_points:
            print(f"✅ All 26m checkpoints for {session_mode} completed.", flush=True)
            break

        next_dt = future_points[0]
        sleep_mins = round((next_dt - now_ist).total_seconds() / 60.0, 1)
        print(f"💤 Sleeping {sleep_mins} min until next 26m checkpoint: {next_dt.strftime('%H:%M:%S IST')}...", flush=True)
        heartbeat_sleep(next_dt)

        print(f"\n🔔 26m Checkpoint reached at {datetime.now(IST).strftime('%H:%M:%S IST')}. Running scan...", flush=True)
        run_mtf_scanner(force_scan=False)

if __name__ == "__main__":
    stay_awake_mtf_clock()
