import os
import re
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
MEMORY_FILE = "equal_swing_26m_memory.json"
BASKET_VERSION = "v1_26m_highest_oi_equal_swings"

# ==============================================================================
# 26-MINUTE EQUAL DOWN-SWING PATTERN PARAMETERS
# ==============================================================================
MIN_RED_BARS_PER_SWING = 3            # Minimum 3 red (down) bars in each corrective swing
MAX_RED_BARS_PER_SWING = 9            # Maximum red bars to count in a single corrective swing
MAX_BLUE_BARS_IN_SWING = 2            # 1-2 small blue pause bars allowed inside a down swing
MAX_INTERNAL_BLUE_BODY_RATIO = 0.65   # Internal blue bar body must be <= 65% of max red body in that swing

# Inter-Swing Bounce & Support Rules
MIN_BOUNCE_BARS = 1                   # Minimum bars in the bounce leg between Down Swing 1 and Down Swing 2
MAX_BOUNCE_BARS = 8                   # Maximum bars in the bounce leg between Down Swing 1 and Down Swing 2
MIN_BOUNCE_RECOVERY_RATIO = 0.35      # Bounce high H2 must recover at least 35% of Swing 1's drop (H1 - L1)
MAX_BOUNCE_RECOVERY_RATIO = 1.35      # Bounce high H2 stays within 135% of Swing 1's drop (corrective range)
MIN_SWING2_DROP_RATIO = 0.35          # Swing 2 drop (H2 - L2) must be at least 35% of Swing 1 drop
MIN_L2_VS_L1_SUPPORT_RATIO = 0.80     # Swing 2 low L2 must hold >= 80% of Swing 1 low L1

# Highest OI Option Selection Window per Stock
STRIKES_TO_CHECK_PER_SIDE = 10        # Checks 2 ATM/ITM + 8 OTM strikes per side to lock in Highest OI CE & PE
LOOKBACK_DAYS_1M = 12                 # 12 calendar days (~7-8 trading days = ~115 bars of 26m data)

# All 15 Daily 26-Minute Candle Close Checkpoints (09:15 origin):
SCAN_CHECKPOINTS_IST = [
    dtime(9, 41, 12),   # Bar 1:  09:15 - 09:41
    dtime(10, 7, 12),   # Bar 2:  09:41 - 10:07
    dtime(10, 33, 12),  # Bar 3:  10:07 - 10:33
    dtime(10, 59, 12),  # Bar 4:  10:33 - 10:59
    dtime(11, 25, 12),  # Bar 5:  10:59 - 11:25
    dtime(11, 51, 12),  # Bar 6:  11:25 - 11:51
    dtime(12, 17, 12),  # Bar 7:  11:51 - 12:17
    dtime(12, 43, 12),  # Bar 8:  12:17 - 12:43
    dtime(13, 9, 12),   # Bar 9:  12:43 - 13:09
    dtime(13, 35, 12),  # Bar 10: 13:09 - 13:35
    dtime(14, 1, 12),   # Bar 11: 13:35 - 14:01
    dtime(14, 27, 12),  # Bar 12: 14:01 - 14:27
    dtime(14, 53, 12),  # Bar 13: 14:27 - 14:53
    dtime(15, 19, 12),  # Bar 14: 14:53 - 15:19
    dtime(15, 30, 12),  # Bar 15: 15:19 - 15:30
]

# ==============================================================================
# LIQUID HIGH-BETA F&O STOCK UNIVERSE (102 STOCKS)
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

HTTP_SESSION = requests.Session()
adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=Retry(total=2, backoff_factor=0.3))
HTTP_SESSION.mount("https://", adapter)
HTTP_SESSION.mount("http://", adapter)

# ==============================================================================
# 2. JSON MEMORY PERSISTENCE
# ==============================================================================
def load_memory():
    default_mem = {
        "basket_date": "",
        "basket_version": "",
        "last_scanned_26m_bar": "",
        "options": []
    }
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
# 3. DEFINEDGE AUTHENTICATION & HISTORICAL DATA ENGINE
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
            raw_d = raw_dt.str.slice(0, 8)
            df["date"] = pd.to_datetime(raw_d, format="%d%m%Y", errors="coerce").dt.date
            df = df.dropna(subset=["date"]).sort_values("date")

            for col in ["open", "high", "low", "close", "volume", "oi"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["oi"] = df["oi"].fillna(0)
            df["volume"] = df["volume"].fillna(0)
            return df.dropna(subset=["open", "high", "low", "close"]).drop_duplicates(subset=["date"], keep="last")
        except Exception:
            time.sleep(1.5)
    return pd.DataFrame()

def fetch_definedge_1m_df(segment, token, start_dt, end_dt):
    from_str = start_dt.strftime("%d%m%Y%H%M")
    to_str = end_dt.strftime("%d%m%Y%H%M")
    url = f"https://data.definedgesecurities.com/sds/history/{segment}/{token}/minute/{from_str}/{to_str}"

    for attempt in range(1, 3):
        session_key = get_definedge_session_key(force_refresh=(attempt > 1))
        try:
            resp = HTTP_SESSION.get(url, headers={"Authorization": session_key}, timeout=25)
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

            for col in ["open", "high", "low", "close", "volume", "oi"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["oi"] = df["oi"].fillna(0)
            df["volume"] = df["volume"].fillna(0)
            return df.dropna(subset=["open", "high", "low", "close"])
        except Exception:
            time.sleep(1.5)
    return pd.DataFrame()

# ==============================================================================
# 4. BASKET BUILDER: HIGHEST OPEN INTEREST CE & PE PER STOCK
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

def load_definedge_master_chains():
    today = datetime.now(IST).date()
    stock_set = set(high_beta_stocks)

    url = "https://app.definedgesecurities.com/public/allmaster.zip"
    resp = HTTP_SESSION.get(url, timeout=30)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pd.read_csv(f, header=None, dtype=str, low_memory=False)

    nfo = df[(df[0] == "NFO") & (df[2].str.strip().isin(stock_set))].copy()
    nfo[1] = nfo[1].str.strip()
    nfo[2] = nfo[2].str.strip()
    nfo[3] = nfo[3].str.strip()
    nfo[4] = nfo[4].str.strip()

    for exp_col in [5, 8, 6, 7]:
        if exp_col in nfo.columns:
            parsed = pd.to_datetime(nfo[exp_col].str.strip().str.zfill(8), format="%d%m%Y", errors="coerce")
            if parsed.notna().sum() > 0:
                nfo["expiry"] = parsed.dt.date
                break

    nfo = nfo[nfo["expiry"].notna() & (nfo["expiry"] >= today)]

    fut_df = nfo[nfo[4] == "FUTSTK"].sort_values("expiry").groupby(2, as_index=False).first()
    fut_map = {row[2]: {"token": row[1], "trading_symbol": row[3]} for _, row in fut_df.iterrows()}

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

    return fut_map, options_by_stock

def pick_highest_oi_contract(candidates, start_dt_opt, now_ist):
    best_cand = None
    best_oi = -1.0
    best_vol = -1.0

    for c in candidates:
        df_opt_day = fetch_definedge_day_df("NFO", c["token"], start_dt_opt, now_ist)
        if not df_opt_day.empty:
            latest_oi = float(df_opt_day["oi"].iloc[-1])
            latest_vol = float(df_opt_day["volume"].iloc[-1])
        else:
            latest_oi, latest_vol = 0.0, 0.0

        if (latest_oi > best_oi) or (latest_oi == best_oi and latest_vol > best_vol):
            best_oi = latest_oi
            best_vol = latest_vol
            best_cand = dict(c)
            best_cand["ref_oi"] = int(max(0, latest_oi))

    return best_cand

def build_highest_oi_ce_pe_basket(now_ist):
    print(f"\n🎯 Building Highest Open Interest CE & PE Basket across {len(high_beta_stocks)} F&O Stocks...")
    get_definedge_session_key()
    fut_map, options_by_stock = load_definedge_master_chains()
    start_dt_opt = (now_ist - timedelta(days=12)).replace(hour=9, minute=15, second=0, microsecond=0)

    def select_stock_highest_oi_pair(symbol):
        fut_info = fut_map.get(symbol)
        chain = options_by_stock.get(symbol, [])
        if not fut_info or not chain:
            return []

        df_fut = fetch_definedge_day_df("NFO", fut_info["token"], start_dt_opt, now_ist)
        if df_fut.empty:
            return []
        ref_price = float(df_fut["close"].iloc[-1])
        if ref_price <= 0:
            return []

        # Candidate active strikes around ATM + OTM where Highest OI resides
        ce_all = sorted([o for o in chain if o["opt_type"] == "CE"], key=lambda x: x["strike"])
        pe_all = sorted([o for o in chain if o["opt_type"] == "PE"], key=lambda x: x["strike"], reverse=True)

        ce_otm = [o for o in ce_all if o["strike"] >= ref_price]
        ce_itm = [o for o in ce_all if o["strike"] < ref_price][-2:]
        ce_cands = (ce_itm + ce_otm)[:STRIKES_TO_CHECK_PER_SIDE]

        pe_otm = [o for o in pe_all if o["strike"] <= ref_price]
        pe_itm = [o for o in pe_all if o["strike"] > ref_price][-2:]
        pe_cands = (pe_itm + pe_otm)[:STRIKES_TO_CHECK_PER_SIDE]

        selected = []
        best_ce = pick_highest_oi_contract(ce_cands, start_dt_opt, now_ist)
        if best_ce:
            best_ce["ref_fut_price"] = round(ref_price, 2)
            selected.append(best_ce)

        best_pe = pick_highest_oi_contract(pe_cands, start_dt_opt, now_ist)
        if best_pe:
            best_pe["ref_fut_price"] = round(ref_price, 2)
            selected.append(best_pe)

        return selected

    with ThreadPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(select_stock_highest_oi_pair, high_beta_stocks))

    basket = [opt for pair in results for opt in pair]
    ce_cnt = sum(1 for o in basket if o["opt_type"] == "CE")
    pe_cnt = sum(1 for o in basket if o["opt_type"] == "PE")
    print(f"✅ Basket Locked: {len(basket)} Contracts ({ce_cnt} Highest-OI CEs + {pe_cnt} Highest-OI PEs).")
    return basket

# ==============================================================================
# 5. 26-MINUTE RESAMPLING (09:15 ORIGIN -> 15 BARS PER DAY)
# ==============================================================================
def get_26m_bar_close_dt(bar_start_ts):
    t = bar_start_ts.time()
    if t >= dtime(15, 19):
        return bar_start_ts.replace(hour=15, minute=30, second=0, microsecond=0)
    return bar_start_ts + timedelta(minutes=26)

def resample_to_26m(df_1m, now_ist):
    if df_1m.empty:
        return pd.DataFrame()
    df = df_1m.copy().between_time("09:15", "15:29")
    if df.empty:
        return pd.DataFrame()

    chunks = []
    for _, group in df.groupby(df.index.date):
        bars = group.resample(
            "26min",
            origin="start_day",
            offset="9h15min",
            label="left",
            closed="left"
        ).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "oi": "last"
        }).dropna(subset=["open", "close"])

        bars = bars[bars.index.time <= dtime(15, 19)]
        chunks.append(bars)

    if not chunks:
        return pd.DataFrame()

    df_26m = pd.concat(chunks).sort_index()
    today_date = now_ist.date()

    if (df_26m.index.date == today_date).any():
        completed_mask = [
            (ts.date() < today_date) or (now_ist >= get_26m_bar_close_dt(ts))
            for ts in df_26m.index
        ]
        df_26m = df_26m[completed_mask]

    return df_26m

# ==============================================================================
# 6. PATTERN DETECTOR: >= 2 DOWN CORRECTIVE SWINGS WITH EQUAL DOWN BARS (N >= 3)
# ==============================================================================
def validate_down_swing_slice(df, s_start, s_end):
    """
    Validates whether df.iloc[s_start : s_end + 1] is a clean corrective down swing:
      - Starts on a red bar (s_start) and ends on a red bar (s_end).
      - Preceded by a bullish bar (s_start - 1 is blue) so we capture the FULL down swing from peak.
      - Contains >= 3 red bars (Close < Open).
      - Contains <= 2 small blue pause bars (Close > Open).
      - Net price drops from swing high to swing low at s_end.
    """
    if s_start < 1 or s_end <= s_start:
        return None

    # Must start right after a blue (up) bar so we don't slice the middle of a longer down swing
    prev_bar = df.iloc[s_start - 1]
    if float(prev_bar["close"]) <= float(prev_bar["open"]):
        return None

    first_bar = df.iloc[s_start]
    last_bar = df.iloc[s_end]

    # Both the first and completing bar of the down swing must be red (down) bars
    if float(first_bar["close"]) >= float(first_bar["open"]):
        return None
    if float(last_bar["close"]) >= float(last_bar["open"]):
        return None

    sub = df.iloc[s_start : s_end + 1]
    opens = sub["open"].values
    closes = sub["close"].values
    highs = sub["high"].values
    lows = sub["low"].values
    bodies = np.abs(closes - opens)

    red_mask = closes < opens
    blue_mask = closes > opens

    red_count = int(np.sum(red_mask))
    blue_count = int(np.sum(blue_mask))

    if red_count < MIN_RED_BARS_PER_SWING or red_count > MAX_RED_BARS_PER_SWING:
        return None
    if blue_count > MAX_BLUE_BARS_IN_SWING:
        return None

    max_red_body = float(np.max(bodies[red_mask])) if red_count > 0 else 0.0
    if max_red_body <= 0:
        return None

    # Any internal blue bar (1-2 allowed) must have a small body compared to the down bars
    if blue_count > 0:
        max_blue_body = float(np.max(bodies[blue_mask]))
        if max_blue_body > (MAX_INTERNAL_BLUE_BODY_RATIO * max_red_body):
            return None

    # Swing High is at the start of the down swing (or the blue peak bar right before it)
    swing_high = max(float(prev_bar["high"]), float(highs[0]), float(highs[min(1, len(highs) - 1)]))
    if float(np.max(highs)) > swing_high * 1.02:
        return None

    # Swing Low must occur at the end of the down swing (s_end or s_end - 1)
    swing_low = float(np.min(lows))
    end_low = min(float(lows[-1]), float(lows[max(0, len(lows) - 2)]))
    if end_low > swing_low * 1.02:
        return None

    # Net downward price progress check
    swing_drop = swing_high - swing_low
    if float(last_bar["close"]) >= float(first_bar["open"]) or swing_drop <= 0:
        return None

    return {
        "s_start": s_start,
        "s_end": s_end,
        "red_count": red_count,
        "blue_count": blue_count,
        "swing_high": round(swing_high, 2),
        "swing_low": round(swing_low, 2),
        "swing_drop": round(swing_drop, 2),
        "start_time": df.index[s_start].strftime("%d-%b %H:%M"),
        "end_time": df.index[s_end].strftime("%d-%b %H:%M")
    }

def validate_inter_swing_bounce(df, s1_info, s2_info):
    """
    Validates that between Down Swing 1 (s1_end) and Down Swing 2 (s2_start),
    price made a genuine upward bounce leg (L1 -> H2) and Swing 2 held support.
    """
    s1_end = s1_info["s1_end"] if "s1_end" in s1_info else s1_info["s_end"]
    s2_start = s2_info["s_start"]

    bounce_bars = s2_start - s1_end - 1
    if bounce_bars < MIN_BOUNCE_BARS or bounce_bars > MAX_BOUNCE_BARS:
        return False

    bounce_slice = df.iloc[s1_end + 1 : s2_start]
    blue_in_bounce = int(np.sum(bounce_slice["close"].values > bounce_slice["open"].values))
    if blue_in_bounce < 1:
        return False

    h1, l1, drop1 = s1_info["swing_high"], s1_info["swing_low"], s1_info["swing_drop"]
    h2, l2, drop2 = s2_info["swing_high"], s2_info["swing_low"], s2_info["swing_drop"]

    # 1. Bounce High H2 must recover at least 35% of Swing 1's drop, up to 135%
    bounce_recovery = (h2 - l1) / drop1 if drop1 > 0 else 0.0
    if bounce_recovery < MIN_BOUNCE_RECOVERY_RATIO or bounce_recovery > MAX_BOUNCE_RECOVERY_RATIO:
        return False

    # 2. Swing 2 drop (H2 - L2) must be proportional (at least 35% of Swing 1 drop)
    if (drop2 / drop1) < MIN_SWING2_DROP_RATIO:
        return False

    # 3. Swing 2 low L2 must hold support near or above L1
    if l2 < (l1 * MIN_L2_VS_L1_SUPPORT_RATIO):
        return False

    return True

def evaluate_26m_equal_down_swings(df_26m):
    """
    Checks if the latest completed 26m bar (`cur_idx`) just completed the 2nd (or 3rd)
    corrective down swing with an EQUAL number of red bars (N == N, where N >= 3).
    """
    if len(df_26m) < 20:
        return None

    cur_idx = len(df_26m) - 1
    cur_bar = df_26m.iloc[cur_idx]

    # Trigger right as the 2nd (or 3rd) down swing completes on the current red bar
    if float(cur_bar["close"]) >= float(cur_bar["open"]):
        return None

    # Find the valid Down Swing 2 ending at cur_idx
    max_s2_len = MAX_RED_BARS_PER_SWING + MAX_BLUE_BARS_IN_SWING
    s2_info = None
    for s2_start in range(max(2, cur_idx - max_s2_len + 1), cur_idx - MIN_RED_BARS_PER_SWING + 2):
        cand_s2 = validate_down_swing_slice(df_26m, s2_start, cur_idx)
        if cand_s2:
            s2_info = cand_s2
            break

    if not s2_info:
        return None

    target_red_count = s2_info["red_count"]
    s2_start = s2_info["s_start"]

    # Search for preceding Down Swing 1 ending 1 to MAX_BOUNCE_BARS before s2_start
    matched_s1 = None
    for s1_end in range(s2_start - MIN_BOUNCE_BARS - 1, max(3, s2_start - MAX_BOUNCE_BARS - 2), -1):
        for s1_start in range(max(1, s1_end - max_s2_len + 1), s1_end - MIN_RED_BARS_PER_SWING + 2):
            cand_s1 = validate_down_swing_slice(df_26m, s1_start, s1_end)
            if cand_s1 and cand_s1["red_count"] == target_red_count:
                if validate_inter_swing_bounce(df_26m, cand_s1, s2_info):
                    matched_s1 = cand_s1
                    break
        if matched_s1:
            break

    if not matched_s1:
        return None

    # Bonus Check: Did an earlier Swing 0 also have the exact same target_red_count (3 Equal Swings)?
    swing_count = 2
    s1_start = matched_s1["s_start"]
    matched_s0 = None

    for s0_end in range(s1_start - MIN_BOUNCE_BARS - 1, max(3, s1_start - MAX_BOUNCE_BARS - 2), -1):
        for s0_start in range(max(1, s0_end - max_s2_len + 1), s0_end - MIN_RED_BARS_PER_SWING + 2):
            cand_s0 = validate_down_swing_slice(df_26m, s0_start, s0_end)
            if cand_s0 and cand_s0["red_count"] == target_red_count:
                if validate_inter_swing_bounce(df_26m, cand_s0, matched_s1):
                    matched_s0 = cand_s0
                    swing_count = 3
                    break
        if matched_s0:
            break

    live_oi = int(float(cur_bar.get("oi", 0) or 0))
    if swing_count == 3:
        pattern_label = f"🔥🔥 3 Swings ({target_red_count}={target_red_count}={target_red_count} Red)"
    else:
        pattern_label = f"🔥 2 Swings ({target_red_count}={target_red_count} Red)"

    return {
        "bar_iso": cur_bar.name.strftime("%Y-%m-%d %H:%M"),
        "slot_hm": cur_bar.name.strftime("%d-%b %H:%M"),
        "close": round(float(cur_bar["close"]), 2),
        "live_oi": live_oi,
        "swing_count": swing_count,
        "equal_red_bars": target_red_count,
        "pattern_label": pattern_label,
        "s1_summary": f"{matched_s1['swing_high']:.1f}->{matched_s1['swing_low']:.1f} ({matched_s1['red_count']}R,{matched_s1['blue_count']}B)",
        "s2_summary": f"{s2_info['swing_high']:.1f}->{s2_info['swing_low']:.1f} ({s2_info['red_count']}R,{s2_info['blue_count']}B)"
    }

# ==============================================================================
# 7. 15px HTML EMAIL ALERT SENDER
# ==============================================================================
def send_26m_equal_swing_email(triggered_list, bar_label, total_scanned):
    now_str = datetime.now(IST).strftime("%d-%b %H:%M:%S")
    subject = f"⚡ 26m Options Equal Down-Swing Alert ({len(triggered_list)} Matches) - [{bar_label}] ({now_str})"

    sort_key = lambda x: (x["swing_count"], x["equal_red_bars"], x.get("live_oi", 0))
    ce_matches = sorted([m for m in triggered_list if m["opt_type"] == "CE"], key=sort_key, reverse=True)
    pe_matches = sorted([m for m in triggered_list if m["opt_type"] == "PE"], key=sort_key, reverse=True)
    ordered = ce_matches + pe_matches

    ce_top5 = ", ".join([m["trading_symbol"] for m in ce_matches[:5]]) or "None"
    pe_top5 = ", ".join([m["trading_symbol"] for m in pe_matches[:5]]) or "None"
    all_syms = ", ".join([m["trading_symbol"] for m in ordered]) or "None"

    headers = [
        "Stock (Type)",
        "Option Symbol (Highest OI)",
        "Equal Down-Swing Pattern",
        "Swing 1 (High->Low | R,B)",
        "Swing 2 (High->Low | R,B)",
        "Close",
        "26m Bar"
    ]

    table_rows = [
        [
            f"{m['underlying']} ({m['opt_type']})",
            f"{m['trading_symbol']} ({max(m.get('live_oi', 0), m.get('ref_oi', 0)):,})",
            m["pattern_label"],
            m["s1_summary"],
            m["s2_summary"],
            f"{m['close']:.2f}",
            m["slot_hm"]
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
                bg = "#f0fdf4" if "3 Swings" in r[2] else ("#f8fafc" if idx % 2 == 1 else "#ffffff")
                tds = "".join(
                    f'<td style="padding:9px 14px;border:1px solid #cbd5e1;white-space:nowrap;font-size:15px;color:#0f172a;text-align:{"right" if i == 5 else "left"};font-weight:{"600" if i in (1, 2) else "400"};">{cell}</td>'
                    for i, cell in enumerate(r)
                )
                tr_list.append(f'<tr style="background-color:{bg};">{tds}</tr>')

            html_body = f"""
            <html>
            <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; font-size: 15px; color: #0f172a;">
                <p style="margin:0 0 8px 0;font-size:16px;"><strong>⚡ 26m Stock Options Equal Down-Swing Scanner (Highest OI CE &amp; PE)</strong></p>
                <p style="margin:0 0 14px 0; color:#475569;font-size:15px;">
                    <strong>26m Bar:</strong> {bar_label} &nbsp;|&nbsp;
                    <strong>Basket Scanned:</strong> {total_scanned} Highest-OI Option Contracts &nbsp;|&nbsp;
                    <strong>Matches:</strong> {len(ordered)}
                </p>
                <p style="margin:0 0 4px 0;"><strong>📋 TOP 5 CALLS (CE - Equal Down Swings Completed):</strong></p>
                <pre style="background:#f1f5f9;padding:10px;border-radius:4px;margin:0 0 12px 0;white-space:pre-wrap;font-size:14px;">{ce_top5}</pre>
                <p style="margin:0 0 4px 0;"><strong>📋 TOP 5 PUTS (PE - Equal Down Swings Completed):</strong></p>
                <pre style="background:#f1f5f9;padding:10px;border-radius:4px;margin:0 0 12px 0;white-space:pre-wrap;font-size:14px;">{pe_top5}</pre>
                <p style="margin:0 0 14px 0;font-size:14px;color:#334155;">
                    <strong>All Triggered Contracts ({len(ordered)}):</strong> {all_syms}
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

            print(f"✅ Alert Email sent successfully to {RECEIVER_EMAIL} for {len(ordered)} contract(s)!")
            return
        except Exception as e:
            print(f"⚠️ Email attempt {attempt}/3 failed: {e}")
            time.sleep(3)

# ==============================================================================
# 8. CORE 26-MINUTE SCANNER & MARKET-CLOCK SCHEDULER
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

def run_26m_scanner(force_scan=False):
    now_ist = datetime.now(IST)
    today_str = now_ist.strftime("%Y-%m-%d")
    memory = load_memory()

    need_new_basket = (
        memory.get("basket_date") != today_str
        or memory.get("basket_version") != BASKET_VERSION
        or not memory.get("options")
    )

    if need_new_basket:
        new_basket = build_highest_oi_ce_pe_basket(now_ist)
        if new_basket:
            memory["basket_date"] = today_str
            memory["basket_version"] = BASKET_VERSION
            memory["options"] = new_basket
            save_memory(memory)

    options_list = memory.get("options", [])
    if not options_list:
        print("⚠️ No Option contracts in basket.")
        return

    expected_bar_label = get_expected_26m_bar_label(now_ist)
    if not force_scan and expected_bar_label is not None:
        if memory.get("last_scanned_26m_bar") == expected_bar_label:
            print(f"ℹ️ 26m Bar [{expected_bar_label}] already scanned. Skipping duplicate pass.")
            return

    get_definedge_session_key()
    start_dt = (now_ist - timedelta(days=LOOKBACK_DAYS_1M)).replace(hour=9, minute=15, second=0, microsecond=0)
    print(f"\n🔎 Scanning {len(options_list)} Highest-OI Options on 26m Timeframe (Target Bar: {expected_bar_label or 'LATEST'})...")

    def scan_single_option(opt):
        try:
            df_1m = fetch_definedge_1m_df("NFO", opt["token"], start_dt, now_ist)
            if df_1m.empty:
                return None
            df_26m = resample_to_26m(df_1m, now_ist)
            if df_26m.empty:
                return None
            res = evaluate_26m_equal_down_swings(df_26m)
            if res:
                if expected_bar_label and res["bar_iso"] != expected_bar_label and not force_scan:
                    return None
                return {**opt, **res}
        except Exception as e:
            print(f"  ⚠️ Error scanning {opt['trading_symbol']}: {e}")
        return None

    with ThreadPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(scan_single_option, options_list))

    triggered = [r for r in results if r is not None]
    for m in triggered:
        print(
            f"  🔥 {m['pattern_label']}: {m['trading_symbol']} ({m['opt_type']} | OI: {max(m.get('live_oi', 0), m.get('ref_oi', 0)):,}) | "
            f"S1: {m['s1_summary']} | S2: {m['s2_summary']} | Close: {m['close']}"
        )

    bar_label_used = expected_bar_label or (triggered[0]["bar_iso"] if triggered else "LATEST")
    if expected_bar_label:
        memory["last_scanned_26m_bar"] = expected_bar_label
        save_memory(memory)

    if triggered:
        send_26m_equal_swing_email(triggered, bar_label_used, len(options_list))
    else:
        print(f"ℹ️ No 26m Options completed an Equal Down-Swing setup on bar [{bar_label_used}].")

def heartbeat_sleep(target_dt):
    while True:
        now_ist = datetime.now(IST)
        rem = (target_dt - now_ist).total_seconds()
        if rem <= 0:
            break
        time.sleep(min(30.0, rem))

def stay_awake_26m_clock():
    now_ist = datetime.now(IST)
    session_mode = os.environ.get("SESSION_MODE", "AUTO").upper()
    print(f"🕒 Starting 26m Equal Down-Swing Option Scanner at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')} (Mode: {session_mode})")

    is_weekday = now_ist.weekday() < 5
    in_market_window = is_weekday and (dtime(9, 0) <= now_ist.time() <= dtime(15, 32))

    # Whenever triggered manually outside live market hours, run an immediate full scan
    if not in_market_window:
        print("⚡ Running immediate single-pass 26m scan (outside live market window)...")
        run_26m_scanner(force_scan=True)
        return

    session_cutoff = dtime(12, 45) if session_mode == "MORNING" else dtime(15, 32)

    memory = load_memory()
    today_str = now_ist.strftime("%Y-%m-%d")
    if (
        memory.get("basket_date") != today_str
        or memory.get("basket_version") != BASKET_VERSION
        or not memory.get("options")
    ):
        print("🌅 Pre-warming today's Highest-OI CE & PE Option basket...")
        new_basket = build_highest_oi_ce_pe_basket(now_ist)
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
            print(f"✅ All 26m checkpoints for {session_mode} completed.")
            break

        next_dt = future_points[0]
        sleep_mins = round((next_dt - now_ist).total_seconds() / 60.0, 1)
        print(f"💤 Sleeping {sleep_mins} min until next 26m checkpoint: {next_dt.strftime('%H:%M:%S IST')}...")
        heartbeat_sleep(next_dt)

        print(f"\n🔔 26m Checkpoint reached at {datetime.now(IST).strftime('%H:%M:%S IST')}. Running scan...")
        run_26m_scanner(force_scan=False)

if __name__ == "__main__":
    stay_awake_26m_clock()
