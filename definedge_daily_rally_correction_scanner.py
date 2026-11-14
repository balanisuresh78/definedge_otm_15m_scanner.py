import os
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
MEMORY_FILE = "rally_corr_daily_memory.json"

# ==============================================================================
# DAILY (1D) NIFTY 500 "WRB RALLY + SLOW TIME-CORRECTION" PARAMETERS
# ==============================================================================
DATA_SEGMENT = "NSE"                  # Scans Continuous NSE Cash Spot across Nifty 500
ATR_FAST_PERIOD = 2                   # ATR(2) for Wide-Range Bar thrust check
ATR_SLOW_PERIOD = 100                 # ATR(100) baseline
ATR_METHOD = "SMA"                    # "SMA" or "RMA"

# Phase 0: "Just Started Rallying" (Launched from a Prior Base / Low)
PRIOR_BASE_LOOKBACK = 20              # 20 trading days prior to rally start
MAX_START_POS_IN_PRIOR_RANGE = 0.65   # Rally Start Low must sit in the lower 65% of the prior 20-day range

# Phase 1: Impulse Rally Rules (Anchored at First Bullish WRB)
WRB_ATR2_PREV_MULT = 1.5              # WRB Rule: ATR(2) >= 1.5x Previous Bar's ATR(2)
WRB_ATR2_VS_ATR100_MIN = 1.05         # WRB Rule: ATR(2) > ATR(100) (blue ATR 2 line above black ATR 100 line)
WRB_BODY_VS_ATR100_MIN = 0.75         # WRB Rule: Bullish body |Close - Open| >= 0.75x ATR(100)
MIN_WRB_COUNT_IN_RALLY = 1            # At least 1 wide-range bullish bar in the rally
MAX_DOWN_BARS_IN_RALLY = 3            # Allow 0, 1, 2, or max 3 small-body corrective down bars inside rally
SMALL_DOWN_BODY_ATR100_MAX = 0.80     # Down bar body inside rally <= 0.80 * ATR(100)
SMALL_DOWN_BODY_VS_UP_MAX = 0.50      # Down bar body inside rally <= 50% of max bullish body in the rally
MIN_RALLY_BARS = 1                    # 1 to 10 bars in the impulse rally leg
MAX_RALLY_BARS = 10                   # Max bars in the impulse rally leg
MIN_RALLY_RANGE_ATR100 = 1.8          # Total rally height (Peak High - Start Low) >= 1.8x ATR(100)

# Phase 2: Slow Time-Correction Rules (Correction Time > Rally Time)
MAX_CORRECTION_BARS = 40              # Max trading days to look back for the rally peak
MIN_RETRACEMENT_OF_RALLY = 0.15       # Pullback low retraces at least 15% from peak high
MAX_RETRACEMENT_OF_RALLY = 0.88       # Pullback low stays strictly above Rally Start Low (<= 88% retracement)

# Lookback (300 calendar days = ~200 trading days for accurate ATR(100))
LOOKBACK_DAYS_DAILY = 300

SCAN_CHECKPOINTS_IST = [
    dtime(15, 30, 15)
]

# ==============================================================================
# BUILT-IN NIFTY 500 CONSTITUENT LIST (PLUS LIVE NSE ARCHIVE AUTO-UPDATE)
# ==============================================================================
FALLBACK_NIFTY_500_STOCKS = [
    "360ONE", "3MINDIA", "ABB", "ACC", "AIAENG", "APLAPOLLO", "AUBANK", "AARTIIND", "AAVAS", "ABBOTINDIA",
    "ACE", "ADANIENSOL", "ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER", "ATGL", "AWL", "ABCAPITAL", "ABFRL",
    "AEGISLOG", "AETHER", "AFFLE", "AJANTPHARM", "APLLTD", "ALKEM", "ALKYLAMINE", "ALLCARGO", "ALOKINDS", "ARE&M",
    "AMBER", "AMBUJACEM", "ANANDRATHI", "ANGELONE", "ANURAS", "APARINDS", "APOLLOHOSP", "APOLLOTYRE", "APTUS", "ACI",
    "ASAHIINDIA", "ASHOKLEY", "ASIANPAINT", "ASTERDM", "ASTRAL", "ATUL", "AUROPHARMA", "AVANTIFEED", "DMART", "AXISBANK",
    "BEML", "BLS", "BSE", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BAJAJHLDNG", "BALAMINES", "BALKRISIND", "BALRAMCHIN",
    "BANDHANBNK", "BANKBARODA", "BANKINDIA", "MAHABANK", "BATAINDIA", "BAYERCROP", "BERGEPAINT", "BDL", "BEL", "BHARATFORG",
    "BHEL", "BPCL", "BHARTIARTL", "BIKAJI", "BIOCON", "BIRLACORPN", "BSOFT", "BLUEDART", "BLUESTARCO", "BBTC",
    "BORORENEW", "BOSCHLTD", "BRIGADE", "BRITANNIA", "MAPMYINDIA", "CCL", "CESC", "CGPOWER", "CIEINDIA", "CRISIL",
    "CSBBANK", "CAMPUS", "CANFINHOME", "CANBK", "CAPLIPOINT", "CGCL", "CARBORUNIV", "CASTROLIND", "CEATLTD", "CELLO",
    "CENTRALBK", "CDSL", "CENTURYPLY", "CENTURYTEX", "CERA", "CHALET", "CHAMBLFERT", "CHEMPLASTS", "CHENNPETRO", "CHOLAHLDNG",
    "CHOLAFIN", "CIPLA", "CUB", "CLEAN", "COALINDIA", "COCHINSHIP", "COFORGE", "COLPAL", "CAMS", "CONCORDBIO",
    "CONCOR", "COROMANDEL", "CRAFTSMAN", "CREDITACC", "CROMPTON", "CUMMINSIND", "CYIENT", "DCMSHRIRAM", "DLF", "DOMS",
    "DABUR", "DALBHARAT", "DATAPATTNS", "DEEPAKFERT", "DEEPAKNTR", "DELHIVERY", "DEVYANI", "DIVISLAB", "DIXON", "LALPATHLAB",
    "DRREDDY", "EIDPARRY", "EIHOTEL", "EPL", "EASEMYTRIP", "EICHERMOT", "ELECON", "ELGIEQUIP", "EMAMILTD", "ENDURANCE",
    "ENGINERSIN", "EQUITASBNK", "ERIS", "ESCORTS", "ETERNAL", "EXIDEIND", "FDC", "NYKAA", "FEDERALBNK", "FACT",
    "FINEORG", "FINCABLES", "FINPIPE", "FSL", "FIVESTAR", "FORTIS", "GAIL", "GMMPFAUDLR", "GMRINFRA", "GRSE",
    "GICRE", "GILLETTE", "GLAND", "GLAXO", "GLENMARK", "MEDANTA", "GODIGIT", "GPIL", "GODFRYPHLP", "GODREJCP",
    "GODREJIND", "GODREJPROP", "GRANULES", "GRAPHITE", "GRASIM", "GESHIP", "GRINDWELL", "GAEL", "FLUOROCHEM", "GUJGASLTD",
    "GMDCLTD", "GNFC", "GPPL", "GSFC", "GSPL", "HEG", "HBLPOWER", "HCLTECH", "HDFCAMC", "HDFCBANK",
    "HDFCLIFE", "HFCL", "HAPPSTMNDS", "HAPPYFORGE", "HAVELLS", "HEROMOTOCO", "HSCL", "HINDALCO", "HAL", "HINDCOPPER",
    "HINDPETRO", "HINDUNILVR", "HINDZINC", "POWERINDIA", "HOMEFIRST", "HONASA", "HONAUT", "HUDCO", "ICICIBANK", "ICICIGI",
    "ICICIPRULI", "ISEC", "IDBI", "IDFCFIRSTB", "IFCI", "IIFL", "IRB", "IRCON", "ITC", "ITI",
    "INDIACEM", "IBULHSGFIN", "INDIAMART", "INDIANB", "IEX", "INDHOTEL", "IOC", "IOB", "IRCTC", "IRFC",
    "IREDA", "IGL", "INDUSTOWER", "INDUSINDBK", "NAUKRI", "INFY", "INOXWIND", "INTELLECT", "INDIGO", "IPCALAB",
    "JBCHEPHARM", "JKCEMENT", "JBMA", "JKLAKSHMI", "JKPAPER", "JMFINANCIL", "JSWENERGY", "JSWINFRA", "JSWSTEEL", "JAIBALAJI",
    "JAMNAAUTO", "JINDALSAW", "JSL", "JINDALSTEL", "JIOFIN", "JUBLFOOD", "JUBLINGREA", "JUBLPHARMA", "JWL", "JUSTDIAL",
    "JYOTHYLAB", "JYOTICNC", "KPRMILL", "KEI", "KNRCON", "KPITTECH", "KRBL", "KSB", "KAJARIACER", "KPIL",
    "KALYANJWL", "KANSAINER", "KARURVYSYA", "KAYNES", "KEC", "KFINTECH", "KOTAKBANK", "KIMS", "LTF", "LTTS",
    "LICHSGFIN", "LTIM", "LT", "LATENTVIEW", "LAURUSLABS", "LXCHEM", "LEMONTREE", "LICI", "LINDEINDIA", "LLOYDSME",
    "LUPIN", "MMTC", "MRF", "MTARTECH", "LODHA", "MGL", "MAHSEAMLES", "M&MFIN", "M&M", "MAHLIFE",
    "MANAPPURAM", "MRPL", "MANKIND", "MARICO", "MARUTI", "MASTEK", "MFSL", "MAXHEALTH", "MAZDOCK", "MEDPLUS",
    "METROBRAND", "METROPOLIS", "MINDACORP", "MSUMI", "MOTILALOFS", "MPHASIS", "MCX", "MUTHOOTFIN", "NATCOPHARM", "NBCC",
    "NCC", "NHPC", "NLCINDIA", "NMDC", "NSLNISP", "NTPC", "NH", "NATIONALUM", "NAVINFLUOR", "NESTLEIND",
    "NETWORK18", "NEWGEN", "NAM-INDIA", "NUVAMA", "NUVOCO", "OBEROIRLTY", "ONGC", "OIL", "OLECTRA", "PAYTM",
    "OFSS", "POLICYBZR", "PCBL", "PIIND", "PNBHOUSING", "PNCINFRA", "PVRINOX", "PAGEIND", "PATANJALI", "PERSISTENT",
    "PETRONET", "PFIZER", "PHOENIXLTD", "PIDILITIND", "PEL", "PPLPHARMA", "POLYMED", "POLYCAB", "POONAWALLA", "PFC",
    "POWERGRID", "PRAJIND", "PRESTIGE", "PRINCEPIPE", "PRSMJOHNSN", "PGHH", "PNB", "QUESS", "RRKABEL", "RBLBANK",
    "RECLTD", "RHIM", "RITES", "RADICO", "RVNL", "RAILTEL", "RAINBOW", "RAJESHEXPO", "RKFORGE", "RCF",
    "RATNAMANI", "RTNINDIA", "RAYMOND", "REDINGTON", "RELIANCE", "RBA", "ROUTE", "SBFC", "SBICARD", "SBILIFE",
    "SJVN", "SKFINDIA", "SRF", "SAFARI", "SAIL", "SANOFI", "SAPPHIRE", "SAREGAMA", "SCHAEFFLER", "SCHNEIDER",
    "SHREECEM", "RENUKA", "SHRIRAMFIN", "SHYAMMETL", "SIEMENS", "SIGNATURE", "SOBHA", "SOLARINDS", "SONACOMS", "SONATSOFTW",
    "STARHEALTH", "SBIN", "SAIL", "SWSOLAR", "STLTECH", "SUMICHEM", "SPARC", "SUNPHARMA", "SUNTV", "SUNDARMFIN",
    "SUNDRMFAST", "SUPREMEIND", "SUVENPHAR", "SUZLON", "SWANENERGY", "SYNGENE", "SYRMA", "TBOTEK", "TVSMOTOR", "TVSSCS",
    "TANLA", "TATACHEM", "TATACOMM", "TCS", "TATACONSUM", "TATAELXSI", "TATAINVEST", "TMPV", "TATAMOTORS", "TATAPOWER",
    "TATASTEEL", "TATATECH", "TTML", "TECHM", "TEJASNET", "NIACL", "RAMCOCEM", "THERMAX", "TIMKEN", "TITAGARH",
    "TITAN", "TORNTPHARM", "TORNTPOWER", "TRENT", "TRIDENT", "TRIVENI", "TRITURBINE", "TIINDIA", "UCOBANK", "UNOMINDA",
    "UPL", "UTIAMC", "UJJIVANSFB", "ULTRACEMCO", "UNIONBANK", "UBL", "UNITDSPR", "USHAMART", "VGUARD", "VIPIND",
    "VAIBHAVGBL", "VTL", "VARROC", "VBL", "MANYAVAR", "VEDL", "VIJAYA", "VINATIORGA", "IDEA", "VOLTAS",
    "WELCORP", "WELSPUNLIV", "WESTLIFE", "WHIRLPOOL", "WIPRO", "YESBANK", "ZEEL", "ZENSARTECH", "ZOMATO", "ZYDUSLIFE",
    "ECLERX"
]

SESSION_CACHE = {"key": "", "date": ""}

# Shared HTTP Session with connection pooling for fast 500-stock scanning
HTTP_SESSION = requests.Session()
adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=Retry(total=2, backoff_factor=0.3))
HTTP_SESSION.mount("https://", adapter)
HTTP_SESSION.mount("http://", adapter)

# ==============================================================================
# 2. JSON MEMORY PERSISTENCE
# ==============================================================================
def load_memory():
    default_mem = {"last_scanned_daily_bar": ""}
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
# 3. DEFINEDGE AUTH, NIFTY 500 UNIVERSE LOADER & DAILY DATA ENGINE
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

def get_nifty_500_symbol_set():
    """Fetches official Nifty 500 list from NSE Archives and merges with built-in 500 list."""
    symbols = set(FALLBACK_NIFTY_500_STOCKS)
    urls = [
        "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
        "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"
    ]
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    for u in urls:
        try:
            r = HTTP_SESSION.get(u, headers=headers, timeout=10)
            if r.status_code == 200 and "Symbol" in r.text:
                df_n500 = pd.read_csv(io.StringIO(r.text))
                if "Symbol" in df_n500.columns:
                    live_syms = set(df_n500["Symbol"].dropna().astype(str).str.strip().tolist())
                    if len(live_syms) >= 400:
                        symbols.update(live_syms)
                        print(f"📥 Loaded live Nifty 500 constituent list ({len(live_syms)} symbols).")
                        break
        except Exception:
            pass
    return symbols

def load_definedge_nifty500_tokens():
    nifty500_set = get_nifty_500_symbol_set()

    url = "https://app.definedgesecurities.com/public/allmaster.zip"
    resp = HTTP_SESSION.get(url, timeout=30)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pd.read_csv(f, header=None, dtype=str, low_memory=False)

    nse_eq = df[
        (df[0] == "NSE")
        & (df[2].str.strip().isin(nifty500_set))
        & (df[4].str.strip() == "EQ")
    ].copy()
    eq_map = {row[2].strip(): row[1].strip() for _, row in nse_eq.iterrows()}
    print(f"✅ Mapped {len(eq_map)} Nifty 500 NSE Cash (EQ) stocks from Definedge Master.")
    return eq_map

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
            df["date"] = pd.to_datetime(raw_d, format="%d%m%Y", errors="coerce")
            df = df.dropna(subset=["date"]).sort_values("date")

            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["open", "high", "low", "close"]).drop_duplicates(subset=["date"], keep="last")
            df = df.set_index("date").sort_index()
            return df
        except Exception as e:
            if attempt == 2:
                print(f"⚠️ Daily fetch error ({segment}/{token}): {e}")
            time.sleep(1.5)
    return pd.DataFrame()

# ==============================================================================
# 4. DAILY INDICATORS (ATR 2, ATR 100, STOCH 5,3,3)
# ==============================================================================
def compute_indicators_daily(df_day):
    df = df_day.copy()
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

    # Bullish Wide-Range Bar (WRB):
    # 1. Bullish candle (Close > Open and Close > Prev Close)
    # 2. ATR(2) >= 1.5x Previous Bar's ATR(2)
    # 3. ATR(2) >= 1.05x ATR(100) (ATR 2 line sits above ATR 100 line)
    # 4. Bullish Body >= 0.75x ATR(100)
    df["is_bull_wrb"] = (
        (df["close"] > df["open"])
        & (df["close"] > prev_close)
        & (df["atr2_ratio"] >= WRB_ATR2_PREV_MULT)
        & (df["atr2_vs_100"] >= WRB_ATR2_VS_ATR100_MIN)
        & (df["body_vs_100"] >= WRB_BODY_VS_ATR100_MIN)
    )
    return df

# ==============================================================================
# 5. PATTERN DETECTOR: WRB-ANCHORED IMPULSE RALLY + SLOWER TIME-CORRECTION
# ==============================================================================
def check_wrb_anchored_rally_and_correction(df, start_idx, peak_idx, end_corr_idx):
    """
    Evaluates [start_idx .. peak_idx] as the Impulse Rally and [peak_idx + 1 .. end_corr_idx]
    as the Slow Time-Correction:
      - `start_idx` is anchored directly to the first Bullish WRB (or the single bullish bar
        immediately before the first WRB) so prior sideways base bars are NEVER mixed in.
      - Rally has >= 1 Bullish WRB (ATR2 >= 1.5x Prev ATR2) and 0..3 small-body red bars.
      - Correction consumes strictly MORE days than the rally (corr_bars > rally_bars)
        and holds strictly above the starting low of the rally.
    """
    rally_bars = peak_idx - start_idx + 1
    corr_bars = end_corr_idx - peak_idx

    if rally_bars < MIN_RALLY_BARS or rally_bars > MAX_RALLY_BARS:
        return None
    if corr_bars <= rally_bars or corr_bars > MAX_CORRECTION_BARS:
        return None

    rally_slice = df.iloc[start_idx : peak_idx + 1]
    corr_slice = df.iloc[peak_idx + 1 : end_corr_idx + 1]

    # 1. Rally must start on a Bullish WRB (or a bullish launch bar immediately followed by a WRB)
    first_bar = rally_slice.iloc[0]
    if float(first_bar["close"]) <= float(first_bar["open"]):
        return None

    starts_with_wrb = bool(first_bar["is_bull_wrb"]) or (
        len(rally_slice) >= 2 and bool(rally_slice.iloc[1]["is_bull_wrb"])
    )
    if not starts_with_wrb:
        return None

    rally_start_low = float(first_bar["low"])
    if float(rally_slice["low"].min()) < rally_start_low:
        return None

    # 2. Peak bar must be the highest point of the rally
    rally_peak_high = float(rally_slice.iloc[-1]["high"])
    rally_peak_close = float(rally_slice.iloc[-1]["close"])
    if float(rally_slice["high"].max()) > rally_peak_high:
        return None

    rally_range = rally_peak_high - rally_start_low
    ref_atr100 = float(df["atr100"].iloc[end_corr_idx] or 0.0)
    if ref_atr100 <= 0 or rally_range < (MIN_RALLY_RANGE_ATR100 * ref_atr100):
        return None

    # 3. Phase 0 ("Just Started Rallying"): Must launch from a prior base / low
    prior_start = max(0, start_idx - PRIOR_BASE_LOOKBACK)
    prior_slice = df.iloc[prior_start:start_idx]
    if len(prior_slice) < 10:
        return None

    # Rally peak must break out above the prior 15 bars before the rally started
    recent_prior = df.iloc[max(0, start_idx - 15) : start_idx]
    if float(recent_prior["high"].max()) >= rally_peak_high:
        return None

    # Rally Start Low must be in the lower 65% of the prior 20-bar range
    context_high = max(float(prior_slice["high"].max()), rally_peak_high)
    context_low = min(float(prior_slice["low"].min()), rally_start_low)
    if (context_high - context_low) > 0:
        start_pos = (rally_start_low - context_low) / (context_high - context_low)
        if start_pos > MAX_START_POS_IN_PRIOR_RANGE:
            return None

    # 4. Phase 1: At least 1 Bullish WRB + Max 3 Small-Body Corrective Down Bars inside Rally
    wrb_bars = rally_slice[rally_slice["is_bull_wrb"]]
    if len(wrb_bars) < MIN_WRB_COUNT_IN_RALLY:
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

    # 5. Phase 2: Slow Time-Correction Above Rally Start Low
    corr_low = float(corr_slice["low"].min())
    corr_max_close = float(corr_slice["close"].max())
    corr_high = float(corr_slice["high"].max())
    cur_close = float(corr_slice["close"].iloc[-1])

    # Must hold strictly above the starting point of the rally
    if corr_low <= rally_start_low:
        return None

    # Correction must not have already closed above the rally peak high (still correcting/coiling)
    if corr_max_close > rally_peak_high or corr_high > (rally_peak_high + 0.10 * rally_range):
        return None

    retracement_pct = round(((rally_peak_high - corr_low) / rally_range) * 100.0, 1)
    if retracement_pct < (MIN_RETRACEMENT_OF_RALLY * 100.0) or retracement_pct > (MAX_RETRACEMENT_OF_RALLY * 100.0):
        return None

    # Current price should still be below the rally peak (in correction / flag zone)
    if cur_close >= rally_peak_high or cur_close <= rally_start_low:
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
        "rally_start_date": rally_slice.index[0].strftime("%d-%b"),
        "rally_peak_date": rally_slice.index[-1].strftime("%d-%b"),
        "rally_start_low": round(rally_start_low, 2),
        "rally_peak_high": round(rally_peak_high, 2),
        "corr_low": round(corr_low, 2),
        "retracement_pct": retracement_pct,
        "rally_gain_pct": round(((rally_peak_high - rally_start_low) / rally_start_low) * 100.0, 1)
    }

def evaluate_daily_rally_slow_correction(df_day):
    if len(df_day) < 50:
        return None

    df = compute_indicators_daily(df_day)
    cur_idx = len(df) - 1
    cur_bar = df.iloc[cur_idx]

    best_match = None
    min_peak_idx = max(PRIOR_BASE_LOOKBACK + 2, cur_idx - MAX_CORRECTION_BARS)

    for peak_idx in range(cur_idx - 2, min_peak_idx - 1, -1):
        min_start_idx = max(PRIOR_BASE_LOOKBACK, peak_idx - MAX_RALLY_BARS + 1)
        # Anchor to the earliest valid WRB start_idx for this peak (full impulse rally leg)
        full_rally_for_peak = None
        for start_idx in range(min_start_idx, peak_idx + 1):
            res = check_wrb_anchored_rally_and_correction(df, start_idx, peak_idx, cur_idx)
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
        "bar_iso": cur_bar.name.strftime("%Y-%m-%d"),
        "slot_hm": cur_bar.name.strftime("%d-%b-%Y"),
        "close": round(float(cur_bar["close"]), 2),
        "cur_atr2_ratio": round(float(cur_bar["atr2_ratio"]), 2),
        "stoch_k": round(float(cur_bar["stoch_k"] or 0.0), 1),
        **best_match
    }

# ==============================================================================
# 6. 15px HTML EMAIL ALERT SENDER (SORTED BY MAX TIME RATIO AT TOP)
# ==============================================================================
def send_daily_pattern_email(triggered_list, bar_label, total_scanned):
    now_str = datetime.now(IST).strftime("%d-%b %H:%M:%S")
    subject = f"🔥 Nifty 500 Daily (1D) WRB Rally + Slow Correction ({len(triggered_list)} Matches) - [{bar_label}] ({now_str})"

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
        "Corr Days",
        "Rally Days (WRB | Red)",
        "Rally Window",
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
            f"{m['corr_bars']} days",
            f"{m['rally_bars']}d ({m['wrb_count']} WRB | {m['down_bars_in_rally']} Red)",
            f"{m['rally_start_date']} -> {m['rally_peak_date']}",
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
                    f'<td style="padding:9px 14px;border:1px solid #cbd5e1;white-space:nowrap;font-size:15px;color:#0f172a;text-align:{"right" if i in (1, 2, 5, 6, 9) else "left"};font-weight:{"600" if i in (0, 1) else "400"};">{cell}</td>'
                    for i, cell in enumerate(r)
                )
                tr_list.append(f'<tr style="background-color:{bg};">{tds}</tr>')

            html_body = f"""
            <html>
            <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; font-size: 15px; color: #0f172a;">
                <p style="margin:0 0 8px 0;font-size:16px;"><strong>🔥 Nifty 500 Daily (1D) WRB Impulse Rally + Slow Time-Correction (Sorted by Max Time Ratio)</strong></p>
                <p style="margin:0 0 14px 0; color:#475569;font-size:15px;">
                    <strong>Daily Candle:</strong> {bar_label} &nbsp;|&nbsp;
                    <strong>Universe Scanned:</strong> {total_scanned} Nifty 500 Stocks &nbsp;|&nbsp;
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
# 7. CORE DAILY SCANNER & SCHEDULER
# ==============================================================================
def run_daily_scanner(force_scan=False):
    now_ist = datetime.now(IST)
    memory = load_memory()

    get_definedge_session_key()
    token_map = load_definedge_nifty500_tokens()
    symbols_list = sorted(token_map.keys())

    start_dt = (now_ist - timedelta(days=LOOKBACK_DAYS_DAILY)).replace(hour=9, minute=15, second=0, microsecond=0)
    print(f"\n🔎 Scanning {len(symbols_list)} Nifty 500 Stocks on DAILY (1D) Timeframe...")

    def scan_single_stock(symbol):
        token = token_map.get(symbol)
        if not token:
            return None
        try:
            df_day = fetch_definedge_day_df(DATA_SEGMENT.upper(), token, start_dt, now_ist)
            if df_day.empty:
                return None
            res = evaluate_daily_rally_slow_correction(df_day)
            if res:
                res["symbol"] = symbol
                return res
        except Exception as e:
            print(f"  ⚠️ Error scanning {symbol}: {e}")
        return None

    with ThreadPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(scan_single_stock, symbols_list))

    triggered = sorted(
        [r for r in results if r is not None],
        key=lambda x: (x["time_ratio"], x["max_wrb_atr2_ratio"]),
        reverse=True
    )

    for m in triggered:
        print(
            f"  🔥 MATCH: {m['symbol']} | Time Ratio [Corr/Rally]: {m['time_ratio']:.2f}x "
            f"({m['corr_bars']}d / {m['rally_bars']}d | Rally: {m['rally_start_date']}->{m['rally_peak_date']}) | "
            f"WRB: {m['wrb_count']} (ATR2 Thrust: {m['max_wrb_atr2_ratio']}x prev, {m['max_wrb_vs_100']}x ATR100) | "
            f"Red in Rally: {m['down_bars_in_rally']} | Retr: {m['retracement_pct']}% | StochK: {m['stoch_k']}"
        )

    bar_label_used = triggered[0]["bar_iso"] if triggered else now_ist.strftime("%Y-%m-%d")
    if not force_scan and memory.get("last_scanned_daily_bar") == bar_label_used:
        print(f"ℹ️ Daily Bar [{bar_label_used}] already scanned. Skipping duplicate email.")
        return

    memory["last_scanned_daily_bar"] = bar_label_used
    save_memory(memory)

    if triggered:
        send_daily_pattern_email(triggered, bar_label_used, len(symbols_list))
    else:
        print(f"ℹ️ No Daily (1D) WRB Rally + Slow Correction setups matched on [{bar_label_used}].")

def heartbeat_sleep(target_dt):
    while True:
        now_ist = datetime.now(IST)
        rem = (target_dt - now_ist).total_seconds()
        if rem <= 0:
            break
        time.sleep(min(30.0, rem))

def stay_awake_daily_clock():
    now_ist = datetime.now(IST)
    print(f"🕒 Starting Nifty 500 Daily (1D) WRB Rally + Slow Correction Scanner at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")

    is_weekday = now_ist.weekday() < 5
    in_pre_close_window = is_weekday and (dtime(15, 0) <= now_ist.time() <= dtime(15, 35))

    if not in_pre_close_window:
        print("⚡ Running immediate single-pass Nifty 500 Daily (1D) scan...")
        run_daily_scanner(force_scan=True)
        return

    target_dt = IST.localize(datetime.combine(now_ist.date(), SCAN_CHECKPOINTS_IST[0]))
    if now_ist < target_dt:
        sleep_mins = round((target_dt - now_ist).total_seconds() / 60.0, 1)
        print(f"💤 Sleeping {sleep_mins} min until 15:30:15 IST Daily Close...")
        heartbeat_sleep(target_dt)

    print(f"\n🔔 Daily Close reached at {datetime.now(IST).strftime('%H:%M:%S IST')}. Running scan...")
    run_daily_scanner(force_scan=False)

if __name__ == "__main__":
    stay_awake_daily_clock()
