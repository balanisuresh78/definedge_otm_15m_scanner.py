import os
import re
import sys
import time
import json
import zipfile
import io
import smtplib
import subprocess
import pyotp
import requests
import pytz
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, time as dtime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from tabulate import tabulate
from integrate import ConnectToIntegrate, IntegrateData, IntegrateOrders

# Force real-time unbuffered logging in GitHub Actions console
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

# ==============================================================================
# 1. CREDENTIALS & CONFIGURATION (LOADED SECURELY FROM GITHUB SECRETS)
# ==============================================================================
API_TOKEN = os.environ.get("DEFINEDGE_API_TOKEN", "").strip()
API_SECRET = os.environ.get("DEFINEDGE_API_SECRET", "").strip()
TOTP_SECRET = os.environ.get("DEFINEDGE_TOTP_SECRET", "").strip()

SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "balanisuresh@gmail.com").strip()
RECEIVER_EMAIL = os.environ.get("RECEIVER_EMAIL", "balanisuresh@gmail.com").strip()
APP_PASSWORD = os.environ.get("APP_PASSWORD", "").strip()

IST = pytz.timezone("Asia/Kolkata")
MEMORY_FILE = "otm_15m_memory.json"
MEMORY_BRANCH = "otm-15m-memory-state"
BASKET_VERSION = "v3_yest_filter_orders"

# 15m ATR(2) & Volume Expansion Parameters
ATR_PERIOD = 2
ATR_MULT_THRESHOLD = 1.5
VOL_MULT_THRESHOLD = 1.5
ATR_METHOD = "SMA"  # "SMA" or "RMA"

# Yesterday Trend Filter Mode for Morning Basket Harvesting:
# "CLOSE_VS_PREV_CLOSE" -> Down Day = Yesterday Close < Day-Before Close | Up Day = Yesterday Close > Day-Before Close
# "CLOSE_VS_OPEN"       -> Down Day = Yesterday Close < Yesterday Open   | Up Day = Yesterday Close > Yesterday Open
YESTERDAY_TREND_MODE = "CLOSE_VS_PREV_CLOSE"

# ==============================================================================
# LIVE ORDER EXECUTION SETTINGS (DEFINEDGE INTEGRATE ORDERS)
# ==============================================================================
ENABLE_LIVE_ORDERS = True          # Set True to send real BUY orders to Definedge, False for email-only
LOTS_PER_TRADE = 1                 # Number of lots to buy per qualifying option
MAX_ORDERS_PER_BAR = 2             # Max BUY orders per 15m candle (picks highest ATR Ratio matches first)
MAX_ORDERS_PER_DAY = 6             # Max total BUY orders across the entire trading day
ORDER_PRODUCT_TYPE = "INTRADAY"    # "INTRADAY" (MIS) or "NORMAL" (NRML carry-forward)
ORDER_PRICE_TYPE = "LIMIT"         # "LIMIT" (Close + buffer%, safer for OPTSTK) or "MARKET"
LIMIT_SLIPPAGE_PCT = 1.0           # +1.0% buffer above 15m close price when ORDER_PRICE_TYPE = "LIMIT"

LOOKBACK_DAYS_15M = 7
MIN_DAYS_TO_EXPIRY = 0  # 0 = current active monthly expiry

# Curated Liquid High-Beta 50 Subset
high_beta_50_stocks = [
    # High-Beta Banks & NBFCs (12)
    "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "BANKBARODA", "CANBK", "CHOLAFIN",
    "FEDERALBNK", "ICICIBANK", "INDUSINDBK", "PNB", "SBIN", "SHRIRAMFIN",
    # Capital Markets & Fintech (4)
    "ANGELONE", "BSE", "CDSL", "MCX",
    # Defence, Railways & PSU Power/CapEx (8)
    "BDL", "BEL", "BHEL", "HAL", "IRFC", "MAZDOCK", "PFC", "RECLTD",
    # Capital Goods, EMS & Industrials (7)
    "ABB", "CGPOWER", "CUMMINSIND", "DIXON", "KAYNES", "POLYCAB", "SIEMENS",
    # Metals & Energy Movers (7)
    "ADANIENT", "ADANIPORTS", "HINDALCO", "HINDCOPPER", "JINDALSTEL", "TATASTEEL", "VEDL",
    # Auto, Retail & New-Age High Beta (6)
    "BAJAJ-AUTO", "M&M", "MOTHERSON", "TATAMOTORS", "TRENT", "ZOMATO",
    # Realty & High-Beta Tech (6)
    "COFORGE", "DLF", "GODREJPROP", "OBEROIRLTY", "OFSS", "PERSISTENT"
]

# ==============================================================================
# 2. PERSISTENT MEMORY MANAGEMENT (REAL-TIME SYNC TO otm-15m-memory-state)
# ==============================================================================
def default_memory():
    return {
        "basket_date": "",
        "basket_is_live": False,
        "basket_version": "",
        "last_scanned_15m_bar": "",
        "orders_placed_today": [],
        "options": []
    }

def load_memory():
    if not os.path.exists(MEMORY_FILE):
        return default_memory()
    try:
        with open(MEMORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if "options" not in data:
                return default_memory()
            if "orders_placed_today" not in data:
                data["orders_placed_today"] = []
            return data
    except Exception as e:
        print(f"⚠️ Could not read {MEMORY_FILE}, starting fresh: {e}")
        return default_memory()

def push_memory_to_git():
    """Immediately persists otm_15m_memory.json to otm-15m-memory-state without touching main."""
    if not os.environ.get("GITHUB_ACTIONS"):
        return
    try:
        cmds = [
            f"cp {MEMORY_FILE} /tmp/{MEMORY_FILE}_backup.json",
            'git config user.name "github-actions[bot]"',
            'git config user.email "github-actions[bot]@users.noreply.github.com"',
            f"git checkout -B {MEMORY_BRANCH}",
            f"cp /tmp/{MEMORY_FILE}_backup.json {MEMORY_FILE}",
            f"git add {MEMORY_FILE}",
            f'git commit -m "Update {MEMORY_FILE}" || true',
            f"git push -f origin {MEMORY_BRANCH}",
            "git checkout -f main || git checkout -f master",
            f"cp /tmp/{MEMORY_FILE}_backup.json {MEMORY_FILE}"
        ]
        subprocess.run(" && ".join(cmds), shell=True, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        print(f"⚠️ Git memory sync warning: {e}")

def save_memory(memory_data):
    with open(MEMORY_FILE, "w", encoding="utf-8") as f:
        json.dump(memory_data, f, indent=2, sort_keys=True)
    print(f"💾 Memory saved to {MEMORY_FILE} ({len(memory_data.get('options', []))} OTM options tracked).")
    push_memory_to_git()

# ==============================================================================
# 3. DEFINEDGE LOGIN & AUTO-RECONNECT WRAPPER
# ==============================================================================
def login_definedge():
    conn = ConnectToIntegrate()
    totp = pyotp.TOTP(TOTP_SECRET).now()
    conn.login(api_token=API_TOKEN, api_secret=API_SECRET, totp=totp)
    return conn, IntegrateData(conn)

def with_connection_retry(func, *args, **kwargs):
    """Wraps Definedge API calls with automatic re-login on connection drops."""
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            conn, ic = login_definedge()
            return func(conn, ic, *args, **kwargs)
        except Exception as e:
            print(f"⚠️ Definedge connection error on attempt {attempt}/{max_retries}: {e}")
            if attempt == max_retries:
                raise
            time.sleep(5)

# ==============================================================================
# 4. MASTER SYMBOL PARSING & FILTERED BASKET BUILDER (DOWN YEST -> CE | UP YEST -> PE)
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
        return strike, opt_type
    return None, None

def load_nfo_master_data(conn):
    today = datetime.now(IST).date()
    min_expiry = today + timedelta(days=MIN_DAYS_TO_EXPIRY)
    stock_set = set(high_beta_50_stocks)

    url = "https://app.definedgesecurities.com/public/allmaster.zip"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            df = pd.read_csv(f, header=None, dtype=str, low_memory=False)

    nfo = df[(df[0] == "NFO") & (df[2].str.strip().isin(stock_set))].copy()
    nfo[2] = nfo[2].str.strip()
    nfo[3] = nfo[3].str.strip()
    nfo[4] = nfo[4].str.strip()

    for exp_col in [5, 8, 6, 7]:
        if exp_col in nfo.columns:
            parsed = pd.to_datetime(nfo[exp_col], format="%d%m%Y", errors="coerce")
            if parsed.notna().sum() > 0:
                nfo["expiry"] = parsed.dt.date
                break

    nfo = nfo[nfo["expiry"].notna() & (nfo["expiry"] >= min_expiry)]

    fut_df = nfo[nfo[4] == "FUTSTK"].sort_values("expiry").groupby(2, as_index=False).first()
    fut_map = {row[2]: {"trading_symbol": row[3], "expiry": row["expiry"]} for _, row in fut_df.iterrows()}

    opt_df = nfo[nfo[4] == "OPTSTK"].copy()
    min_opt_exp = opt_df.groupby(2)["expiry"].transform("min")
    opt_df = opt_df[opt_df["expiry"] == min_opt_exp]

    options_by_stock = {s: [] for s in stock_set}
    for _, row in opt_df.iterrows():
        und = row[2]
        trd_sym = row[3]
        strike, opt_type = extract_strike_and_type_from_row(row, und, trd_sym)
        # Column 7 in Definedge allmaster.zip is LOTSIZE
        try:
            lot_size = int(float(row[7])) if (7 in row.index and pd.notna(row[7]) and float(row[7]) > 0) else 1
        except Exception:
            lot_size = 1

        if strike is not None and opt_type in ("CE", "PE"):
            options_by_stock[und].append({
                "underlying": und,
                "trading_symbol": trd_sym,
                "strike": strike,
                "opt_type": opt_type,
                "lot_size": lot_size,
                "expiry": str(row["expiry"])
            })

    return fut_map, options_by_stock

def fetch_1m_df(conn, ic, trading_symbol, start_dt, end_dt):
    hist = ic.historical_data(
        exchange=conn.EXCHANGE_TYPE_NFO,
        trading_symbol=trading_symbol,
        timeframe=conn.TIMEFRAME_TYPE_MIN,
        start=start_dt,
        end=end_dt
    )
    df = pd.DataFrame(list(hist))
    if df.empty:
        return pd.DataFrame()

    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is None:
        df["datetime"] = df["datetime"].dt.tz_localize(IST)
    else:
        df["datetime"] = df["datetime"].dt.tz_convert(IST)

    df = df.set_index("datetime").sort_index()
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def get_yesterday_direction(df_fut_1m, today_date):
    """
    Determines whether the underlying stock had an UP day or DOWN day yesterday
    using completed trading sessions prior to today_date.
    """
    df_prev = df_fut_1m[df_fut_1m.index.date < today_date].between_time("09:15", "15:29")
    if df_prev.empty:
        return "FLAT", 0.0

    daily_summary = df_prev.groupby(df_prev.index.date).agg(
        open=("open", "first"),
        close=("close", "last")
    ).dropna()

    if daily_summary.empty:
        return "FLAT", 0.0

    yest_open = float(daily_summary["open"].iloc[-1])
    yest_close = float(daily_summary["close"].iloc[-1])

    if YESTERDAY_TREND_MODE.upper() == "CLOSE_VS_OPEN" or len(daily_summary) < 2:
        ref_base = yest_open
    else:
        ref_base = float(daily_summary["close"].iloc[-2])

    if ref_base <= 0:
        return "FLAT", 0.0

    pct_chg = round(((yest_close - ref_base) / ref_base) * 100.0, 2)
    if yest_close < ref_base:
        return "DOWN", pct_chg
    elif yest_close > ref_base:
        return "UP", pct_chg
    return "FLAT", 0.0

def build_basket_task(conn, ic, now_ist):
    print(
        f"\n🎯 Building Filtered OTM Option Basket for {len(high_beta_50_stocks)} High-Beta Stocks "
        f"(Down Yesterday -> OTM CE | Up Yesterday -> OTM PE | Mode: {YESTERDAY_TREND_MODE})..."
    )
    fut_map, options_by_stock = load_nfo_master_data(conn)
    start_dt = (now_ist - timedelta(days=10)).replace(hour=9, minute=15, second=0, microsecond=0)
    today_date = now_ist.date()

    basket = []
    ce_count = 0
    pe_count = 0

    for symbol in high_beta_50_stocks:
        fut_info = fut_map.get(symbol)
        opt_chain = options_by_stock.get(symbol, [])
        if not fut_info or not opt_chain:
            continue

        try:
            df_fut = fetch_1m_df(conn, ic, fut_info["trading_symbol"], start_dt, now_ist)
            if df_fut.empty:
                continue

            ref_price = float(df_fut["close"].iloc[-1])
            yest_dir, yest_pct = get_yesterday_direction(df_fut, today_date)

            if yest_dir == "DOWN":
                ce_candidates = sorted(
                    [o for o in opt_chain if o["opt_type"] == "CE" and o["strike"] > ref_price],
                    key=lambda x: x["strike"]
                )
                if ce_candidates:
                    otm_ce = dict(ce_candidates[0])
                    otm_ce["ref_fut_price"] = round(ref_price, 2)
                    otm_ce["yest_dir"] = "DOWN"
                    otm_ce["yest_pct"] = yest_pct
                    basket.append(otm_ce)
                    ce_count += 1
                    print(
                        f"  📉 {symbol} (Yest: {yest_pct:+.2f}% DOWN | Fut: {ref_price:.2f}) -> "
                        f"OTM CE: {otm_ce['trading_symbol']} (Lot: {otm_ce['lot_size']})"
                    )

            elif yest_dir == "UP":
                pe_candidates = sorted(
                    [o for o in opt_chain if o["opt_type"] == "PE" and o["strike"] < ref_price],
                    key=lambda x: x["strike"],
                    reverse=True
                )
                if pe_candidates:
                    otm_pe = dict(pe_candidates[0])
                    otm_pe["ref_fut_price"] = round(ref_price, 2)
                    otm_pe["yest_dir"] = "UP"
                    otm_pe["yest_pct"] = yest_pct
                    basket.append(otm_pe)
                    pe_count += 1
                    print(
                        f"  📈 {symbol} (Yest: {yest_pct:+.2f}% UP   | Fut: {ref_price:.2f}) -> "
                        f"OTM PE: {otm_pe['trading_symbol']} (Lot: {otm_pe['lot_size']})"
                    )

        except Exception as e:
            print(f"  ⚠️ Error selecting OTM option for {symbol}: {e}")

    print(f"🎯 Basket Ready: {len(basket)} OTM Contracts locked in ({ce_count} OTM CEs + {pe_count} OTM PEs).")
    return basket

# ==============================================================================
# 5. 15-MINUTE RESAMPLING, ATR(2) & VOLUME BREAKOUT LOGIC
# ==============================================================================
def resample_to_15m(df_1m, now_ist):
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
            (ts.date() < today_date)
            or (now_ist >= ts + timedelta(minutes=15))
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

    cond_close_gt_prev_close = float(cur_bar["close"]) > float(prev_bar["close"])
    cond_close_gt_prev_high = float(cur_bar["close"]) > float(prev_bar["high"])
    cond_atr_expansion = cur_atr > (prev_atr * ATR_MULT_THRESHOLD)
    cond_vol_expansion = cur_vol > (prev_vol * VOL_MULT_THRESHOLD)

    if not (cond_close_gt_prev_close and cond_close_gt_prev_high and cond_atr_expansion and cond_vol_expansion):
        return None

    atr_ratio = round(cur_atr / prev_atr, 2)
    vol_ratio = round(cur_vol / prev_vol, 2)
    gain_pct = round(((float(cur_bar["close"]) - float(prev_bar["close"])) / float(prev_bar["close"])) * 100.0, 2)

    return {
        "bar_time": cur_bar.name.strftime("%d-%b %H:%M"),
        "bar_iso": cur_bar.name.strftime("%Y-%m-%d %H:%M"),
        "close": round(float(cur_bar["close"]), 2),
        "prev_close": round(float(prev_bar["close"]), 2),
        "prev_high": round(float(prev_bar["high"]), 2),
        "atr2": round(cur_atr, 2),
        "prev_atr2": round(prev_atr, 2),
        "atr_ratio": atr_ratio,
        "vol": int(cur_vol),
        "prev_vol": int(prev_vol),
        "vol_ratio": vol_ratio,
        "gain_pct": gain_pct
    }

# ==============================================================================
# 6. LIVE DEFINEDGE ORDER EXECUTION (IntegrateOrders)
# ==============================================================================
def round_to_tick(price, tick=0.05):
    return round(round(price / tick) * tick, 2)

def execute_buy_orders_for_matches(triggered_list, memory):
    """
    Sends BUY orders to Definedge for the top-ranked matches on the current bar,
    respecting MAX_ORDERS_PER_BAR, MAX_ORDERS_PER_DAY, and duplicate protection.
    """
    is_live_market = dtime(9, 15) <= datetime.now(IST).time() <= dtime(15, 29, 50)
    orders_today = memory.get("orders_placed_today", [])

    # Sort all matches by highest ATR Ratio so we prioritize the strongest breakouts
    ranked_matches = sorted(triggered_list, key=lambda x: x["atr_ratio"], reverse=True)

    if not ENABLE_LIVE_ORDERS:
        for m in ranked_matches:
            m["order_info"] = "⏸️ Disabled"
        return ranked_matches

    if not is_live_market:
        for m in ranked_matches:
            m["order_info"] = "⏹️ After-Hours (No Order)"
        return ranked_matches

    try:
        conn, _ = login_definedge()
        io_client = IntegrateOrders(conn)
    except Exception as e:
        print(f"⚠️ Could not initialize IntegrateOrders: {e}")
        for m in ranked_matches:
            m["order_info"] = f"❌ Login Err"
        return ranked_matches

    orders_sent_this_bar = 0
    for m in ranked_matches:
        trd_sym = m["trading_symbol"]
        lot_size = int(m.get("lot_size", 1))
        qty = lot_size * LOTS_PER_TRADE

        if trd_sym in orders_today:
            m["order_info"] = "⏭️ Already Bought Today"
            continue

        if len(orders_today) >= MAX_ORDERS_PER_DAY:
            m["order_info"] = f"⏭️ Daily Cap ({MAX_ORDERS_PER_DAY}) Hit"
            continue

        if orders_sent_this_bar >= MAX_ORDERS_PER_BAR:
            m["order_info"] = f"⏭️ Bar Cap (Top {MAX_ORDERS_PER_BAR})"
            continue

        # Product Type: INTRADAY or NORMAL
        prod_type = (
            getattr(conn, "PRODUCT_TYPE_NORMAL", "M")
            if ORDER_PRODUCT_TYPE.upper() == "NORMAL"
            else getattr(conn, "PRODUCT_TYPE_INTRADAY", "I")
        )

        # Price Type: LIMIT (with buffer) or MARKET
        if ORDER_PRICE_TYPE.upper() == "MARKET":
            p_type = getattr(conn, "PRICE_TYPE_MARKET", "MKT")
            order_price = 0
        else:
            p_type = getattr(conn, "PRICE_TYPE_LIMIT", "LMT")
            buffered_price = float(m["close"]) * (1.0 + (LIMIT_SLIPPAGE_PCT / 100.0))
            order_price = round_to_tick(buffered_price, 0.05)

        try:
            resp = io_client.place_order(
                exchange=conn.EXCHANGE_TYPE_NFO,
                order_type=conn.ORDER_TYPE_BUY,
                price=order_price,
                price_type=p_type,
                product_type=prod_type,
                quantity=qty,
                tradingsymbol=trd_sym
            )
            order_id = resp.get("order_id") or resp.get("norenordno") or resp.get("status") or "Sent"
            status_str = str(resp.get("status", "")).upper()

            if status_str in ("OK", "SUCCESS") or resp.get("order_id"):
                orders_sent_this_bar += 1
                orders_today.append(trd_sym)
                m["order_info"] = f"✅ BUY {qty} @ {order_price or 'MKT'} (ID:{order_id})"
                print(f"  🛒 ORDER PLACED: {trd_sym} | Qty: {qty} | Price: {order_price or 'MKT'} | Resp: {resp}")
            else:
                msg = resp.get("message") or resp.get("emsg") or str(resp)
                m["order_info"] = f"⚠️ {str(msg)[:28]}"
                print(f"  ⚠️ ORDER REJECTED for {trd_sym}: {resp}")
        except Exception as e:
            m["order_info"] = f"❌ Err: {str(e)[:22]}"
            print(f"  ❌ Order placement exception for {trd_sym}: {e}")

    memory["orders_placed_today"] = orders_today
    return ranked_matches

# ==============================================================================
# 7. EMAIL ALERT SENDER WITH ORDER EXECUTION REPORT
# ==============================================================================
def send_otm_15m_email_alert(triggered_list, bar_label, total_tracked):
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            now_str = datetime.now(IST).strftime("%d-%b %H:%M:%S")
            subject = f"⚡ 15m OTM Options Breakout + Orders ({len(triggered_list)} Matches) - [{bar_label}] ({now_str})"

            ce_matches = sorted([m for m in triggered_list if m["opt_type"] == "CE"], key=lambda x: x["atr_ratio"], reverse=True)
            pe_matches = sorted([m for m in triggered_list if m["opt_type"] == "PE"], key=lambda x: x["atr_ratio"], reverse=True)

            ce_top5 = ", ".join([m["trading_symbol"] for m in ce_matches[:5]]) or "None"
            pe_top5 = ", ".join([m["trading_symbol"] for m in pe_matches[:5]]) or "None"
            ce_all = ", ".join([m["trading_symbol"] for m in ce_matches]) or "None"
            pe_all = ", ".join([m["trading_symbol"] for m in pe_matches]) or "None"

            body_lines = [
                f"15m OTM Options Reversal Alert (Down Yest -> CE | Up Yest -> PE | ATR(2) > 1.5x | Vol > 1.5x)",
                f"15m Bar: {bar_label} | Basket Size: {total_tracked} Filtered OTM Options\n",
                f"📋 OTM CALLS (CE - Stock Down Yesterday) - Top 5:\n{ce_top5}\n",
                f"📋 OTM PUTS (PE - Stock Up Yesterday) - Top 5:\n{pe_top5}\n",
                f"All CE ({len(ce_matches)}): {ce_all}",
                f"All PE ({len(pe_matches)}): {pe_all}\n"
            ]

            table_rows = [
                [
                    f"{m['underlying']} ({m.get('yest_pct', 0):+.1f}%)",
                    m["trading_symbol"],
                    f"{m['opt_type']} ({m['strike']:g})",
                    m["close"],
                    m["prev_high"],
                    f"{m['atr_ratio']}x",
                    f"{m['vol_ratio']}x ({m['vol']:,})",
                    f"+{m['gain_pct']}%",
                    m.get("order_info", "N/A")
                ]
                for m in (ce_matches + pe_matches)
            ]
            headers = [
                "Stock (Yest %)", "Option Symbol", "Type",
                "Close", "-1 High", "ATR Ratio", "Vol Ratio (Vol)", "Bar %", "Definedge Order Status"
            ]
            body_lines.append(tabulate(table_rows, headers=headers, tablefmt="grid"))

            msg = MIMEMultipart()
            msg["From"] = SENDER_EMAIL
            msg["To"] = RECEIVER_EMAIL
            msg["Subject"] = subject
            msg.attach(MIMEText("\n".join(body_lines), "plain"))

            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
                server.login(SENDER_EMAIL, APP_PASSWORD)
                server.send_message(msg)

            print(f"✅ 15m OTM Options Alert Email sent successfully for {len(triggered_list)} contract(s)!")
            return
        except Exception as e:
            print(f"⚠️ Email dispatch attempt {attempt}/{max_retries} failed: {e}")
            if attempt == max_retries:
                print("❌ All email retry attempts failed for this batch!")
            else:
                time.sleep(3)

# ==============================================================================
# 8. MAIN RUNNER WITH CONNECTION RETRY WRAPPER
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

def scan_task(conn, ic, options_list, expected_bar_label, now_ist, force_scan):
    print(f"\n🔎 Scanning {len(options_list)} Filtered OTM Options on 15m timeframe (Target Bar: {expected_bar_label or 'LATEST'})...")
    start_dt_15m = (now_ist - timedelta(days=LOOKBACK_DAYS_15M)).replace(hour=9, minute=15, second=0, microsecond=0)

    triggered_alerts = []
    latest_bar_seen = expected_bar_label or ""

    for opt in options_list:
        trd_sym = opt["trading_symbol"]
        try:
            df_1m = fetch_1m_df(conn, ic, trd_sym, start_dt_15m, now_ist)
            if df_1m.empty:
                continue

            df_15m = resample_to_15m(df_1m, now_ist)
            if df_15m.empty:
                continue

            res = evaluate_15m_option_breakout(df_15m)
            latest_bar_seen = df_15m.index[-1].strftime("%Y-%m-%d %H:%M")

            if res:
                if expected_bar_label and res["bar_iso"] != expected_bar_label and not force_scan:
                    continue

                alert_item = {**opt, **res}
                triggered_alerts.append(alert_item)
                print(
                    f"  🔥 MATCH: {trd_sym} ({opt['opt_type']} | Lot: {opt.get('lot_size', 1)} | Yest: {opt.get('yest_pct', 0):+.1f}%) on {res['bar_time']} | "
                    f"Close: {res['close']} > -1 High: {res['prev_high']} | "
                    f"ATR(2): {res['atr2']} vs {res['prev_atr2']} ({res['atr_ratio']}x) | "
                    f"Vol: {res['vol']:,} vs {res['prev_vol']:,} ({res['vol_ratio']}x)"
                )
        except Exception as e:
            print(f"  ⚠️ Error scanning {trd_sym}: {e}")

    return triggered_alerts, latest_bar_seen

def run_scanner(force_scan=False):
    now_ist = datetime.now(IST)
    today_str = now_ist.strftime("%Y-%m-%d")
    is_live_hours = dtime(9, 15) <= now_ist.time() <= dtime(15, 35)

    memory = load_memory()

    # Reset daily order tracker on a new trading day
    if memory.get("basket_date") != today_str:
        memory["orders_placed_today"] = []

    need_new_basket = (
        memory.get("basket_date") != today_str
        or memory.get("basket_version") != BASKET_VERSION
        or not memory.get("options")
        or (is_live_hours and not memory.get("basket_is_live", False))
    )

    if need_new_basket:
        basket = with_connection_retry(build_basket_task, now_ist)
        if basket:
            memory["basket_date"] = today_str
            memory["basket_is_live"] = is_live_hours
            memory["basket_version"] = BASKET_VERSION
            memory["options"] = basket
            save_memory(memory)

    options_list = memory.get("options", [])
    if not options_list:
        print("⚠️ No OTM Option contracts in basket. Saving state & exiting.")
        save_memory(memory)
        return

    expected_bar_label = get_expected_15m_bar_label(now_ist)
    if not force_scan and expected_bar_label is not None:
        if memory.get("last_scanned_15m_bar") == expected_bar_label:
            print(f"ℹ️ 15m bar [{expected_bar_label}] already scanned. Skipping duplicate check.")
            return

    triggered_alerts, latest_bar_seen = with_connection_retry(
        scan_task, options_list, expected_bar_label, now_ist, force_scan
    )

    if expected_bar_label:
        memory["last_scanned_15m_bar"] = expected_bar_label
    elif latest_bar_seen:
        memory["last_scanned_15m_bar"] = latest_bar_seen

    if triggered_alerts:
        # 1. Place live BUY orders on Definedge simultaneously & attach order status
        triggered_alerts = execute_buy_orders_for_matches(triggered_alerts, memory)
        # 2. Save updated memory (including orders_placed_today)
        save_memory(memory)
        # 3. Send Email Alert with Order Status column
        send_otm_15m_email_alert(triggered_alerts, latest_bar_seen, len(options_list))
    else:
        save_memory(memory)
        print(f"ℹ️ No 15m OTM Options matched the ATR(2) + Vol breakout criteria on bar [{latest_bar_seen}].")

# ==============================================================================
# 9. FULL-DAY CHAINED MARKET CLOCK WITH HEARTBEAT SLEEP
# ==============================================================================
SCAN_CHECKPOINTS_IST = [
    dtime(9, 30, 12), dtime(9, 45, 12),
    dtime(10, 0, 12), dtime(10, 15, 12), dtime(10, 30, 12), dtime(10, 45, 12),
    dtime(11, 0, 12), dtime(11, 15, 12), dtime(11, 30, 12), dtime(11, 45, 12),
    dtime(12, 0, 12), dtime(12, 15, 12), dtime(12, 30, 12), dtime(12, 45, 12),
    dtime(13, 0, 12), dtime(13, 15, 12), dtime(13, 30, 12), dtime(13, 45, 12),
    dtime(14, 0, 12), dtime(14, 15, 12), dtime(14, 30, 12), dtime(14, 45, 12),
    dtime(15, 0, 12), dtime(15, 15, 12), dtime(15, 30, 12),
]

def heartbeat_sleep(target_dt):
    """Sleeps until target_dt in max 5-minute chunks so GitHub Actions never drops an idle runner."""
    while True:
        now_ist = datetime.now(IST)
        rem = (target_dt - now_ist).total_seconds()
        if rem <= 0:
            break
        chunk = min(rem, 300.0)
        time.sleep(max(1.0, chunk))
        now_ist = datetime.now(IST)
        if (target_dt - now_ist).total_seconds() > 30:
            print(f"💓 Heartbeat at {now_ist.strftime('%H:%M:%S IST')} — waiting for {target_dt.strftime('%H:%M:%S IST')}...")

def stay_awake_market_clock():
    event_name = os.environ.get("GITHUB_EVENT_NAME", "workflow_dispatch")
    session_mode = os.environ.get("SESSION_MODE", "MORNING")
    now_ist = datetime.now(IST)

    if now_ist.time() < dtime(8, 0) or now_ist.time() > dtime(15, 33):
        if session_mode == "MORNING":
            print("🚀 Outside live market hours. Executing single test/basket pass...")
            run_scanner(force_scan=(event_name == "workflow_dispatch"))
        else:
            print("⏹️ Outside market hours (Afternoon job skipped).")
        return

    if session_mode == "MORNING":
        if now_ist.time() >= dtime(11, 27):
            print("⏩ Already past 11:27 AM IST. Handing over immediately to Afternoon Session job...")
            return
        session_cutoff = dtime(11, 27)
    else:
        session_cutoff = dtime(15, 34)

    print(f"⏰ [{session_mode}] 15m OTM Options Clock active (Started {now_ist.strftime('%H:%M:%S IST')}, Cutoff: {session_cutoff.strftime('%H:%M IST')}).")

    if dtime(9, 16) <= now_ist.time() < dtime(9, 30, 12):
        print(f"🌅 Pre-warming today's Filtered OTM Basket at {now_ist.strftime('%H:%M:%S IST')}...")
        try:
            run_scanner(force_scan=False)
        except Exception as e:
            print(f"⚠️ Pre-warm warning: {e}")

    elif dtime(9, 30, 12) <= now_ist.time() <= dtime(15, 33):
        print(f"🔔 Initial catch-up scan at {now_ist.strftime('%H:%M:%S IST')}...")
        try:
            run_scanner(force_scan=False)
        except Exception as e:
            print(f"⚠️ Error in initial scan: {e}")

    while True:
        now_ist = datetime.now(IST)
        if now_ist.weekday() >= 5 or now_ist.time() >= session_cutoff:
            print(f"⏹️ Reached {session_mode} cutoff ({now_ist.strftime('%H:%M:%S IST')}). Exiting cleanly.")
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

