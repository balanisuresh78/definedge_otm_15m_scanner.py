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
from integrate import ConnectToIntegrate, IntegrateData

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
BASKET_VERSION = "v10_larger_cell_font"

# 15m Option ATR(2) & Volume Expansion Parameters
ATR_PERIOD = 2
ATR_MULT_THRESHOLD = 1.5
VOL_MULT_THRESHOLD = 1.5
ATR_METHOD = "SMA"  # "SMA" or "RMA"

# Yesterday Trend Filter Mode for Morning Basket Harvesting:
# "CLOSE_VS_PREV_CLOSE" -> Down Day = Yesterday Close < Day-Before Close | Up Day = Yesterday Close > Day-Before Close
YESTERDAY_TREND_MODE = "CLOSE_VS_PREV_CLOSE"

# Underlying Security Respective 15m Slot Volume Ratio Filter:
# Formula: (Today's Slot Volume) / (100-Day Avg Volume of that exact 15m slot)
UNDERLYING_VOL_FILTER_OP = "NONE"
UNDERLYING_VOL_RATIO_THRESHOLD = 1.0
LOOKBACK_DAYS_100D_SLOT = 160  # ~160 calendar days covers 100+ trading days

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
# 2. PERSISTENT MEMORY MANAGEMENT
# ==============================================================================
def default_memory():
    return {
        "basket_date": "",
        "basket_is_live": False,
        "basket_version": "",
        "last_scanned_15m_bar": "",
        "und_slot_100d_cache": {},
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
            if "und_slot_100d_cache" not in data:
                data["und_slot_100d_cache"] = {}
            return data
    except Exception as e:
        print(f"⚠️ Could not read {MEMORY_FILE}, starting fresh: {e}")
        return default_memory()

def push_memory_to_git():
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
# 4. MASTER SYMBOL PARSING & FILTERED BASKET BUILDER
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

def load_master_data(conn):
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

    nse_eq = df[
        (df[0] == "NSE")
        & (df[2].str.strip().isin(stock_set))
        & (df[4].str.strip() == "EQ")
    ].copy()
    eq_map = {row[2].strip(): row[3].strip() for _, row in nse_eq.iterrows()}

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
        if strike is not None and opt_type in ("CE", "PE"):
            options_by_stock[und].append({
                "underlying": und,
                "trading_symbol": trd_sym,
                "strike": strike,
                "opt_type": opt_type,
                "expiry": str(row["expiry"])
            })

    return eq_map, fut_map, options_by_stock

def fetch_1m_df(conn, ic, trading_symbol, start_dt, end_dt, exchange_type=None):
    exch = exchange_type if exchange_type is not None else conn.EXCHANGE_TYPE_NFO
    hist = ic.historical_data(
        exchange=exch,
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

def get_yesterday_direction(df_1m, today_date):
    df_prev = df_1m[df_1m.index.date < today_date].between_time("09:15", "15:29")
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
        f"(Down Yesterday -> OTM CE | Up Yesterday -> OTM PE)..."
    )
    eq_map, fut_map, options_by_stock = load_master_data(conn)
    start_dt_fut = (now_ist - timedelta(days=10)).replace(hour=9, minute=15, second=0, microsecond=0)
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
            df_fut = fetch_1m_df(conn, ic, fut_info["trading_symbol"], start_dt_fut, now_ist, conn.EXCHANGE_TYPE_NFO)
            if df_fut.empty:
                continue

            ref_price = float(df_fut["close"].iloc[-1])
            yest_dir, yest_pct = get_yesterday_direction(df_fut, today_date)
            if yest_dir not in ("DOWN", "UP"):
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
                selected_opt["eq_symbol"] = eq_map.get(symbol, "")
                selected_opt["fut_symbol"] = fut_info["trading_symbol"]
                basket.append(selected_opt)
        except Exception as e:
            print(f"  ⚠️ Error selecting OTM option for {symbol}: {e}")

    print(f"🎯 Basket Ready: {len(basket)} OTM Contracts locked in ({ce_count} OTM CEs + {pe_count} OTM PEs).")
    return basket

# ==============================================================================
# 5. 15-MINUTE RESAMPLING, ATR(2), OPT VOL & INVERTED UNDERLYING SLOT VOL
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

    return {
        "bar_ts": cur_bar.name,
        "bar_time": cur_bar.name.strftime("%d-%b %H:%M"),
        "bar_iso": cur_bar.name.strftime("%Y-%m-%d %H:%M"),
        "slot_hm": cur_bar.name.strftime("%H:%M"),
        "close": round(float(cur_bar["close"]), 2),
        "atr_ratio": atr_ratio,
        "vol": int(cur_vol),
        "vol_ratio": vol_ratio
    }

def get_underlying_respective_slot_vol_ratio(conn, ic, opt, bar_ts, now_ist, slot_cache):
    symbol = opt["underlying"]
    eq_sym = opt.get("eq_symbol", "")
    fut_sym = opt.get("fut_symbol", "")
    slot_hm = bar_ts.strftime("%H:%M")
    target_date = bar_ts.date()

    if symbol not in slot_cache or not slot_cache.get(symbol):
        start_dt_100d = (now_ist - timedelta(days=160)).replace(hour=9, minute=15, second=0, microsecond=0)
        if eq_sym:
            df_hist_1m = fetch_1m_df(conn, ic, eq_sym, start_dt_100d, now_ist, conn.EXCHANGE_TYPE_NSE)
        else:
            df_hist_1m = fetch_1m_df(conn, ic, fut_sym, start_dt_100d, now_ist, conn.EXCHANGE_TYPE_NFO)

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
    if eq_sym:
        df_today_1m = fetch_1m_df(conn, ic, eq_sym, start_today, now_ist, conn.EXCHANGE_TYPE_NSE)
    else:
        df_today_1m = fetch_1m_df(conn, ic, fut_sym, start_today, now_ist, conn.EXCHANGE_TYPE_NFO)

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
        ratio = None

    return ratio, avg_100d_slot_vol, today_slot_vol

# ==============================================================================
# 6. PERFECTLY ALIGNED HTML EMAIL SENDER (LARGER FONT FOR HEADERS & CELLS)
# ==============================================================================
def send_otm_15m_email_alert(triggered_list, bar_label, total_tracked):
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            now_str = datetime.now(IST).strftime("%d-%b %H:%M:%S")
            subject = f"⚡ 15m OTM Options Breakout ({len(triggered_list)} Matches) - [{bar_label}] ({now_str})"

            ce_matches = sorted([m for m in triggered_list if m["opt_type"] == "CE"], key=lambda x: x["atr_ratio"], reverse=True)
            pe_matches = sorted([m for m in triggered_list if m["opt_type"] == "PE"], key=lambda x: x["atr_ratio"], reverse=True)
            ordered_matches = ce_matches + pe_matches

            ce_top5 = ", ".join([m["trading_symbol"] for m in ce_matches[:5]]) or "None"
            pe_top5 = ", ".join([m["trading_symbol"] for m in pe_matches[:5]]) or "None"
            ce_all = ", ".join([m["trading_symbol"] for m in ce_matches]) or "None"
            pe_all = ", ".join([m["trading_symbol"] for m in pe_matches]) or "None"

            headers = [
                "Stock (Yest %)",
                "Option Symbol",
                "15m Slot",
                "Close",
                "ATR Ratio",
                "Opt Vol Ratio",
                "Und Slot Vol Ratio (Today/100dAvg)"
            ]

            table_rows = [
                [
                    f"{m['underlying']} ({m.get('yest_pct', 0):+.1f}%)",
                    m["trading_symbol"],
                    m["slot_hm"],
                    f"{m['close']:.2f}",
                    f"{m['atr_ratio']:.2f}x",
                    f"{m['vol_ratio']:.2f}x ({m['vol']:,})",
                    f"{m.get('und_vol_ratio', 0):.2f}x ({m.get('und_today_slot_vol', 0):,}/{m.get('und_avg100_slot_vol', 0):,})"
                ]
                for m in ordered_matches
            ]

            # 1. Plain-text fallback
            plain_lines = [
                "15m OTM Options Reversal Alert (Down Yest -> CE | Up Yest -> PE | ATR(2) > 1.5x | Opt Vol > 1.5x)",
                f"15m Bar: {bar_label} | Basket Size: {total_tracked} Filtered OTM Options\n",
                f"OTM CALLS (CE) - Top 5:\n{ce_top5}\n",
                f"OTM PUTS (PE) - Top 5:\n{pe_top5}\n",
                f"All CE ({len(ce_matches)}): {ce_all}",
                f"All PE ({len(pe_matches)}): {pe_all}\n",
                tabulate(table_rows, headers=headers, tablefmt="simple")
            ]
            plain_body = "\n".join(plain_lines)

            # 2. Clean HTML Table (15px font size for both <th> and <td> cells)
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
                <p style="margin:0 0 8px 0;font-size:16px;"><strong>⚡ 15m OTM Options Reversal Breakout Alert</strong></p>
                <p style="margin:0 0 14px 0; color:#475569;font-size:15px;">
                    <strong>15m Bar:</strong> {bar_label} &nbsp;|&nbsp;
                    <strong>Basket Size:</strong> {total_tracked} Filtered OTM Options
                </p>
                <p style="margin:0 0 4px 0;"><strong>📋 OTM CALLS (CE - Stock Down Yesterday) - Top 5:</strong></p>
                <pre style="background:#f1f5f9;padding:10px;border-radius:4px;margin:0 0 12px 0;white-space:pre-wrap;font-size:14px;">{ce_top5}</pre>
                <p style="margin:0 0 4px 0;"><strong>📋 OTM PUTS (PE - Stock Up Yesterday) - Top 5:</strong></p>
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

            print(f"✅ 15m OTM Options Alert Email sent successfully for {len(triggered_list)} contract(s)!")
            return
        except Exception as e:
            print(f"⚠️ Email dispatch attempt {attempt}/{max_retries} failed: {e}")
            if attempt == max_retries:
                print("❌ All email retry attempts failed for this batch!")
            else:
                time.sleep(3)

# ==============================================================================
# 7. MAIN RUNNER WITH CONNECTION RETRY WRAPPER
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

def scan_task(conn, ic, options_list, expected_bar_label, now_ist, force_scan, slot_cache):
    print(f"\n🔎 Scanning {len(options_list)} Filtered OTM Options on 15m timeframe (Target Bar: {expected_bar_label or 'LATEST'})...")
    start_dt_15m = (now_ist - timedelta(days=7)).replace(hour=9, minute=15, second=0, microsecond=0)

    triggered_alerts = []
    latest_bar_seen = expected_bar_label or ""

    for opt in options_list:
        trd_sym = opt["trading_symbol"]
        try:
            df_1m = fetch_1m_df(conn, ic, trd_sym, start_dt_15m, now_ist, conn.EXCHANGE_TYPE_NFO)
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

                bar_ts = res.pop("bar_ts")
                und_ratio, avg100_slot, today_slot = get_underlying_respective_slot_vol_ratio(
                    conn, ic, opt, bar_ts, now_ist, slot_cache
                )

                alert_item = {
                    **opt,
                    **res,
                    "und_vol_ratio": und_ratio if und_ratio is not None else 0.0,
                    "und_avg100_slot_vol": avg100_slot,
                    "und_today_slot_vol": today_slot
                }
                triggered_alerts.append(alert_item)
                print(
                    f"  🔥 MATCH: {trd_sym} ({opt['opt_type']} | Slot {res['slot_hm']} | "
                    f"Und Slot Vol Ratio [Today/100dAvg]: {alert_item['und_vol_ratio']:.2f}x) | "
                    f"Close: {res['close']} > -1 High: {res['prev_high']} | "
                    f"ATR(2): {res['atr2']} vs {res['prev_atr2']} ({res['atr_ratio']}x) | "
                    f"Opt Vol: {res['vol']:,} vs {res['prev_vol']:,} ({res['vol_ratio']}x)"
                )
        except Exception as e:
            print(f"  ⚠️ Error scanning {trd_sym}: {e}")

    return triggered_alerts, latest_bar_seen

def run_scanner(force_scan=False):
    now_ist = datetime.now(IST)
    today_str = now_ist.strftime("%Y-%m-%d")
    is_live_hours = dtime(9, 15) <= now_ist.time() <= dtime(15, 35)

    memory = load_memory()

    if memory.get("basket_date") != today_str:
        memory["und_slot_100d_cache"] = {}

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

    slot_cache = memory.get("und_slot_100d_cache", {})
    triggered_alerts, latest_bar_seen = with_connection_retry(
        scan_task, options_list, expected_bar_label, now_ist, force_scan, slot_cache
    )
    memory["und_slot_100d_cache"] = slot_cache

    if expected_bar_label:
        memory["last_scanned_15m_bar"] = expected_bar_label
    elif latest_bar_seen:
        memory["last_scanned_15m_bar"] = latest_bar_seen

    save_memory(memory)

    if triggered_alerts:
        send_otm_15m_email_alert(triggered_alerts, latest_bar_seen, len(options_list))
    else:
        print(f"ℹ️ No 15m OTM Options matched the ATR(2) + Vol breakout criteria on bar [{latest_bar_seen}].")

# ==============================================================================
# 8. FULL-DAY CHAINED MARKET CLOCK WITH HEARTBEAT SLEEP
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
