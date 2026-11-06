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
# 1. CREDENTIALS & CONFIGURATION
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
MEMORY_FILE = "volume_alert_memory.json"

VELOCITY_MULT_THRESHOLD = 2.0  # Fires if current speed is >= 2.0x normal session pace
LOOKBACK_DAYS_HIST = 160

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
# 2. JSON MEMORY PERSISTENCE (DAILY DEDUPLICATION)
# ==============================================================================
def load_memory():
    default_mem = {
        "date": "",
        "sma_alerted": [],
        "vel_alerted": []
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
# 3. DEFINEDGE AUTHENTICATION & DATA ENGINE
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
            return df.dropna(subset=["open", "close"]).drop_duplicates(subset=["date"], keep="last")
        except Exception:
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
        except Exception:
            time.sleep(2)
    return pd.DataFrame()

def load_definedge_master_tokens():
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
    fut_df = nfo[nfo[4].str.strip() == "FUTSTK"].sort_values(5).groupby(2, as_index=False).first()
    fut_map = {row[2].strip(): row[1].strip() for _, row in fut_df.iterrows()}

    return eq_token_map, fut_map

# ==============================================================================
# 4. DUAL-CATEGORY EMAIL DISPATCHER
# ==============================================================================
def send_dual_category_alert(sma_alerts, vel_alerts):
    now_str = datetime.now(IST).strftime("%d-%b %H:%M:%S")
    subject = f"⚡ Volume & Velocity Breakouts | SMA: {len(sma_alerts)} | Velocity: {len(vel_alerts)} - [{now_str}]"

    # Plain Text Summary
    print(f"\n📊 {subject}")
    if sma_alerts:
        print("--- CATEGORY 1: TRIPLE VOLUME SMA BREAKOUTS ---")
        for a in sma_alerts:
            print(f"{a['symbol']} | Vol: {a['current_vol']:,} > Max SMA ({a['max_hurdle']:,})")
    if vel_alerts:
        print("--- CATEGORY 2: VELOCITY MULTIPLIER BREAKOUTS ---")
        for v in vel_alerts:
            print(f"{v['symbol']} | Velocity Multiplier: {v['velocity_mult']}x")

    if not SENDER_EMAIL or not RECEIVER_EMAIL or not APP_PASSWORD:
        print("⚠️ Email credentials missing. Skipping dispatch.")
        return

    # Build HTML Email Body
    sma_table_html = ""
    if sma_alerts:
        headers_1 = ["Stock", "Current Vol", "10d SMA", "50d SMA", "100d SMA", "Time"]
        rows_1 = [[a["symbol"], f"{a['current_vol']:,}", f"{a['sma_10']:,}", f"{a['sma_50']:,}", f"{a['sma_100']:,}", a["timestamp"]] for a in sma_alerts]
        th1 = "".join(f'<th style="background-color:#1e293b;color:#ffffff;padding:8px 12px;border:1px solid #cbd5e1;font-size:14px;">{h}</th>' for h in headers_1)
        tr1 = "".join(f'<tr style="background-color:{"#f8fafc" if i%2 else "#ffffff"};">' + "".join(f'<td style="padding:8px 12px;border:1px solid #cbd5e1;font-size:14px;">{c}</td>' for c in r) + '</tr>' for i, r in enumerate(rows_1))
        sma_table_html = f'<table style="border-collapse:collapse;margin-bottom:20px;"><thead><tr>{th1}</tr></thead><tbody>{tr1}</tbody></table>'

    vel_table_html = ""
    if vel_alerts:
        headers_2 = ["Stock", "Velocity Multiplier", "Current Vol", "Expected Normal Vol", "Time"]
        rows_2 = [[v["symbol"], f"{v['velocity_mult']}x", f"{v['current_vol']:,}", f"{v['expected_vol']:,}", v["timestamp"]] for v in vel_alerts]
        th2 = "".join(f'<th style="background-color:#0f172a;color:#ffffff;padding:8px 12px;border:1px solid #cbd5e1;font-size:14px;">{h}</th>' for h in headers_2)
        tr2 = "".join(f'<tr style="background-color:{"#f8fafc" if i%2 else "#ffffff"};">' + "".join(f'<td style="padding:8px 12px;border:1px solid #cbd5e1;font-size:14px;">{c}</td>' for c in r) + '</tr>' for i, r in enumerate(rows_2))
        vel_table_html = f'<table style="border-collapse:collapse;"><thead><tr>{th2}</tr></thead><tbody>{tr2}</tbody></table>'

    html_body = f"""
    <html>
    <body style="font-family:sans-serif;font-size:15px;color:#0f172a;">
        <h3 style="color:#0f172a;margin-bottom:5px;">⚡ High-Beta Intraday Volume & Velocity Alert</h3>
        <p style="color:#475569;margin-top:0;">Timestamp: {now_str}</p>
        
        <h4 style="color:#0284c7;margin-bottom:8px;">📌 Category 1: Triple Volume SMA Breakouts ({len(sma_alerts)})</h4>
        {sma_table_html if sma_alerts else "<p>No new SMA breakouts in this cycle.</p>"}

        <h4 style="color:#059669;margin-bottom:8px;margin-top:20px;">🚀 Category 2: Velocity Multiplier Breakouts ({len(vel_alerts)})</h4>
        {vel_table_html if vel_alerts else "<p>No new velocity spikes in this cycle.</p>"}
    </body>
    </html>
    """

    for attempt in range(1, 4):
        try:
            msg = MIMEMultipart("alternative")
            msg["From"] = SENDER_EMAIL
            msg["To"] = RECEIVER_EMAIL
            msg["Subject"] = subject
            msg.attach(MIMEText("Please view HTML email for details.", "plain"))
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
                server.login(SENDER_EMAIL, APP_PASSWORD)
                server.send_message(msg)
            print(f"✅ Email alert sent successfully to {RECEIVER_EMAIL}!")
            return
        except Exception as e:
            print(f"⚠️ Email attempt {attempt}/3 failed: {e}")
            time.sleep(3)

# ==============================================================================
# 5. CORE CONTINUOUS SCANNER ENGINE
# ==============================================================================
def run_scanner_cycle():
    now_ist = datetime.now(IST)
    today_str = now_ist.strftime("%Y-%m-%d")

    memory = load_memory()
    if memory.get("date") != today_str:
        memory = {"date": today_str, "sma_alerted": [], "vel_alerted": []}

    sma_already_alerted = set(memory.get("sma_alerted", []))
    vel_already_alerted = set(memory.get("vel_alerted", []))

    print(f"\n🔎 Running Continuous Scan Cycle at {now_ist.strftime('%H:%M:%S IST')}...")
    get_definedge_session_key()
    eq_token_map, fut_map = load_definedge_master_tokens()

    start_dt_hist = (now_ist - timedelta(days=LOOKBACK_DAYS_HIST)).replace(hour=9, minute=15, second=0, microsecond=0)
    start_today = IST.localize(datetime.combine(now_ist.date(), dtime(9, 15)))
    elapsed_seconds = (now_ist - start_today).total_seconds()
    if elapsed_seconds <= 0:
        return

    triggered_sma_list = []
    triggered_vel_list = []

    for symbol in high_beta_50_stocks:
        eq_token = eq_token_map.get(symbol, "")
        fut_token = fut_map.get(symbol, "")
        token_to_use = eq_token if eq_token else fut_token
        segment_to_use = "NSE" if eq_token else "NFO"

        if not token_to_use:
            continue

        try:
            # 1. Fetch Daily History for 10, 50, 100-day Volume SMAs
            df_day = fetch_definedge_day_df(segment_to_use, token_to_use, start_dt_hist, now_ist)
            if df_day.empty or len(df_day) < 100:
                continue

            hist_vol = df_day["volume"].iloc[:-1]
            sma_10 = float(hist_vol.tail(10).mean())
            sma_50 = float(hist_vol.tail(50).mean())
            sma_100 = float(hist_vol.tail(100).mean())
            max_sma_hurdle = max(sma_10, sma_50, sma_100)
            avg_daily_vol = float(hist_vol.tail(50).mean())

            # 2. Fetch Today's Intraday 1-Minute Data
            df_1m = fetch_definedge_1m_df(segment_to_use, token_to_use, start_today, now_ist)
            if df_1m.empty:
                continue

            today_df = df_1m.between_time("09:15", "15:30")
            if today_df.empty:
                continue

            current_cum_vol = int(today_df["volume"].sum())

            # --- CATEGORY 1: TRIPLE VOLUME SMA CHECK ---
            if symbol not in sma_already_alerted and current_cum_vol > max_sma_hurdle:
                triggered_sma_list.append({
                    "symbol": symbol,
                    "current_vol": current_cum_vol,
                    "sma_10": int(sma_10),
                    "sma_50": int(sma_50),
                    "sma_100": int(sma_100),
                    "max_hurdle": int(max_sma_hurdle),
                    "timestamp": now_ist.strftime("%H:%M:%S")
                })
                sma_already_alerted.add(symbol)

            # --- CATEGORY 2: VELOCITY MULTIPLIER CHECK ---
            # Total session seconds = 22,500 (6.25 hours). Expected volume at this exact second:
            expected_vol_now = (avg_daily_vol / 22500.0) * elapsed_seconds
            if expected_vol_now > 0:
                velocity_mult = round(current_cum_vol / expected_vol_now, 2)
                if symbol not in vel_already_alerted and velocity_mult >= VELOCITY_MULT_THRESHOLD:
                    triggered_vel_list.append({
                        "symbol": symbol,
                        "velocity_mult": velocity_mult,
                        "current_vol": current_cum_vol,
                        "expected_vol": int(expected_vol_now),
                        "timestamp": now_ist.strftime("%H:%M:%S")
                    })
                    vel_already_alerted.add(symbol)

        except Exception as e:
            print(f"⚠️ Error scanning {symbol}: {e}")

    # Save state memory
    memory["sma_alerted"] = list(sma_already_alerted)
    memory["vel_alerted"] = list(vel_already_alerted)
    save_memory(memory)

    # Dispatch combined email if either category triggered
    if triggered_sma_list or triggered_vel_list:
        send_dual_category_alert(triggered_sma_list, triggered_vel_list)
    else:
        print(f"ℹ️ No new breakouts in this cycle ({now_ist.strftime('%H:%M:%S IST')}).")

# ==============================================================================
# 6. ALL-DAY CONTINUOUS MARKET CLOCK
# ==============================================================================
def start_market_clock():
    now_ist = datetime.now(IST)
    print(f"🕒 Starting Continuous Volume & Velocity Scanner Clock at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}...")

    is_weekday = now_ist.weekday() < 5
    in_market_window = is_weekday and (dtime(9, 10) <= now_ist.time() <= dtime(15, 35))

    if not in_market_window:
        print("⚡ Running immediate single-pass test scan (outside live market hours)...")
        run_scanner_cycle()
        return

    session_cutoff = dtime(15, 32)
    poll_interval_seconds = 60  # Check every 60 seconds

    try:
        get_definedge_session_key()
    except Exception as e:
        print(f"⚠️ Pre-warm login error: {e}")

    while True:
        now_ist = datetime.now(IST)
        if now_ist.time() > session_cutoff:
            print(f"⏹️ Reached session cutoff ({session_cutoff}). Exiting cleanly for today.")
            break

        if dtime(9, 15) <= now_ist.time() <= dtime(15, 30):
            try:
                run_scanner_cycle()
            except Exception as e:
                print(f"⚠️ Error in scanner cycle: {e}")

        time.sleep(poll_interval_seconds)

if __name__ == "__main__":
    start_market_clock()
