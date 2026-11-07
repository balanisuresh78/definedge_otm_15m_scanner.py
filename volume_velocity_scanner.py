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

VELOCITY_MULT_THRESHOLD = 2.0      # Category 2 trigger: Running TPS >= 2.0x Historical TPS
MIN_ELAPSED_SECONDS_FOR_VEL = 300  # Wait 5 mins after 09:15 open (09:20 IST)
POLL_INTERVAL_SECONDS = 60         # Continuous scan frequency during live market hours
LOOKBACK_DAYS_HIST = 175           # Calendar days to guarantee >= 100 trading days

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
DAILY_BASELINE_CACHE = {}

def log(msg):
    print(msg, flush=True)

# ==============================================================================
# 2. JSON MEMORY PERSISTENCE
# ==============================================================================
def load_memory(session_date_str):
    default_mem = {
        "date": session_date_str,
        "sma_alerted": [],
        "vel_alerted": []
    }
    if os.path.exists(MEMORY_FILE):
        try:
            with open(MEMORY_FILE, "r") as f:
                data = json.load(f)
                if data.get("date") == session_date_str:
                    default_mem.update(data)
        except Exception as e:
            log(f"⚠️ Memory read warning: {e}")
    return default_mem

def save_memory(mem):
    try:
        with open(MEMORY_FILE, "w") as f:
            json.dump(mem, f, indent=2)
    except Exception as e:
        log(f"⚠️ Memory write warning: {e}")

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
            r1 = requests.get(url_step1, headers={"api_secret": DEFINEDGE_API_SECRET}, timeout=12)
            r1.raise_for_status()
            otp_token = r1.json().get("otp_token")

            totp_code = pyotp.TOTP(DEFINEDGE_TOTP_SECRET).now()
            url_step2 = "https://signin.definedgesecurities.com/auth/realms/debroking/dsbpkc/token"
            r2 = requests.post(url_step2, json={"otp_token": otp_token, "otp": totp_code}, timeout=12)
            r2.raise_for_status()
            session_key = r2.json().get("api_session_key")

            if session_key:
                SESSION_CACHE["key"] = session_key
                SESSION_CACHE["date"] = today_str
                log("🔑 Definedge session authenticated successfully.")
                return session_key
        except Exception as e:
            log(f"⚠️ Definedge login attempt {attempt}/3 failed: {e}")
            time.sleep(3)
    raise RuntimeError("Could not authenticate with Definedge API. Check GitHub Secrets.")

def fetch_definedge_day_df(segment, token, start_dt, end_dt):
    from_str = start_dt.strftime("%d%m%Y%H%M")
    to_str = end_dt.strftime("%d%m%Y%H%M")
    url = f"https://data.definedgesecurities.com/sds/history/{segment}/{token}/day/{from_str}/{to_str}"

    for attempt in range(1, 3):
        session_key = get_definedge_session_key(force_refresh=(attempt > 1))
        try:
            resp = requests.get(url, headers={"Authorization": session_key}, timeout=12)
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
            time.sleep(1)
    return pd.DataFrame()

def fetch_definedge_1m_df(segment, token, start_dt, end_dt):
    from_str = start_dt.strftime("%d%m%Y%H%M")
    to_str = end_dt.strftime("%d%m%Y%H%M")
    url = f"https://data.definedgesecurities.com/sds/history/{segment}/{token}/minute/{from_str}/{to_str}"

    for attempt in range(1, 3):
        session_key = get_definedge_session_key(force_refresh=(attempt > 1))
        try:
            resp = requests.get(url, headers={"Authorization": session_key}, timeout=12)
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
            time.sleep(1)
    return pd.DataFrame()

def load_definedge_master_tokens():
    log("📦 Downloading Definedge Master Token File (Once per session)...")
    stock_set = set(high_beta_50_stocks)
    url = "https://app.definedgesecurities.com/public/allmaster.zip"
    resp = requests.get(url, timeout=25)
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

    log(f"✅ Loaded tokens for {len(eq_token_map)} NSE Cash & {len(fut_map)} NFO Futures stocks.")
    return eq_token_map, fut_map

# ==============================================================================
# 4. PRE-WARM 10d, 50d, 100d VOLUME AVERAGES + TRUE YESTERDAY CLOSE
# ==============================================================================
def build_daily_volume_baseline_cache(eq_token_map, fut_map, now_ist):
    log("🌅 Pre-calculating 10, 50 & 100-Day Volume SMAs + Exact Previous Day Close...")
    start_dt_hist = (now_ist - timedelta(days=LOOKBACK_DAYS_HIST)).replace(hour=9, minute=15, second=0, microsecond=0)
    recent_1m_start = (now_ist - timedelta(days=10)).replace(hour=9, minute=15, second=0, microsecond=0)
    today_date = now_ist.date()

    sample_token = eq_token_map.get("SBIN") or next(iter(eq_token_map.values()))
    sample_1m = fetch_definedge_1m_df("NSE", sample_token, recent_1m_start, now_ist)
    if not sample_1m.empty:
        sample_1m_mkt = sample_1m.between_time("09:15", "15:29")
        unique_dates = sorted(list(set(sample_1m_mkt.index.date)))
        active_session_date = unique_dates[-1]
    else:
        active_session_date = today_date

    for symbol in high_beta_50_stocks:
        eq_token = eq_token_map.get(symbol, "")
        fut_token = fut_map.get(symbol, "")
        token_to_use = eq_token if eq_token else fut_token
        segment_to_use = "NSE" if eq_token else "NFO"

        if not token_to_use:
            continue

        df_day = fetch_definedge_day_df(segment_to_use, token_to_use, start_dt_hist, now_ist)
        if df_day.empty:
            continue

        df_recent_1m = fetch_definedge_1m_df(segment_to_use, token_to_use, recent_1m_start, now_ist)
        df_recent_mkt = df_recent_1m.between_time("09:15", "15:29") if not df_recent_1m.empty else pd.DataFrame()

        df_prior_day = df_day[df_day["date"] < active_session_date].copy()
        if not df_recent_mkt.empty:
            recent_daily_from_1m = df_recent_mkt[df_recent_mkt.index.date < active_session_date].groupby(
                df_recent_mkt[df_recent_mkt.index.date < active_session_date].index.date
            ).agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                volume=("volume", "sum")
            ).reset_index()
            recent_daily_from_1m.columns = ["date", "open", "high", "low", "close", "volume"]

            combined_prior = pd.concat(
                [df_prior_day[["date", "open", "high", "low", "close", "volume"]], recent_daily_from_1m],
                ignore_index=True
            )
            combined_prior = combined_prior.drop_duplicates(subset=["date"], keep="last").sort_values("date")
        else:
            combined_prior = df_prior_day

        if len(combined_prior) < 20:
            continue

        prev_close = float(combined_prior["close"].iloc[-1])
        prev_close_date = combined_prior["date"].iloc[-1]

        hist_vol = combined_prior["volume"]
        sma_10 = float(hist_vol.tail(10).mean())
        sma_50 = float(hist_vol.tail(50).mean())
        sma_100 = float(hist_vol.tail(100).mean())
        max_hurdle = max(sma_10, sma_50, sma_100)

        baseline_tps = max_hurdle / 22500.0

        DAILY_BASELINE_CACHE[symbol] = {
            "segment": segment_to_use,
            "token": token_to_use,
            "prev_close": prev_close,
            "prev_close_date": str(prev_close_date),
            "sma_10": int(round(sma_10)),
            "sma_50": int(round(sma_50)),
            "sma_100": int(round(sma_100)),
            "max_hurdle": int(round(max_hurdle)),
            "baseline_tps": round(baseline_tps, 2)
        }

    log(f"✅ Cached 10/50/100-Day Volume SMAs & True Daily Prev Close for {len(DAILY_BASELINE_CACHE)}/50 stocks.")
    return active_session_date

# ==============================================================================
# 5. FLOW QUALITY & PURE VWAP BIAS ANALYZER
# ==============================================================================
def evaluate_slice_metrics(df_slice, prev_close, max_hurdle):
    cum_vol = int(df_slice["volume"].sum())
    price = float(df_slice["close"].iloc[-1])
    pct_chg = round(((price - prev_close) / prev_close) * 100.0, 2) if prev_close > 0 else 0.0

    typical_price = (df_slice["high"] + df_slice["low"] + df_slice["close"]) / 3.0
    vwap = float((typical_price * df_slice["volume"]).sum() / cum_vol) if cum_vol > 0 else price

    if price >= vwap:
        vwap_bias = f"ABOVE VWAP: CE Bias ({vwap:.1f})"
    else:
        vwap_bias = f"BELOW VWAP: PE Bias ({vwap:.1f})"

    bars_5m = df_slice["volume"].resample("5min").sum()
    num_5m_bars = len(bars_5m)
    max_5m_share = float(bars_5m.max() / cum_vol) if cum_vol > 0 else 1.0

    expected_5m_vol = max_hurdle / 75.0
    active_5m_bars = int((bars_5m >= (0.6 * expected_5m_vol)).sum())
    participation_pct = int(round((active_5m_bars / num_5m_bars) * 100)) if num_5m_bars > 0 else 0

    if num_5m_bars >= 4 and max_5m_share > 0.50 and participation_pct < 45:
        flow_tag = f"BULK SPIKE ({participation_pct}% Active)"
    else:
        flow_tag = f"SUSTAINED ({participation_pct}% Active)"

    return price, pct_chg, round(vwap, 2), vwap_bias, flow_tag

# ==============================================================================
# 6. DUAL-CATEGORY HTML + PLAIN TEXT EMAIL SENDER
# ==============================================================================
def send_dual_category_email(sma_alerts, vel_alerts, session_date):
    now_str = datetime.now(IST).strftime("%d-%b %H:%M:%S")
    both_count = sum(1 for a in sma_alerts if a.get("in_both")) or sum(1 for v in vel_alerts if v.get("in_both"))
    subject = (
        f"⚡ High-Beta Volume Alert | ★ Both: {both_count} | "
        f"Cat 1 (3-SMA): {len(sma_alerts)} | Cat 2 (Vel): {len(vel_alerts)} [{now_str}]"
    )

    sorted_sma = sorted(sma_alerts, key=lambda x: (0 if x["in_both"] else 1, x["sort_time"]))
    sorted_vel = sorted(vel_alerts, key=lambda x: (0 if x["in_both"] else 1, x["sort_time"]))

    headers_cat1 = [
        "Stock (Daily %)", "Category Match", "Trigger Time", "LTP",
        "Vol at Trigger (Current)", "10d SMA", "50d SMA", "100d SMA", "Flow Quality", "VWAP Bias"
    ]
    rows_cat1 = [
        [
            f"{a['symbol']} ({a['pct_chg']:+.2f}%)",
            f"★ BOTH (Vel @ {a['vel_cross_hm']})" if a["in_both"] else "Cat 1 Only",
            a["trigger_time"],
            f"{a['price']:.2f}",
            f"{a['trigger_vol']:,} ({a['current_vol']:,})",
            f"{a['sma_10']:,}",
            f"{a['sma_50']:,}",
            f"{a['sma_100']:,}",
            a["flow_tag"],
            a["vwap_bias"]
        ]
        for a in sorted_sma
    ]

    headers_cat2 = [
        "Stock (Daily %)", "Category Match", "Trigger Time", "LTP",
        "Velocity Mult", "Trigger TPS (Hist TPS)", "Cum Vol", "Flow Quality", "VWAP Bias"
    ]
    rows_cat2 = [
        [
            f"{v['symbol']} ({v['pct_chg']:+.2f}%)",
            f"★ BOTH (3-SMA @ {v['sma_cross_hm']})" if v["in_both"] else "Cat 2 Only",
            v["trigger_time"],
            f"{v['price']:.2f}",
            f"{v['velocity_mult']:.2f}x",
            f"{v['live_tps']:,}/s ({v['baseline_tps']:,}/s)",
            f"{v['current_vol']:,}",
            v["flow_tag"],
            v["vwap_bias"]
        ]
        for v in sorted_vel
    ]

    plain_parts = [
        f"Session Date: {session_date} | Alert Generated: {now_str} IST",
        "Sorting: [1] Stocks in BOTH Categories at Top -> [2] Chronological Trigger Time\n"
    ]
    if rows_cat1:
        plain_parts.append("=== CATEGORY 1: VOLUME GREATER THAN ALL 3 AVERAGES (10d, 50d & 100d SMA) ===")
        plain_parts.append(tabulate(rows_cat1, headers=headers_cat1, tablefmt="simple") + "\n")
    if rows_cat2:
        plain_parts.append(f"=== CATEGORY 2: VELOCITY MULTIPLIER BREAKOUTS (>= {VELOCITY_MULT_THRESHOLD}x Historical Pace) ===")
        plain_parts.append(tabulate(rows_cat2, headers=headers_cat2, tablefmt="simple") + "\n")

    plain_body = "\n".join(plain_parts)
    log(f"\n📧 DISPATCHING DUAL-CATEGORY ALERT:\n{plain_body}")

    if not SENDER_EMAIL or not RECEIVER_EMAIL or not APP_PASSWORD:
        log("⚠️ Email credentials missing in environment. Skipping SMTP send.")
        return

    def build_html_table(headers, rows, sorted_items, header_bg):
        if not rows:
            return "<p style='color:#64748b;'>No new triggers in this category on this cycle.</p>"
        th = "".join(
            f'<th style="background-color:{header_bg};color:#ffffff;padding:9px 12px;border:1px solid #cbd5e1;text-align:left;font-size:14px;white-space:nowrap;">{h}</th>'
            for h in headers
        )
        trs = []
        for i, (r, item) in enumerate(zip(rows, sorted_items)):
            if item.get("in_both"):
                bg = "#fef9c3"
                font_wt = "600"
            else:
                bg = "#f8fafc" if i % 2 == 1 else "#ffffff"
                font_wt = "400"
            tds = "".join(
                f'<td style="padding:8px 12px;border:1px solid #cbd5e1;font-size:14px;color:#0f172a;font-weight:{font_wt};white-space:nowrap;">{cell}</td>'
                for cell in r
            )
            trs.append(f'<tr style="background-color:{bg};">{tds}</tr>')
        return f'<table style="border-collapse:collapse;font-family:Consolas,monospace;margin-bottom:18px;"><thead><tr>{th}</tr></thead><tbody>{"".join(trs)}</tbody></table>'

    html_cat1 = build_html_table(headers_cat1, rows_cat1, sorted_sma, "#1e293b")
    html_cat2 = build_html_table(headers_cat2, rows_cat2, sorted_vel, "#065f46")

    html_body = f"""
    <html>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;font-size:15px;color:#0f172a;">
        <h3 style="margin:0 0 6px 0;">⚡ High-Beta Underlying Volume & Velocity Breakout Alert</h3>
        <p style="margin:0 0 6px 0;color:#475569;">
            <strong>Trading Session:</strong> {session_date} &nbsp;|&nbsp; <strong>Dispatched:</strong> {now_str} IST
        </p>

        <h4 style="margin:0 0 8px 0;color:#1e293b;">
            📌 Category 1: Volume Surpassed All 3 Averages (10d, 50d & 100d SMA) — [{len(rows_cat1)} Stocks]
        </h4>
        {html_cat1}

        <h4 style="margin:18px 0 8px 0;color:#065f46;">
            🚀 Category 2: Intraday Velocity Multiplier (>= {VELOCITY_MULT_THRESHOLD}x Historical Shares/Sec) — [{len(rows_cat2)} Stocks]
        </h4>
        {html_cat2}
    </body>
    </html>
    """

    for attempt in range(1, 4):
        try:
            msg = MIMEMultipart("alternative")
            msg["From"] = SENDER_EMAIL
            msg["To"] = RECEIVER_EMAIL
            msg["Subject"] = subject
            msg.attach(MIMEText(plain_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=25) as server:
                server.login(SENDER_EMAIL, APP_PASSWORD)
                server.send_message(msg)
            log(f"✅ Dual-Category Email sent to {RECEIVER_EMAIL}!")
            return
        except Exception as e:
            log(f"⚠️ Email send attempt {attempt}/3 failed: {e}")
            time.sleep(2)

# ==============================================================================
# 7. FAST INTRADAY SCANNER CYCLE (TRUE BAR-TO-BAR CROSSOVER DETECTION)
# ==============================================================================
def run_intraday_scan_pass(active_session_date):
    now_ist = datetime.now(IST)
    session_date_str = str(active_session_date)

    memory = load_memory(session_date_str)
    sma_already_alerted = set(memory.get("sma_alerted", []))
    vel_already_alerted = set(memory.get("vel_alerted", []))

    start_session_dt = IST.localize(datetime.combine(active_session_date, dtime(9, 15)))
    end_query_dt = now_ist if active_session_date == now_ist.date() else IST.localize(datetime.combine(active_session_date, dtime(15, 30)))

    new_sma_alerts = []
    new_vel_alerts = []
    live_dashboard_rows = []

    for symbol, base in DAILY_BASELINE_CACHE.items():
        try:
            df_1m = fetch_definedge_1m_df(base["segment"], base["token"], start_session_dt, end_query_dt)
            if df_1m.empty:
                continue

            today_df = df_1m[df_1m.index.date == active_session_date].between_time("09:15", "15:29").copy()
            if today_df.empty:
                continue

            today_df["cum_vol"] = today_df["volume"].cumsum()
            current_cum_vol = int(today_df["cum_vol"].iloc[-1])
            max_hurdle = base["max_hurdle"]
            prev_close = base["prev_close"]

            bar_end_times = today_df.index + timedelta(minutes=1)
            elapsed_secs_series = np.clip((bar_end_times - start_session_dt).total_seconds(), 60.0, 22500.0)
            today_df["elapsed_sec"] = elapsed_secs_series
            today_df["running_tps"] = today_df["cum_vol"] / today_df["elapsed_sec"]
            today_df["running_vel_mult"] = (
                today_df["running_tps"] / base["baseline_tps"] if base["baseline_tps"] > 0 else 0.0
            )

            # True Crossover Detection: Shifted boolean checks so 09:19 doesn't clump all early stocks
            today_df["sma_crossed"] = today_df["cum_vol"] > max_hurdle
            today_df["vel_crossed"] = (
                (today_df["elapsed_sec"] >= MIN_ELAPSED_SECONDS_FOR_VEL)
                & (today_df["running_vel_mult"] >= VELOCITY_MULT_THRESHOLD)
            )

            sma_cross_df = today_df[today_df["sma_crossed"]]
            sma_cross_hm = sma_cross_df.index[0].strftime("%H:%M") if not sma_cross_df.empty else "-"

            # True Crossover: currently true and was false (or it's the first eligible bar if already past threshold)
            today_df["vel_new_cross"] = today_df["vel_crossed"] & (~today_df["vel_crossed"].shift(1).fillna(False))
            vel_cross_df = today_df[today_df["vel_new_cross"]]
            if vel_cross_df.empty:
                vel_cross_df = today_df[today_df["vel_crossed"]]  # Fallback to first eligible bar

            vel_cross_hm = vel_cross_df.index[0].strftime("%H:%M") if not vel_cross_df.empty else "-"

            in_both_today = (not sma_cross_df.empty) and (not vel_cross_df.empty)
            in_any_today = (not sma_cross_df.empty) or (not vel_cross_df.empty)
            earliest_trig_hm = min(
                [t for t in (sma_cross_hm, vel_cross_hm) if t != "-"],
                default="99:99"
            )

            cur_price, cur_pct_chg, cur_vwap, cur_vwap_bias, cur_flow_tag = evaluate_slice_metrics(
                today_df, prev_close, max_hurdle
            )
            cur_vel_mult = round(float(today_df["running_vel_mult"].iloc[-1]), 2)
            vol_vs_max_ratio = round(current_cum_vol / max_hurdle, 2) if max_hurdle > 0 else 0.0

            live_dashboard_rows.append({
                "symbol": symbol,
                "pct_chg": cur_pct_chg,
                "price": cur_price,
                "cum_vol": current_cum_vol,
                "max_hurdle": max_hurdle,
                "vol_pct_of_max": int(round(vol_vs_max_ratio * 100)),
                "in_both": in_both_today,
                "in_any": in_any_today,
                "earliest_trig_hm": earliest_trig_hm,
                "sma_cross_hm": sma_cross_hm,
                "vel_cross_hm": vel_cross_hm,
                "velocity_mult": cur_vel_mult,
                "flow_tag": cur_flow_tag,
                "vwap_bias": cur_vwap_bias
            })

            # --- CATEGORY 1 TRIGGER ---
            if symbol not in sma_already_alerted and not sma_cross_df.empty:
                first_cross_ts = sma_cross_df.index[0]
                slice_at_cross = today_df.loc[:first_cross_ts]
                trig_price, trig_pct, trig_vwap, trig_bias, trig_flow = evaluate_slice_metrics(
                    slice_at_cross, prev_close, max_hurdle
                )
                trig_vol = int(slice_at_cross["cum_vol"].iloc[-1])

                new_sma_alerts.append({
                    "symbol": symbol,
                    "sort_time": first_cross_ts.strftime("%H:%M"),
                    "trigger_time": first_cross_ts.strftime("%H:%M IST"),
                    "sma_cross_hm": sma_cross_hm,
                    "vel_cross_hm": vel_cross_hm,
                    "in_both": in_both_today,
                    "price": trig_price,
                    "pct_chg": trig_pct,
                    "trigger_vol": trig_vol,
                    "current_vol": current_cum_vol,
                    "sma_10": base["sma_10"],
                    "sma_50": base["sma_50"],
                    "sma_100": base["sma_100"],
                    "max_hurdle": max_hurdle,
                    "vol_vs_max_ratio": vol_vs_max_ratio,
                    "flow_tag": trig_flow,
                    "vwap_bias": trig_bias
                })
                sma_already_alerted.add(symbol)

            # --- CATEGORY 2 TRIGGER ---
            if symbol not in vel_already_alerted and not vel_cross_df.empty:
                first_vel_ts = vel_cross_df.index[0]
                slice_at_vel = today_df.loc[:first_vel_ts]
                v_price, v_pct, v_vwap, v_bias, v_flow = evaluate_slice_metrics(
                    slice_at_vel, prev_close, max_hurdle
                )
                v_mult_at_trig = round(float(slice_at_vel["running_vel_mult"].iloc[-1]), 2)
                v_tps_at_trig = round(float(slice_at_vel["running_tps"].iloc[-1]), 2)
                v_vol_at_trig = int(slice_at_vel["cum_vol"].iloc[-1])

                new_vel_alerts.append({
                    "symbol": symbol,
                    "sort_time": first_vel_ts.strftime("%H:%M"),
                    "trigger_time": first_vel_ts.strftime("%H:%M IST"),
                    "sma_cross_hm": sma_cross_hm,
                    "vel_cross_hm": vel_cross_hm,
                    "in_both": in_both_today,
                    "price": v_price,
                    "pct_chg": v_pct,
                    "velocity_mult": v_mult_at_trig,
                    "live_tps": v_tps_at_trig,
                    "baseline_tps": base["baseline_tps"],
                    "current_vol": v_vol_at_trig,
                    "flow_tag": v_flow,
                    "vwap_bias": v_bias
                })
                vel_already_alerted.add(symbol)

        except Exception as e:
            log(f"⚠️ Error scanning {symbol}: {e}")

    both_cats_today_set = sma_already_alerted.intersection(vel_already_alerted)
    for a in new_sma_alerts:
        if a["symbol"] in both_cats_today_set:
            a["in_both"] = True
    for v in new_vel_alerts:
        if v["symbol"] in both_cats_today_set:
            v["in_both"] = True

    if live_dashboard_rows:
        top_rows = sorted(
            live_dashboard_rows,
            key=lambda x: (
                0 if x["in_both"] else (1 if x["in_any"] else 2),
                x["earliest_trig_hm"],
                -x["vol_pct_of_max"]
            )
        )[:15]
        console_table = [
            [
                f"{r['symbol']} ({r['pct_chg']:+.2f}%)",
                "★ BOTH CATS" if r["in_both"] else ("1 CAT" if r["in_any"] else "-"),
                f"{r['price']:.2f}",
                r["sma_cross_hm"],
                r["vel_cross_hm"],
                f"{r['cum_vol']:,} ({r['vol_pct_of_max']}%)",
                f"{r['velocity_mult']:.2f}x",
                r["flow_tag"],
                r["vwap_bias"]
            ]
            for r in top_rows
        ]
        log(f"\n📊 Live High-Beta Dashboard [Sorted: ★ BOTH -> Trigger Time] ({now_ist.strftime('%H:%M:%S IST')}):")
        log(tabulate(
            console_table,
            headers=[
                "Stock (Daily %)", "Match", "LTP", "3-SMA Time", "Vel>=2x Time",
                "Cum Vol (% Max SMA)", "Live Vel", "Flow Quality", "VWAP Bias"
            ],
            tablefmt="simple"
        ))

    memory["sma_alerted"] = sorted(list(sma_already_alerted))
    memory["vel_alerted"] = sorted(list(vel_already_alerted))
    save_memory(memory)

    if new_sma_alerts or new_vel_alerts:
        send_dual_category_email(new_sma_alerts, new_vel_alerts, active_session_date)
    else:
        log(
            f"ℹ️ No new first-time triggers on this pass "
            f"(Already alerted today -> Cat 1 [3-SMA]: {len(sma_already_alerted)}, "
            f"Cat 2 [Velocity]: {len(vel_already_alerted)}, ★ Both: {len(both_cats_today_set)})."
        )

# ==============================================================================
# 8. CONTINUOUS MARKET CLOCK
# ==============================================================================
def main():
    now_ist = datetime.now(IST)
    log(f"🚀 Starting High-Beta Triple-SMA & Velocity Scanner at {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")

    get_definedge_session_key()
    eq_token_map, fut_map = load_definedge_master_tokens()
    active_session_date = build_daily_volume_baseline_cache(eq_token_map, fut_map, now_ist)

    is_weekday = now_ist.weekday() < 5
    in_live_hours = is_weekday and (dtime(9, 10) <= now_ist.time() <= dtime(15, 32))

    if not in_live_hours:
        log("⚡ Outside live NSE market hours — running immediate single-pass verification scan & exiting...")
        run_intraday_scan_pass(active_session_date)
        log("🏁 Single-pass verification complete.")
        return

    session_cutoff = dtime(15, 31)
    while True:
        now_ist = datetime.now(IST)
        if now_ist.time() > session_cutoff:
            log(f"⏹️ Market session ended ({session_cutoff}). Exiting cleanly.")
            break

        if now_ist.time() >= dtime(9, 15):
            cycle_start = time.time()
            run_intraday_scan_pass(active_session_date)
            elapsed = time.time() - cycle_start
            sleep_for = max(5.0, POLL_INTERVAL_SECONDS - elapsed)
            log(f"💤 Cycle finished in {elapsed:.1f}s. Sleeping {sleep_for:.0f}s until next continuous check...")
            time.sleep(sleep_for)
        else:
            wait_secs = (IST.localize(datetime.combine(now_ist.date(), dtime(9, 15))) - now_ist).total_seconds()
            log(f"⏳ Waiting {wait_secs:.0f}s for 09:15:00 IST market open...")
            time.sleep(min(30.0, max(1.0, wait_secs)))

if __name__ == "__main__":
    main()
