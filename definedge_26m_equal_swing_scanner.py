def validate_inter_swing_bounce(df, s1_info, s2_info):
    s1_end = s1_info["s_end"]
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

    # 3. STRICT SUPPORT: Swing 2 low L2 MUST NOT breach Swing 1 low L1 (allows max 2% wick tolerance)
    if l2 < (l1 * 0.98):
        return False

    return True
