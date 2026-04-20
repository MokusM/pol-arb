"""
TREND filter for /arb entry — block trades on trending markets where opp_ask
typically overshoots BE before trigger fires (= hedge impossible → full loss).

Fetches 1m klines from Binance public API, computes ADX + momentum + ATR%.
Market classified as TREND if both:
  - ADX >= TREND_ADX_THRESHOLD (default 25)
  - momentum_pct > TREND_MOMENTUM_MULT × ATR%  (default 1.5×)

Fails open: on Binance API error, allow /arb (don't block whole bot on external outage).
"""

import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

TREND_ADX_THRESHOLD = 25.0
TREND_MOMENTUM_MULT = 1.5
KLINES_LIMIT = 50
ADX_PERIOD = 14
ATR_PERIOD = 14
MOMENTUM_LOOKBACK = 5

ASSET_TO_BINANCE = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT",
    "XRP": "XRPUSDT", "DOGE": "DOGEUSDT",
}


async def _fetch_klines(symbol: str, interval: str = "1m", limit: int = KLINES_LIMIT) -> list[dict]:
    """Return list of {'open', 'high', 'low', 'close'} dicts, most recent last."""
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(BINANCE_KLINES, params={"symbol": symbol, "interval": interval, "limit": limit})
            if r.status_code != 200:
                logger.warning("binance klines %s: HTTP %d", symbol, r.status_code)
                return []
            return [
                {"open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4])}
                for k in r.json()
            ]
    except Exception as e:
        logger.warning("binance klines %s error: %s", symbol, e)
        return []


def _tr_series(klines: list[dict]) -> list[float]:
    trs = []
    for i in range(1, len(klines)):
        h, l, prev_c = klines[i]["high"], klines[i]["low"], klines[i - 1]["close"]
        trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    return trs


def _adx(klines: list[dict], period: int = ADX_PERIOD) -> float:
    """Wilder-smoothed ADX. Returns latest value."""
    if len(klines) < period * 2 + 1:
        return 0.0

    highs = [k["high"] for k in klines]
    lows = [k["low"] for k in klines]

    tr = _tr_series(klines)
    plus_dm = []
    minus_dm = []
    for i in range(1, len(klines)):
        up = highs[i] - highs[i - 1]
        dn = lows[i - 1] - lows[i]
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)

    if len(tr) < period:
        return 0.0

    tr_s = sum(tr[:period])
    plus_s = sum(plus_dm[:period])
    minus_s = sum(minus_dm[:period])

    dx_list = []
    for i in range(period, len(tr)):
        tr_s = tr_s - (tr_s / period) + tr[i]
        plus_s = plus_s - (plus_s / period) + plus_dm[i]
        minus_s = minus_s - (minus_s / period) + minus_dm[i]
        if tr_s <= 0:
            continue
        plus_di = 100.0 * plus_s / tr_s
        minus_di = 100.0 * minus_s / tr_s
        denom = plus_di + minus_di
        if denom <= 0:
            continue
        dx_list.append(100.0 * abs(plus_di - minus_di) / denom)

    if not dx_list:
        return 0.0
    if len(dx_list) < period:
        return dx_list[-1]
    # Wilder smoothing of DX → ADX
    adx = sum(dx_list[:period]) / period
    for dx in dx_list[period:]:
        adx = (adx * (period - 1) + dx) / period
    return adx


def _atr_pct(klines: list[dict], period: int = ATR_PERIOD) -> float:
    """ATR as % of current close."""
    tr = _tr_series(klines)
    if len(tr) < period:
        return 0.0
    atr = sum(tr[-period:]) / period
    close = klines[-1]["close"]
    return (atr / close * 100.0) if close > 0 else 0.0


def _momentum_pct(klines: list[dict], lookback: int = MOMENTUM_LOOKBACK) -> float:
    """|close[-1] - close[-1-lookback]| / close[-1-lookback] × 100."""
    if len(klines) < lookback + 1:
        return 0.0
    start = klines[-lookback - 1]["close"]
    end = klines[-1]["close"]
    return (abs(end - start) / start * 100.0) if start > 0 else 0.0


async def check_trend(asset: str) -> tuple[bool, str]:
    """
    Check if market is trending.

    Returns (is_trend, diagnostic_str).
    Fails open: on Binance API error returns (False, "binance unavailable").
    """
    symbol = ASSET_TO_BINANCE.get(asset.upper())
    if not symbol:
        return False, f"unknown asset {asset}"

    klines = await _fetch_klines(symbol)
    if len(klines) < 30:
        return False, "binance unavailable (fail-open)"

    adx = _adx(klines)
    atr_p = _atr_pct(klines)
    mom_p = _momentum_pct(klines)

    is_trend = adx >= TREND_ADX_THRESHOLD and mom_p > TREND_MOMENTUM_MULT * atr_p

    diag = f"ADX={adx:.1f} mom={mom_p:.3f}% atr={atr_p:.3f}%"
    return is_trend, diag
