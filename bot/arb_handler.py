"""
Manual Arb/Hedge bot for Polymarket SOL 5m markets — event-driven rewrite.

Architecture:
- One WS connection per active hedge, subscribed to BOTH leg1 and opp tokens
- Single state machine: LEG1_OPEN → LEG2_OPEN → CLOSED
- Decisions fire on each WS book/price update (no REST polling, no time-based cooldown)
- in_flight flag prevents concurrent orders during placement
- Single FAK orders only (no bracket, no auto-scale, no retry on errors → no double-buy)

Auto stop-loss (LEG1_OPEN) — debounced trigger + three-tier exit:
- ARM      when leg1_max_seen >= entry + AUTO_HEDGE_ARM_PROFIT (0.10)
- TRIGGER  when armed AND leg1_bid <= entry (= would lose money if sold)
            AND condition held for >= TRIGGER_DEBOUNCE_SEC (3.0s) — filters wicks
- TIER 1   opp_ask <= BE                          → hedge at BE (best)
- TIER 2   opp_ask <= BE + MAX_HEDGE_LOSS (0.10)  → hedge at controlled loss
- TIER 3   leg1_bid > 0                            → sell leg1 at bid (forced exit)

Auto-exit (LEG2_OPEN):
- bid_up + bid_down >= EXIT_THRESHOLD (1.03) → sell both at WS bids

Near-expiry sell (LEG1_OPEN, unhedged):
- <60s left + leg1 bid <= entry/2 → auto-sell leg1
- <30s left → force-sell leg1 at any positive bid

Public API:
- do_arb_buy   — leg1 buy + start monitor
- do_arb_hedge — manual hedge (cancel monitor, place leg2, restart monitor in LEG2_OPEN)
- do_arb_sell  — manual sell-all (cancel monitor, sell legs at bid)
"""

import asyncio
import html
import json
import logging
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
import websockets

logger = logging.getLogger(__name__)

# ── Endpoints ──
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
WS_CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
ET = ZoneInfo("America/New_York")

ASSETS = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "XRP": "xrp", "DOGE": "doge"}
WINDOWS_MIN = {"5m": 5, "15m": 15}

# ── Arb config ──
ARB_SHARES = 5.5            # leg1 size (5 + fee buffer for round-trips)
ARB_MIN_LEG1_PRICE = 0.35   # skip cheap-side if too cheap
ARB_MIN_TIME_LEFT = 120     # seconds
FAK_SLIPPAGE = 0.03         # FAK price = ask + slippage (room for ask to move between fetch and post)

# ── Auto stop-loss ──
AUTO_HEDGE_ARM_PROFIT = 0.10        # ARM when leg1_max_seen ≥ entry + this
TRIGGER_DEBOUNCE_SEC = 3.0          # leg1_bid must stay ≤ entry for this long before firing
FEE_BUFFER = 0.05                   # opp BE = 1.0 - leg1_entry - this
MAX_HEDGE_LOSS_PER_SHARE = 0.10     # tier 2: hedge at up to BE + this if BE unreachable
NEAR_EXPIRY_SEC = 60
FORCE_SELL_SEC = 30

# ── Auto-exit ──
EXIT_THRESHOLD = 1.03
EXIT_RETRY_COOLDOWN = 5.0   # seconds between retry attempts on partial-fill


class HedgeState(Enum):
    LEG1_OPEN = "leg1_open"
    LEG2_OPEN = "leg2_open"
    CLOSED = "closed"


_active_monitors: dict[int, asyncio.Task] = {}


# ── Market data ──

def _current_unix_start(window_min: int) -> int:
    now = datetime.now(ET)
    minute_floor = (now.minute // window_min) * window_min
    base = now.replace(minute=minute_floor, second=0, microsecond=0)
    return int(base.timestamp())


async def find_current_market(asset: str, window: str) -> Optional[dict]:
    asset_slug = ASSETS.get(asset.upper())
    window_min = WINDOWS_MIN.get(window)
    if not asset_slug or not window_min:
        return None

    async with httpx.AsyncClient(timeout=5) as c:
        for offset in (0, window_min):
            unix = _current_unix_start(window_min) + offset * 60
            slug = f"{asset_slug}-updown-{window}-{unix}"
            try:
                r = await c.get(f"{GAMMA}/markets/slug/{slug}")
                if r.status_code != 200:
                    continue
                m = r.json()
                if m.get("closed"):
                    continue
                tokens = m.get("clobTokenIds") or []
                if isinstance(tokens, str):
                    tokens = json.loads(tokens)
                if len(tokens) < 2:
                    continue
                end_iso = m.get("endDate") or ""
                end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                tl = (end_dt - datetime.now(timezone.utc)).total_seconds()
                if tl <= 15:
                    continue
                return {
                    "market_id": str(m.get("id", "")),
                    "slug": slug,
                    "title": m.get("question", ""),
                    "token_up": str(tokens[0]),
                    "token_down": str(tokens[1]),
                    "time_left_sec": int(tl),
                    "neg_risk": bool(m.get("negRisk", False)),
                }
            except Exception as e:
                logger.debug("find_market: %s", e)
    return None


async def _fetch_book(token: str) -> tuple[float, float]:
    """REST seed for ask, bid (only used at monitor start)."""
    try:
        async with httpx.AsyncClient(timeout=3) as c:
            r = await c.get(f"{CLOB}/book", params={"token_id": token})
            if r.status_code != 200:
                return 0.0, 0.0
            book = r.json()
            asks = [float(a["price"]) for a in (book.get("asks") or []) if float(a.get("price", 0)) > 0]
            bids = [float(b["price"]) for b in (book.get("bids") or []) if float(b.get("price", 0)) > 0]
            return (min(asks) if asks else 0.0, max(bids) if bids else 0.0)
    except Exception:
        return 0.0, 0.0


async def _refetch_market_tokens(slug: str) -> Optional[tuple[str, str, bool, int]]:
    """Refetch token_up, token_down, neg_risk, time_left_sec via slug."""
    async with httpx.AsyncClient(timeout=5) as c:
        try:
            r = await c.get(f"{GAMMA}/markets/slug/{slug}")
            if r.status_code != 200:
                return None
            m = r.json()
            tokens = m.get("clobTokenIds") or []
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            if len(tokens) < 2:
                return None
            end_iso = m.get("endDate") or ""
            end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
            tl = max(0, int((end_dt - datetime.now(timezone.utc)).total_seconds()))
            return str(tokens[0]), str(tokens[1]), bool(m.get("negRisk", False)), tl
        except Exception as e:
            logger.warning("refetch_market: %s", e)
            return None


# ── Order primitives (no retry on errors → no duplicates) ──

def _classify_error(err: object) -> str:
    """Map raw CLOB error to short user-friendly reason."""
    s = str(err).lower()
    if "no orders found to match" in s:
        return "price moved (FAK no-match)"
    if "market_resolved" in s or "market is resolved" in s:
        return "market closed/resolved"
    if "request exception" in s or "status_code=none" in s:
        return "network error"
    if "insufficient" in s and "balance" in s:
        return "insufficient balance"
    if "insufficient" in s and "allowance" in s:
        return "insufficient allowance"
    if "tick" in s:
        return "invalid tick price"
    if "min" in s and "size" in s:
        return "below min order size"
    return str(err)[:80]


async def _verify_matched(exec_client, result: dict, side: str) -> tuple[Optional[dict], Optional[str]]:
    """FAK should always come back as 'matched'. If 'live' (rare race) — try cancel and treat as failure."""
    status = (result.get("status") or "").lower()
    if status == "matched":
        return result, None
    oid = str(result.get("orderID") or result.get("order_id") or "")
    if status == "live" and oid:
        try:
            await exec_client.cancel_order(oid)
            logger.warning("FAK %s went live unexpectedly — cancelled order %s", side, oid[:12])
        except Exception as e:
            logger.error("FAK %s went live, cancel failed: %s", side, e)
        return None, "FAK went live (cancelled, no fill)"
    return None, _classify_error(f"unexpected status={status}")


async def _safe_fak_buy(exec_client, *, token, price, shares, neg_risk, market_slug, market_id) -> tuple[Optional[dict], Optional[str]]:
    """Single FAK buy. Returns (result, None) on success, (None, reason) on failure."""
    try:
        result = await exec_client.buy_shares(
            token_id=token, price=price, size=shares,
            neg_risk=neg_risk, market_slug=market_slug, market_id=market_id,
            order_type="FAK", use_exact_price=True,
        )
        if isinstance(result, dict) and result.get("success") is not False:
            return await _verify_matched(exec_client, result, "BUY")
        err = result.get("error", "?") if isinstance(result, dict) else result
        reason = _classify_error(err)
        logger.warning("FAK buy returned error: %s", err)
        return None, reason
    except Exception as e:
        reason = _classify_error(e)
        logger.error("FAK buy exception: %s", e)
        return None, reason


async def _safe_fak_sell(exec_client, *, token, price, shares) -> tuple[Optional[dict], Optional[str]]:
    """Single FAK sell. Returns (result, None) on success, (None, reason) on failure."""
    try:
        result = await exec_client.sell_shares(token, price, shares, order_type="FAK")
        if isinstance(result, dict) and result.get("success") is not False:
            return await _verify_matched(exec_client, result, "SELL")
        err = result.get("error", "?") if isinstance(result, dict) else result
        reason = _classify_error(err)
        logger.warning("FAK sell returned error: %s", err)
        return None, reason
    except Exception as e:
        reason = _classify_error(e)
        logger.error("FAK sell exception: %s", e)
        return None, reason


# ── Monitor (event-driven state machine) ──

class HedgeMonitor:
    def __init__(self, hedge_id, market, leg1_token, opp_token, leg1_side,
                 leg1_shares, leg1_price, leg1_cost, time_left_sec, exec_client,
                 neg_risk, chat_id, tg_send_fn):
        self.hedge_id = hedge_id
        self.market = market
        self.leg1_token = leg1_token
        self.opp_token = opp_token
        self.leg1_side = leg1_side
        self.opp_side = "DOWN" if leg1_side == "UP" else "UP"
        self.leg1_shares = leg1_shares
        self.leg1_price = leg1_price
        self.leg1_cost = leg1_cost
        self.exec_client = exec_client
        self.neg_risk = neg_risk
        self.chat_id = chat_id
        self.tg_send_fn = tg_send_fn

        self.expiry = datetime.now(timezone.utc) + timedelta(seconds=time_left_sec)
        self.state = HedgeState.LEG1_OPEN

        self.ask: dict[str, float] = {leg1_token: 0.0, opp_token: 0.0}
        self.bid: dict[str, float] = {leg1_token: 0.0, opp_token: 0.0}

        self.leg1_max_seen = 0.0
        self.armed = False
        self.arm_threshold = round(leg1_price + AUTO_HEDGE_ARM_PROFIT, 3)
        self.opp_be_price = round(1.0 - leg1_price - FEE_BUFFER, 3)
        # Trigger fires when leg1_bid ≤ entry (= we'd lose money if sold) AND held for TRIGGER_DEBOUNCE_SEC
        self._trigger_first_met_ts: Optional[float] = None

        self.leg2_shares = 0.0
        self.leg2_price = 0.0
        self.leg2_cost = 0.0

        self.in_flight = False
        self._last_exit_attempt_ts = 0.0

    async def run(self):
        sub_msg = json.dumps({"type": "market", "assets_ids": [self.leg1_token, self.opp_token]})

        secs_left = int((self.expiry - datetime.now(timezone.utc)).total_seconds())
        logger.info(
            "MON #%d start: %s entry=%.3f arm>=%.3f trig=bid≤%.3f(+%.0fs) opp_BE<=%.3f state=%s expiry %ds",
            self.hedge_id, self.leg1_side, self.leg1_price,
            self.arm_threshold, self.leg1_price, TRIGGER_DEBOUNCE_SEC, self.opp_be_price,
            self.state.value, secs_left,
        )

        # Seed prices
        for tok in (self.leg1_token, self.opp_token):
            ask, bid = await _fetch_book(tok)
            self.ask[tok] = ask
            self.bid[tok] = bid

        leg1_seed = self.ask[self.leg1_token]
        if leg1_seed > self.leg1_max_seen:
            self.leg1_max_seen = leg1_seed
            if self.leg1_max_seen >= self.arm_threshold:
                self.armed = True
                logger.info("MON #%d ARMED on seed: leg1=%.3f", self.hedge_id, leg1_seed)

        try:
            async with websockets.connect(WS_CLOB_URL, ping_interval=10, ping_timeout=5) as ws:
                await ws.send(sub_msg)

                async for raw in ws:
                    try:
                        msgs = json.loads(raw)
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                        self._apply_ws_msgs(msgs)
                    except Exception:
                        continue

                    if await self._tick():
                        return
        except asyncio.CancelledError:
            logger.info("MON #%d cancelled", self.hedge_id)
            raise
        except Exception as e:
            logger.error("MON #%d WS error: %s", self.hedge_id, e)
        finally:
            _active_monitors.pop(self.hedge_id, None)

    def _apply_ws_msgs(self, msgs: list):
        for m in msgs:
            asset_id = m.get("asset_id")
            if asset_id in self.ask:
                if "asks" in m:
                    asks = [float(a.get("price", 0)) for a in (m.get("asks") or []) if float(a.get("price", 0)) > 0]
                    if asks:
                        self.ask[asset_id] = min(asks)
                if "bids" in m:
                    bids = [float(b.get("price", 0)) for b in (m.get("bids") or []) if float(b.get("price", 0)) > 0]
                    if bids:
                        self.bid[asset_id] = max(bids)

            for ch in m.get("price_changes") or []:
                ch_id = ch.get("asset_id")
                if ch_id not in self.ask:
                    continue
                side = (ch.get("side") or "").upper()
                if side == "SELL":
                    ba = ch.get("best_ask")
                    if ba:
                        self.ask[ch_id] = float(ba)
                elif side == "BUY":
                    bb = ch.get("best_bid")
                    if bb:
                        self.bid[ch_id] = float(bb)

    async def _tick(self) -> bool:
        """Run all decision checks. Return True to exit monitor."""
        if self.state == HedgeState.CLOSED:
            return True

        leg1_ask = self.ask[self.leg1_token]
        if leg1_ask > self.leg1_max_seen:
            self.leg1_max_seen = leg1_ask
            if not self.armed and self.leg1_max_seen >= self.arm_threshold:
                self.armed = True
                logger.info("MON #%d ARMED: leg1_max=%.3f", self.hedge_id, self.leg1_max_seen)

        if self.in_flight:
            return False

        secs_left = (self.expiry - datetime.now(timezone.utc)).total_seconds()

        # Near-expiry auto-sell (LEG1_OPEN only)
        if self.state == HedgeState.LEG1_OPEN and secs_left <= NEAR_EXPIRY_SEC:
            leg1_bid = self.bid[self.leg1_token]
            should_sell = leg1_bid > 0.01 and (
                leg1_bid <= self.leg1_price / 2 or secs_left <= FORCE_SELL_SEC
            )
            if should_sell:
                self.in_flight = True
                try:
                    await self._auto_sell_leg1(leg1_bid, secs_left)
                finally:
                    self.in_flight = False
                self.state = HedgeState.CLOSED
                return True
            if secs_left <= 5:
                if self.tg_send_fn:
                    await self.tg_send_fn(self.chat_id,
                        f"⏱ <b>Leg1 #{self.hedge_id} expiry</b> — no bid, settlement")
                self.state = HedgeState.CLOSED
                return True

        # Auto stop-loss (LEG1_OPEN):
        #   TRIGGER condition: armed AND leg1_bid ≤ entry (= we'd lose money if sold)
        #                       AND condition has been true for ≥ TRIGGER_DEBOUNCE_SEC (filters wicks)
        #   Three-tier exit:
        #     1) opp_ask ≤ BE                       → hedge at BE (best)
        #     2) opp_ask ≤ BE + MAX_HEDGE_LOSS      → hedge with controlled loss
        #     3) leg1_bid > 0                        → sell leg1 at bid (forced exit)
        if self.state == HedgeState.LEG1_OPEN and self.armed:
            leg1_bid = self.bid[self.leg1_token]
            in_loss_zone = leg1_bid > 0 and leg1_bid <= self.leg1_price

            now_ts = datetime.now(timezone.utc).timestamp()
            if in_loss_zone:
                if self._trigger_first_met_ts is None:
                    self._trigger_first_met_ts = now_ts
                    logger.info("MON #%d trigger condition first met: leg1_bid=%.3f ≤ entry=%.3f — debouncing %.0fs",
                                self.hedge_id, leg1_bid, self.leg1_price, TRIGGER_DEBOUNCE_SEC)
                elif now_ts - self._trigger_first_met_ts >= TRIGGER_DEBOUNCE_SEC:
                    opp_ask = self.ask[self.opp_token]

                    tier: int = 0
                    if 0 < opp_ask <= self.opp_be_price:
                        tier = 1
                    elif 0 < opp_ask <= self.opp_be_price + MAX_HEDGE_LOSS_PER_SHARE:
                        tier = 2
                    elif leg1_bid > 0.01:
                        tier = 3

                    if tier in (1, 2):
                        self.in_flight = True
                        try:
                            await self._auto_hedge(opp_ask, tier=tier)
                        finally:
                            self.in_flight = False
                    elif tier == 3:
                        self.in_flight = True
                        try:
                            await self._stop_loss_sell_leg1(leg1_bid)
                        finally:
                            self.in_flight = False
                        self.state = HedgeState.CLOSED
                        return True
                    # else: no liquidity — wait
            else:
                # leg1_bid > entry → recovered, reset debounce
                if self._trigger_first_met_ts is not None:
                    logger.info("MON #%d trigger cleared: leg1_bid=%.3f > entry=%.3f — debounce reset",
                                self.hedge_id, leg1_bid, self.leg1_price)
                    self._trigger_first_met_ts = None

        # Auto-exit (LEG2_OPEN)
        if self.state == HedgeState.LEG2_OPEN:
            b_up = self.bid[self.market["token_up"]]
            b_down = self.bid[self.market["token_down"]]
            if b_up > 0 and b_down > 0 and b_up + b_down >= EXIT_THRESHOLD:
                # Throttle retries — bid_sum often crosses threshold many times in a row
                now_ts = datetime.now(timezone.utc).timestamp()
                if now_ts - self._last_exit_attempt_ts >= EXIT_RETRY_COOLDOWN:
                    self._last_exit_attempt_ts = now_ts
                    self.in_flight = True
                    try:
                        succeeded = await self._auto_exit(b_up, b_down)
                    finally:
                        self.in_flight = False
                    if succeeded:
                        self.state = HedgeState.CLOSED
                        return True
                    # else: partial fill, stay in LEG2_OPEN, retry on next opportunity

        return False

    async def _auto_hedge(self, opp_ask: float, tier: int = 1):
        from bot.storage import mark_hedge_filled
        target = round(opp_ask + FAK_SLIPPAGE, 3)
        label = "BE" if tier == 1 else "loss-budget"
        logger.info("MON #%d AUTO-HEDGE [%s]: opp_ask=%.3f BE=%.3f → FAK @ %.3f",
                    self.hedge_id, label, opp_ask, self.opp_be_price, target)
        result, reason = await _safe_fak_buy(
            self.exec_client, token=self.opp_token, price=target, shares=self.leg1_shares,
            neg_risk=self.neg_risk, market_slug=self.market["slug"], market_id=self.market["market_id"],
        )
        if not result:
            logger.info("MON #%d AUTO-HEDGE failed: %s — will retry on next tick", self.hedge_id, reason)
            return  # stay in LEG1_OPEN — next tick may retry

        ep = float(result.get("_effective_price", target))
        sh = float(result.get("_order_size", self.leg1_shares))
        cost = round(sh * ep, 2)
        oid = str(result.get("orderID") or result.get("order_id") or "")

        mark_hedge_filled(self.hedge_id, self.opp_side, sh, ep, cost, oid)
        self.leg2_shares = sh
        self.leg2_price = ep
        self.leg2_cost = cost
        self.state = HedgeState.LEG2_OPEN

        total = self.leg1_cost + cost
        profit = round(min(self.leg1_shares, sh) * 1.0 - total, 2)
        logger.info("MON #%d hedge filled [%s]: %s %.1fsh @ %.3f pnl=$%+.2f",
                    self.hedge_id, label, self.opp_side, sh, ep, profit)
        emoji = "🛡" if tier == 1 else "⚠️"
        msg_label = "stop-loss BE" if tier == 1 else f"stop-loss (loss budget, opp above {self.opp_be_price:.2f})"
        if self.tg_send_fn:
            await self.tg_send_fn(self.chat_id,
                f"{emoji} <b>Auto-hedge #{self.hedge_id}</b> ({msg_label})\n"
                f"{self.opp_side} {sh:.1f}sh @ {ep:.3f} = ${cost:.2f}\n"
                f"Total: ${total:.2f} | <b>PnL: ${profit:+.2f}</b>"
            )

    async def _stop_loss_sell_leg1(self, leg1_bid: float):
        """Tier 3: opp_ask too high to hedge — dump leg1 at bid."""
        from bot.storage import mark_hedge_sold
        target_bid = leg1_bid
        logger.info("MON #%d STOP-LOSS SELL leg1: bid=%.3f (opp_ask=%.3f > BE+budget=%.3f)",
                    self.hedge_id, leg1_bid, self.ask[self.opp_token],
                    round(self.opp_be_price + MAX_HEDGE_LOSS_PER_SHARE, 3))
        result, reason = await _safe_fak_sell(self.exec_client, token=self.leg1_token,
                                               price=target_bid, shares=self.leg1_shares)
        if not result:
            logger.info("MON #%d STOP-LOSS sell failed: %s", self.hedge_id, reason)
            if self.tg_send_fn:
                await self.tg_send_fn(self.chat_id,
                    f"⚠️ <b>Stop-loss sell #{self.hedge_id} FAIL</b>: {html.escape(reason or '?')}")
            return
        try:
            avg = float(result.get("average_price") or result.get("price") or target_bid)
            sz = float(result.get("size") or result.get("_order_size") or self.leg1_shares)
            received = round(avg * sz, 2)
        except Exception:
            avg = target_bid
            received = round(target_bid * self.leg1_shares, 2)
        pnl = round(received - self.leg1_cost, 2)
        if self.tg_send_fn:
            await self.tg_send_fn(self.chat_id,
                f"🚪 <b>Stop-loss sell #{self.hedge_id}</b> (opp недосяжний)\n"
                f"{self.leg1_side} sold @ {avg:.3f} | Got ${received:.2f}\n"
                f"<b>PnL: ${pnl:+.2f}</b>"
            )
        mark_hedge_sold(self.hedge_id)

    async def _auto_exit(self, b_up: float, b_down: float) -> bool:
        """Return True if both legs sold and hedge is fully closed; False if partial/failed."""
        from bot.storage import mark_hedge_sold
        token_up = self.market["token_up"]
        token_down = self.market["token_down"]
        sh_up = self.leg1_shares if self.leg1_side == "UP" else self.leg2_shares
        sh_down = self.leg2_shares if self.leg1_side == "UP" else self.leg1_shares

        bid_sum = b_up + b_down
        logger.info("MON #%d EXIT: bid_sum=%.3f", self.hedge_id, bid_sum)

        res_up, reason_up = await _safe_fak_sell(self.exec_client, token=token_up, price=b_up, shares=sh_up)
        if not res_up:
            logger.warning("MON #%d UP sell FAILED (%s) — skip DOWN, will retry", self.hedge_id, reason_up)
            if self.tg_send_fn:
                await self.tg_send_fn(self.chat_id,
                    f"⚠️ <b>Exit #{self.hedge_id} retry</b>\nUP sell failed: {html.escape(reason_up or '?')} — DOWN not attempted, will retry")
            return False

        res_down, reason_down = await _safe_fak_sell(self.exec_client, token=token_down, price=b_down, shares=sh_down)
        if not res_down:
            logger.warning("MON #%d DOWN sell FAILED (%s) after UP sold", self.hedge_id, reason_down)

        actual = 0.0
        sold_up = sold_down = 0.0
        for res, shares_target in ((res_up, sh_up), (res_down, sh_down)):
            if not res:
                continue
            try:
                ep = float(res.get("_effective_price") or 0)
                sz = float(res.get("_order_size") or 0)
                if ep > 0 and sz > 0:
                    actual += ep * sz
                    if res is res_up:
                        sold_up = sz
                    else:
                        sold_down = sz
            except Exception:
                pass

        total_cost = self.leg1_cost + self.leg2_cost
        profit = round(actual - total_cost, 2)

        # Both sides actually sold (or close to it)?
        both_ok = res_up is not None and res_down is not None
        partial = " (partial)" if not both_ok else ""

        if self.tg_send_fn:
            fail_lines = []
            if not res_up:
                fail_lines.append(f"⚠️ UP sell FAIL: {html.escape(reason_up or '?')}")
            if not res_down:
                fail_lines.append(f"⚠️ DOWN sell FAIL: {html.escape(reason_down or '?')}")
            extra = ("\n" + "\n".join(fail_lines)) if fail_lines else ""
            await self.tg_send_fn(self.chat_id,
                f"💰 <b>Early exit #{self.hedge_id}</b>{partial}\n"
                f"bid_sum={bid_sum:.3f} ({b_up:.3f}+{b_down:.3f})\n"
                f"Sold: UP {sold_up:.1f}sh, DOWN {sold_down:.1f}sh\n"
                f"Received: ${actual:.2f} | Cost: ${total_cost:.2f}\n"
                f"<b>PnL: ${profit:+.2f}</b>{extra}"
            )

        # Only mark sold if both sides actually filled
        if both_ok:
            mark_hedge_sold(self.hedge_id)
            return True
        logger.warning("MON #%d EXIT: not both sides sold — leaving hedge open in DB", self.hedge_id)
        return False

    async def _auto_sell_leg1(self, leg1_bid: float, secs_left: float):
        from bot.storage import mark_hedge_sold
        logger.info("MON #%d auto-sell leg1: bid=%.3f, %ds to expiry",
                    self.hedge_id, leg1_bid, int(secs_left))
        result, reason = await _safe_fak_sell(self.exec_client, token=self.leg1_token,
                                               price=leg1_bid, shares=self.leg1_shares)
        if not result:
            logger.warning("MON #%d expiry sell failed: %s", self.hedge_id, reason)
            if self.tg_send_fn:
                await self.tg_send_fn(self.chat_id,
                    f"⚠️ <b>Auto-sell leg1 #{self.hedge_id} FAIL</b>: {html.escape(reason or '?')} — settlement")
            return
        try:
            avg = float(result.get("average_price") or result.get("price") or leg1_bid)
            sz = float(result.get("size") or result.get("_order_size") or self.leg1_shares)
            received = round(avg * sz, 2)
        except Exception:
            received = round(leg1_bid * self.leg1_shares, 2)
        loss = round(received - self.leg1_cost, 2)
        if self.tg_send_fn:
            await self.tg_send_fn(self.chat_id,
                f"⏱ <b>Auto-sell leg1 #{self.hedge_id}</b> (expiry)\n"
                f"Sold @ {leg1_bid:.3f} | Got ${received:.2f}\n"
                f"<b>PnL: ${loss:+.2f}</b>"
            )
        mark_hedge_sold(self.hedge_id)


def start_monitor(hedge_id, market, leg1_token, opp_token, leg1_side,
                  leg1_shares, leg1_price, leg1_cost, time_left_sec, exec_client,
                  neg_risk, chat_id, tg_send_fn,
                  initial_state: HedgeState = HedgeState.LEG1_OPEN,
                  leg2_shares: float = 0.0, leg2_price: float = 0.0, leg2_cost: float = 0.0):
    if hedge_id in _active_monitors:
        logger.warning("Monitor #%d already running — skip start", hedge_id)
        return
    mon = HedgeMonitor(hedge_id, market, leg1_token, opp_token, leg1_side,
                       leg1_shares, leg1_price, leg1_cost, time_left_sec,
                       exec_client, neg_risk, chat_id, tg_send_fn)
    mon.state = initial_state
    mon.leg2_shares = leg2_shares
    mon.leg2_price = leg2_price
    mon.leg2_cost = leg2_cost
    task = asyncio.create_task(mon.run())
    _active_monitors[hedge_id] = task


async def cancel_monitor(hedge_id: int):
    """Cancel and await monitor exit so caller can safely start a new one."""
    task = _active_monitors.pop(hedge_id, None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        logger.info("Monitor #%d cancelled", hedge_id)


# ── Public API ──

async def do_arb_buy(asset, window, stake_usd, exec_client, wallet_key, chat_id, tg_send_fn=None) -> str:
    """Buy cheap side via single FAK. Save pending hedge. Start monitor."""
    from bot.storage import save_pending_hedge, get_latest_pending_hedge

    market = await find_current_market(asset, window)
    if not market:
        return f"❌ Не знайдено активний {asset} {window} маркет"

    existing = get_latest_pending_hedge(chat_id, wallet_key)
    if existing and existing.get("market_id") == market["market_id"]:
        return f"⚠️ Вже є leg1 на цьому ринку (#{existing['id']})"

    ask_up, _ = await _fetch_book(market["token_up"])
    ask_down, _ = await _fetch_book(market["token_down"])
    if ask_up <= 0 or ask_down <= 0:
        return "❌ Немає ліквідності"

    if ask_up <= ask_down:
        side, token, ask, opp_ask = "UP", market["token_up"], ask_up, ask_down
        opp_token = market["token_down"]
    else:
        side, token, ask, opp_ask = "DOWN", market["token_down"], ask_down, ask_up
        opp_token = market["token_up"]

    if ask < ARB_MIN_LEG1_PRICE:
        return f"⚠️ Skip: leg1 {ask:.2f} (min {ARB_MIN_LEG1_PRICE:.2f})"
    if market["time_left_sec"] < ARB_MIN_TIME_LEFT:
        return f"⚠️ Skip: {market['time_left_sec']}s left"

    # TREND filter: skip if market is trending (hedge unreachable → full loss risk)
    from bot.trend_filter import check_trend
    is_trend, diag = await check_trend(asset)
    logger.info("ARB %s trend-filter: trend=%s | %s", asset, is_trend, diag)
    if is_trend:
        return f"🔴 <b>TRENDING — /arb skipped</b>\n{diag}\n(арб незахеджимий у трендах — чекай range)"

    target = round(ask + FAK_SLIPPAGE, 3)
    logger.info("ARB BUY: %s %s @ %.3f (ask=%.3f opp=%.3f shares=%.1f tl=%ds | %s)",
                asset, side, target, ask, opp_ask, ARB_SHARES, market["time_left_sec"], diag)

    result, reason = await _safe_fak_buy(
        exec_client, token=token, price=target, shares=ARB_SHARES,
        neg_risk=market["neg_risk"], market_slug=market["slug"], market_id=market["market_id"],
    )
    if not result:
        return f"❌ Buy failed: {html.escape(reason or '?')}"

    ep = float(result.get("_effective_price", target))
    shares = float(result.get("_order_size", ARB_SHARES))
    cost = round(shares * ep, 2)
    order_id = str(result.get("orderID") or result.get("order_id") or "")

    hedge_id = save_pending_hedge(
        market_id=market["market_id"], market_slug=market["slug"],
        asset=asset, window=window,
        leg1_side=side, leg1_shares=shares, leg1_price=ep, leg1_cost=cost,
        leg1_order_id=order_id, wallet_key=wallet_key, chat_id=chat_id,
        opp_ask_at_entry=opp_ask, spread_at_entry=round(ep + opp_ask, 3),
        time_left_at_entry=market["time_left_sec"],
    )
    if not hedge_id:
        return "❌ DB save failed (position opened — fix manually)"

    start_monitor(
        hedge_id=hedge_id, market=market, leg1_token=token, opp_token=opp_token,
        leg1_side=side, leg1_shares=shares, leg1_price=ep, leg1_cost=cost,
        time_left_sec=market["time_left_sec"], exec_client=exec_client,
        neg_risk=market["neg_risk"], chat_id=str(chat_id), tg_send_fn=tg_send_fn,
    )

    return (
        f"🎯 <b>Leg1 filled #{hedge_id}</b>\n"
        f"{asset} {window} | {side} {shares:.1f}sh @ {ep:.3f} = ${cost:.2f}\n"
        f"Opp ask: {opp_ask:.3f}\n"
        f"Stop-loss: ARM @ leg1 ≥ {round(ep + AUTO_HEDGE_ARM_PROFIT, 3):.3f} → "
        f"trig leg1_bid ≤ {ep:.3f} ({TRIGGER_DEBOUNCE_SEC:.0f}s debounce)\n"
        f"  hedge BE ≤ {round(1.0 - ep - FEE_BUFFER, 3):.3f} | "
        f"loss ≤ {round(1.0 - ep - FEE_BUFFER + MAX_HEDGE_LOSS_PER_SHARE, 3):.3f} | "
        f"else dump leg1\n"
        f"TL: {market['time_left_sec']}s\n"
        f"\n/hedge — захедж зараз | /sell — продати"
    )


async def do_arb_hedge(exec_client, wallet_key, chat_id, tg_send_fn=None) -> str:
    """Manual hedge: cancel monitor, single FAK on opp, restart monitor in LEG2_OPEN."""
    from bot.storage import get_latest_pending_hedge, mark_hedge_filled

    pending = get_latest_pending_hedge(chat_id, wallet_key)
    if not pending:
        return "❌ Немає активного leg1"

    hedge_id = pending["id"]
    await cancel_monitor(hedge_id)

    info = await _refetch_market_tokens(pending["market_slug"])
    if not info:
        return "❌ Маркет недоступний"
    token_up, token_down, neg_risk, time_left_sec = info

    leg1_side = pending["leg1_side"]
    opp_side = "DOWN" if leg1_side == "UP" else "UP"
    opp_token = token_down if leg1_side == "UP" else token_up
    leg1_token = token_up if leg1_side == "UP" else token_down

    opp_ask, _ = await _fetch_book(opp_token)
    if opp_ask <= 0:
        return f"❌ Немає ask на {opp_side}"

    target = round(opp_ask + FAK_SLIPPAGE, 3)
    leg1_shares = pending["leg1_shares"]
    leg1_cost = pending["leg1_cost"]
    leg1_price = pending["leg1_price"]

    logger.info("MANUAL HEDGE #%d: opp=%s ask=%.3f → FAK @ %.3f shares=%.1f",
                hedge_id, opp_side, opp_ask, target, leg1_shares)

    result, reason = await _safe_fak_buy(
        exec_client, token=opp_token, price=target, shares=leg1_shares,
        neg_risk=neg_risk, market_slug=pending["market_slug"], market_id=pending["market_id"],
    )
    if not result:
        return f"❌ Hedge failed: {html.escape(reason or '?')}"

    ep = float(result.get("_effective_price", target))
    shares = float(result.get("_order_size", leg1_shares))
    cost = round(shares * ep, 2)
    order_id = str(result.get("orderID") or result.get("order_id") or "")

    mark_hedge_filled(hedge_id, opp_side, shares, ep, cost, order_id)

    total = leg1_cost + cost
    min_payout = min(leg1_shares, shares) * 1.0
    max_payout = max(leg1_shares, shares) * 1.0

    market = {
        "market_id": pending["market_id"], "slug": pending["market_slug"],
        "token_up": token_up, "token_down": token_down, "neg_risk": neg_risk,
    }
    start_monitor(
        hedge_id=hedge_id, market=market, leg1_token=leg1_token, opp_token=opp_token,
        leg1_side=leg1_side, leg1_shares=leg1_shares, leg1_price=leg1_price, leg1_cost=leg1_cost,
        time_left_sec=time_left_sec, exec_client=exec_client, neg_risk=neg_risk,
        chat_id=str(chat_id), tg_send_fn=tg_send_fn,
        initial_state=HedgeState.LEG2_OPEN,
        leg2_shares=shares, leg2_price=ep, leg2_cost=cost,
    )

    return (
        f"🛡 <b>Hedge filled #{hedge_id}</b>\n"
        f"Leg1: {leg1_side} {leg1_shares:.1f}sh = ${leg1_cost:.2f}\n"
        f"Leg2: {opp_side} {shares:.1f}sh @ {ep:.3f} = ${cost:.2f}\n"
        f"Total: ${total:.2f}\n"
        f"Expected: ${min_payout - total:+.2f} (worst) / ${max_payout - total:+.2f} (best)\n"
        f"👁 Auto-exit ON (≥{EXIT_THRESHOLD:.2f})"
    )


async def do_arb_sell(exec_client, wallet_key, chat_id, tg_send_fn=None) -> str:
    """Sell all open legs of latest active hedge at current bid."""
    from bot.storage import get_latest_active_hedge, mark_hedge_sold

    hedge = get_latest_active_hedge(chat_id, wallet_key)
    if not hedge:
        return "❌ Немає активних позицій"

    hedge_id = hedge["id"]
    await cancel_monitor(hedge_id)

    info = await _refetch_market_tokens(hedge.get("market_slug") or "")
    if not info:
        return "❌ Маркет недоступний"
    token_up, token_down, _, _ = info

    leg1_side = hedge["leg1_side"]
    leg1_token = token_up if leg1_side == "UP" else token_down
    leg1_shares = float(hedge.get("leg1_shares") or 0)
    leg1_cost = float(hedge.get("leg1_cost") or 0)

    leg2_side = hedge.get("leg2_side")
    leg2_token = token_down if leg1_side == "UP" else token_up
    leg2_shares = float(hedge.get("leg2_shares") or 0)
    leg2_cost = float(hedge.get("leg2_cost") or 0)

    _, bid_up = await _fetch_book(token_up)
    _, bid_down = await _fetch_book(token_down)
    leg1_bid = bid_up if leg1_side == "UP" else bid_down
    leg2_bid = bid_down if leg1_side == "UP" else bid_up

    async def _sell(token, bid, shares, label):
        if shares <= 0:
            return 0.0, f"{label}: skip (0 shares)"
        if bid <= 0:
            return 0.0, f"{label}: skip (no bid)"
        res, reason = await _safe_fak_sell(exec_client, token=token, price=bid, shares=shares)
        if not res:
            return 0.0, f"❌ {label} sell FAIL: {html.escape(reason or '?')}"
        try:
            avg = float(res.get("average_price") or res.get("price") or bid)
            sz = float(res.get("size") or res.get("_order_size") or shares)
            got = round(avg * sz, 2)
        except Exception:
            avg, sz, got = bid, shares, round(bid * shares, 2)
        return got, f"{label} {sz:.1f}sh @ {avg:.3f} = ${got:.2f}"

    lines = []
    total_received = 0.0

    got1, line1 = await _sell(leg1_token, leg1_bid, leg1_shares, leg1_side)
    total_received += got1
    lines.append(line1)

    has_leg2 = bool(leg2_side) and leg2_shares > 0
    if has_leg2:
        got2, line2 = await _sell(leg2_token, leg2_bid, leg2_shares, leg2_side)
        total_received += got2
        lines.append(line2)

    mark_hedge_sold(hedge_id)
    total_cost = leg1_cost + (leg2_cost if has_leg2 else 0)
    pnl = round(total_received - total_cost, 2)

    return (
        f"💸 <b>Sell #{hedge_id}</b>\n" + "\n".join(lines)
        + f"\nReceived: ${total_received:.2f} | Cost: ${total_cost:.2f}\n"
          f"<b>PnL: ${pnl:+.2f}</b>"
    )
