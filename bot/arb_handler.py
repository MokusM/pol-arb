"""
Arb/Hedge assistant — buys cheap side, then lets user trigger hedge.

Commands:
  /arb  — inline menu with buttons per asset/window
  /hedge — buys opposite side of last pending leg1
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx

logger = logging.getLogger(__name__)

GAMMA = "https://gamma-api.polymarket.com"
ET = ZoneInfo("America/New_York")

ASSETS = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "XRP": "xrp", "DOGE": "doge"}
WINDOWS_MIN = {"5m": 5, "15m": 15}


def _current_unix_start(window_min: int) -> int:
    now = datetime.now(ET)
    minute_floor = (now.minute // window_min) * window_min
    base = now.replace(minute=minute_floor, second=0, microsecond=0)
    return int(base.timestamp())


async def find_current_market(asset: str, window: str) -> dict | None:
    """Fetch current active {asset}-updown-{window}-{unix} market from Gamma."""
    asset_slug = ASSETS.get(asset.upper())
    window_min = WINDOWS_MIN.get(window)
    if not asset_slug or not window_min:
        return None

    async with httpx.AsyncClient(timeout=5) as c:
        # Try current + next window
        for offset in (0, window_min):
            unix = _current_unix_start(window_min)
            if offset:
                unix += 60 * offset
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
                    try:
                        tokens = json.loads(tokens)
                    except Exception:
                        continue
                if len(tokens) < 2:
                    continue
                end_iso = m.get("endDate") or ""
                try:
                    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                    tl = (end_dt - datetime.now(timezone.utc)).total_seconds()
                except Exception:
                    tl = 0
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
                logger.debug("find_market %s %s: %s", asset, window, e)
    return None


async def _fetch_book(token: str) -> tuple[float, float]:
    """Return (best_ask, best_bid) for a token."""
    try:
        async with httpx.AsyncClient(timeout=3) as c:
            r = await c.get("https://clob.polymarket.com/book", params={"token_id": token})
            if r.status_code != 200:
                return 0.0, 0.0
            book = r.json()
            asks = [float(a["price"]) for a in (book.get("asks") or []) if float(a.get("price", 0)) > 0]
            bids = [float(b["price"]) for b in (book.get("bids") or []) if float(b.get("price", 0)) > 0]
            return (min(asks) if asks else 0.0, max(bids) if bids else 0.0)
    except Exception:
        return 0.0, 0.0


async def get_clob_asks(token_up: str, token_down: str) -> tuple[float, float]:
    """Return (ask_up, ask_down) from CLOB orderbook."""
    (ask_up, _), (ask_down, _) = await asyncio.gather(
        _fetch_book(token_up), _fetch_book(token_down),
    )
    return ask_up, ask_down


async def get_clob_bids(token_up: str, token_down: str) -> tuple[float, float]:
    """Return (bid_up, bid_down) from CLOB orderbook."""
    (_, bid_up), (_, bid_down) = await asyncio.gather(
        _fetch_book(token_up), _fetch_book(token_down),
    )
    return bid_up, bid_down


# ── Auto-exit monitor via WebSocket ──

EXIT_THRESHOLD = 1.03  # sell both when bid_up + bid_down >= this
WS_CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
TRAILING_HEDGE_REVERSAL = 0.05  # buy when ask reverses +5c from lowest seen

_active_monitors: dict[int, asyncio.Task] = {}
_trailing_monitors: dict[int, asyncio.Task] = {}


async def _trailing_hedge_monitor(
    hedge_id: int,
    opp_token: str,
    leg1_token: str,
    shares: float,
    max_price: float,
    initial_ask: float,
    leg1_price: float,
    time_left_sec: int,
    exec_client,
    market: dict,
    neg_risk: bool,
    chat_id: str,
    tg_send_fn,
):
    """WS monitor: buy opposite at BE price, but only after ask was below BE first."""
    import websockets

    # BE price = 1.0 - leg1_price - fee buffer
    FEE_BUFFER = 0.05
    be_price = round(1.0 - leg1_price - FEE_BUFFER, 2)
    trigger_price = min(max_price, be_price)
    has_been_below = False  # only arm after 1.5s below BE
    armed_since = None
    triggered = False

    # Auto-sell leg1 before expiry if unhedged
    AUTO_SELL_BEFORE_EXPIRY = 60  # seconds
    expiry_time = datetime.now(timezone.utc) + timedelta(seconds=time_left_sec)

    logger.info("TRAILING HEDGE #%d: BE trigger=%.3f (1.0 - %.3f - %.2f) | ask=%.3f | below=%s | expiry in %ds",
                hedge_id, trigger_price, leg1_price, FEE_BUFFER, initial_ask, has_been_below, time_left_sec)

    async def _do_buy(price):
        """Execute hedge buy and handle result."""
        result = await _buy_with_retry(
            exec_client,
            token_id=opp_token, price=price, size=shares,
            neg_risk=neg_risk,
            market_slug=market["slug"], market_id=market["market_id"],
            order_type="FAK", use_exact_price=True,
        )
        if not result or not isinstance(result, dict) or result.get("success") is False:
            logger.warning("TRAILING #%d buy failed: %s", hedge_id,
                           result.get("error") if isinstance(result, dict) else "?")
            return False

        ep = float(result.get("_effective_price", price))
        filled_shares = float(result.get("_order_size", shares))
        cost = round(filled_shares * ep, 2)
        oid = str(result.get("orderID") or result.get("order_id") or "")

        from bot.storage import get_latest_pending_hedge, mark_hedge_filled
        pending = get_latest_pending_hedge(chat_id)
        if not pending or pending["id"] != hedge_id:
            return False

        leg1_side = pending["leg1_side"]
        opp_side = "DOWN" if leg1_side == "UP" else "UP"
        mark_hedge_filled(hedge_id, opp_side, filled_shares, ep, cost, oid)

        leg1_cost = pending.get("leg1_cost", 0) or 0
        total = leg1_cost + cost
        profit = min(pending.get("leg1_shares", 0), filled_shares) * 1.0 - total

        logger.info("TRAILING HEDGE #%d FILLED: %s @ %.3f profit=$%.2f", hedge_id, opp_side, ep, profit)
        if tg_send_fn:
            await tg_send_fn(chat_id,
                f"🛡 <b>Auto-hedge #{hedge_id}</b> (BE trigger)\n"
                f"{opp_side} {filled_shares:.1f}sh @ {ep:.3f} = ${cost:.2f}\n"
                f"Total: ${total:.2f} | <b>Profit: ${profit:+.2f}</b>"
            )

        # Start exit monitor
        if leg1_side == "UP":
            sh_up, sh_down = pending["leg1_shares"], filled_shares
        else:
            sh_up, sh_down = filled_shares, pending["leg1_shares"]
        token_up = leg1_token if leg1_side == "UP" else opp_token
        token_down = opp_token if leg1_side == "UP" else leg1_token
        if tg_send_fn:
            start_exit_monitor(hedge_id, token_up, token_down,
                sh_up, sh_down, total, exec_client, chat_id, tg_send_fn)
        return True

    try:
        sub_msg = json.dumps({"type": "market", "assets_ids": [opp_token]})

        async with websockets.connect(WS_CLOB_URL, ping_interval=10, ping_timeout=5) as ws:
            await ws.send(sub_msg)

            # Seed with REST ask
            ask, _ = await _fetch_book(opp_token)
            if ask > 0 and ask < trigger_price:
                has_been_below = True

            async for raw in ws:
                try:
                    msgs = json.loads(raw)
                    if not isinstance(msgs, list):
                        msgs = [msgs]

                    for m in msgs:
                        current_ask = None

                        # Book update
                        if m.get("asset_id") == opp_token and "asks" in m:
                            asks_list = [float(a.get("price", 0)) for a in (m.get("asks") or []) if float(a.get("price", 0)) > 0]
                            if asks_list:
                                current_ask = min(asks_list)

                        # Price changes
                        for ch in m.get("price_changes") or []:
                            if ch.get("asset_id") == opp_token and (ch.get("side") or "").upper() == "SELL":
                                ba = ch.get("best_ask")
                                if ba:
                                    current_ask = float(ba)

                        if current_ask is None:
                            continue

                        # No auto-hedge by reversal — user hedges manually
                        # Only auto-sell leg1 before expiry (handled below)

                except Exception:
                    continue

                # Check expiry — auto-sell leg1 if running out of time
                now = datetime.now(timezone.utc)
                secs_left = (expiry_time - now).total_seconds()
                if secs_left <= AUTO_SELL_BEFORE_EXPIRY:
                    # Auto-sell only if leg1 lost value (bid < half of entry)
                    _, leg1_bid = await _fetch_book(leg1_token)
                    half_entry = leg1_price / 2
                    if leg1_bid <= half_entry and leg1_bid > 0.01:
                        logger.info("TRAILING #%d: %ds to expiry, bid %.3f < half entry %.3f — auto-sell",
                                    hedge_id, int(secs_left), leg1_bid, half_entry)
                    elif secs_left <= 30 and leg1_bid > 0.01:
                        # Last 30s — sell regardless
                        logger.info("TRAILING #%d: %ds to expiry — force sell", hedge_id, int(secs_left))
                    else:
                        continue  # leg1 still has value or no bid, wait

                    if leg1_bid > 0.01:
                        sell_result = await exec_client.sell_shares(
                            leg1_token, leg1_bid, shares,
                        )
                        sell_ok = isinstance(sell_result, dict) and sell_result.get("success") is not False
                        received = 0
                        if sell_ok:
                            try:
                                avg_p = float(sell_result.get("average_price") or sell_result.get("price") or leg1_bid)
                                sz = float(sell_result.get("size") or sell_result.get("_order_size") or shares)
                                received = round(avg_p * sz, 2)
                            except Exception:
                                received = round(leg1_bid * shares, 2)

                        leg1_cost = round(leg1_price * shares, 2)
                        loss = round(received - leg1_cost, 2)
                        if tg_send_fn:
                            await tg_send_fn(chat_id,
                                f"⏱ <b>Auto-sell leg1 #{hedge_id}</b> (expiry)\n"
                                f"Sold @ {leg1_bid:.3f} | Got ${received:.2f}\n"
                                f"<b>Loss: ${loss:+.2f}</b> (vs full loss ${-leg1_cost:.2f})"
                            )
                        logger.info("TRAILING #%d auto-sell: bid=%.3f received=$%.2f loss=$%.2f",
                                    hedge_id, leg1_bid, received, loss)
                    else:
                        if tg_send_fn:
                            await tg_send_fn(chat_id,
                                f"⏱ <b>Leg1 #{hedge_id} expiry</b> — no bid, settlement"
                            )
                    return

    except asyncio.CancelledError:
        logger.info("TRAILING HEDGE #%d cancelled", hedge_id)
    except Exception as e:
        logger.error("TRAILING HEDGE #%d error: %s", hedge_id, e)
    finally:
        _trailing_monitors.pop(hedge_id, None)


def start_trailing_hedge(hedge_id: int, opp_token: str, leg1_token: str,
                         shares: float, max_price: float, initial_ask: float,
                         leg1_price: float, time_left_sec: int,
                         exec_client, market: dict, neg_risk: bool,
                         chat_id: str, tg_send_fn):
    """Start background trailing hedge monitor."""
    if hedge_id in _trailing_monitors:
        return
    task = asyncio.create_task(
        _trailing_hedge_monitor(
            hedge_id, opp_token, leg1_token, shares, max_price, initial_ask,
            leg1_price, time_left_sec, exec_client, market, neg_risk, chat_id, tg_send_fn,
        )
    )
    _trailing_monitors[hedge_id] = task


def cancel_trailing_hedge(hedge_id: int):
    """Cancel trailing hedge monitor (when user manually hedges)."""
    task = _trailing_monitors.pop(hedge_id, None)
    if task and not task.done():
        task.cancel()
        logger.info("Trailing hedge #%d cancelled (manual hedge)", hedge_id)


def cancel_exit_monitor(hedge_id: int):
    """Cancel auto-exit WS monitor (when user manually sells)."""
    task = _active_monitors.pop(hedge_id, None)
    if task and not task.done():
        task.cancel()
        logger.info("Exit monitor #%d cancelled (manual sell)", hedge_id)


async def _buy_with_retry(exec_client, max_retries: int = 4, delay: float = 1.0, **kwargs) -> dict | None:
    """Buy shares with aggressive retry on CLOB 500 errors."""
    for attempt in range(max_retries):
        result = await exec_client.buy_shares(**kwargs)
        if result and isinstance(result, dict):
            if result.get("success") is not False:
                return result
            err = result.get("error", "")
            if "could not run the execution" in err:
                logger.warning("CLOB 500 retry %d/%d", attempt + 1, max_retries)
                await asyncio.sleep(delay)
                continue
            return result  # non-retryable error
        return result
    return result  # last attempt result


async def monitor_and_exit(
    hedge_id: int,
    token_up: str, token_down: str,
    shares_up: float, shares_down: float,
    total_cost: float,
    exec_client,
    chat_id: str,
    tg_send_fn,
):
    """WebSocket monitor: sell both sides when bid_up + bid_down > threshold."""
    import websockets

    logger.info("EXIT MONITOR #%d WS started: threshold=%.2f", hedge_id, EXIT_THRESHOLD)

    # Track best bids from WS updates
    best_bids = {token_up: 0.0, token_down: 0.0}

    def _update_bids_from_book(token: str, bids_list: list):
        prices = [float(b.get("price", 0)) for b in bids_list if float(b.get("price", 0)) > 0]
        if prices:
            best_bids[token] = max(prices)

    def _check_exit() -> bool:
        b_up = best_bids.get(token_up, 0)
        b_down = best_bids.get(token_down, 0)
        if b_up <= 0 or b_down <= 0:
            return False
        return b_up + b_down >= EXIT_THRESHOLD

    try:
        sub_msg = json.dumps({
            "type": "market",
            "assets_ids": [token_up, token_down],
        })

        async with websockets.connect(WS_CLOB_URL, ping_interval=10, ping_timeout=5) as ws:
            await ws.send(sub_msg)
            logger.info("EXIT WS #%d connected, watching %s + %s", hedge_id, token_up[:12], token_down[:12])

            # Also seed with REST bids
            bid_up, bid_down = await get_clob_bids(token_up, token_down)
            best_bids[token_up] = bid_up
            best_bids[token_down] = bid_down

            async for raw in ws:
                try:
                    msgs = json.loads(raw)
                    if not isinstance(msgs, list):
                        msgs = [msgs]

                    for m in msgs:
                        # Book snapshot
                        asset_id = m.get("asset_id")
                        if asset_id in best_bids and "bids" in m:
                            _update_bids_from_book(asset_id, m.get("bids") or [])

                        # Price changes
                        for ch in m.get("price_changes") or []:
                            ch_id = ch.get("asset_id")
                            if ch_id not in best_bids:
                                continue
                            side = (ch.get("side") or "").upper()
                            if side == "BUY":
                                bb = ch.get("best_bid")
                                if bb:
                                    best_bids[ch_id] = float(bb)

                except Exception:
                    continue

                # Check exit condition
                if _check_exit():
                    b_up = best_bids[token_up]
                    b_down = best_bids[token_down]
                    bid_sum = b_up + b_down
                    sell_value = b_up * shares_up + b_down * shares_down
                    settle_profit = max(shares_up, shares_down) * 1.00 - total_cost

                    logger.info(
                        "EXIT TRIGGER #%d: bid_sum=%.3f (%.3f+%.3f)",
                        hedge_id, bid_sum, b_up, b_down,
                    )

                    # Sell both sides sequentially — if first fails, don't sell second
                    async def _sell_side(tok, price, sh):
                        """Sell with retry: live→cancel+retry@0.01, min_size→skip."""
                        res = await exec_client.sell_shares(tok, price, sh)
                        if not res or not isinstance(res, dict):
                            return res
                        # Handle "live" status
                        if (res.get("status") or "").lower() == "live":
                            oid = str(res.get("orderID") or res.get("order_id") or "")
                            if oid:
                                try:
                                    await exec_client.cancel_order(oid)
                                except Exception:
                                    pass
                            res = await exec_client.sell_shares(tok, 0.01, sh)
                        return res

                    def _is_ok(res):
                        return (isinstance(res, dict)
                                and res.get("success") is not False
                                and (res.get("status") or "").lower() != "live")

                    # Sell first side
                    res_up = await _sell_side(token_up, b_up, shares_up)
                    up_ok = _is_ok(res_up)

                    if not up_ok:
                        # First side failed — don't sell second, wait for settlement
                        logger.warning("EXIT #%d: UP sell FAILED — skip DOWN sell, wait settlement", hedge_id)
                        await tg_send_fn(chat_id,
                            f"⚠️ <b>Exit #{hedge_id} partial fail</b>\n"
                            f"UP sell failed — DOWN NOT sold\n"
                            f"Settlement close position"
                        )
                        continue  # stay in WS loop, don't mark sold_early

                    # First side OK — sell second
                    res_down = await _sell_side(token_down, b_down, shares_down)
                    down_ok = _is_ok(res_down)

                    if not down_ok:
                        # Second failed but first already sold — log it
                        logger.warning("EXIT #%d: DOWN sell FAILED after UP sold", hedge_id)

                    actual = 0
                    for res in [res_up, res_down]:
                        if isinstance(res, dict) and res.get("success") is not False:
                            try:
                                avg_p = float(res.get("average_price") or res.get("price") or 0)
                                sz = float(res.get("size") or res.get("_order_size") or 0)
                                if avg_p > 0 and sz > 0:
                                    actual += avg_p * sz
                            except Exception:
                                pass
                    if actual <= 0:
                        actual = sell_value

                    real_profit = actual - total_cost

                    msg = (
                        f"💰 <b>Early exit #{hedge_id}</b>\n"
                        f"bid_sum={bid_sum:.3f} ({b_up:.3f}+{b_down:.3f})\n"
                        f"Sold: UP {'ok' if up_ok else 'FAIL'} | DOWN {'ok' if down_ok else 'FAIL'}\n"
                        f"Received: ~${actual:.2f} | Cost: ${total_cost:.2f}\n"
                        f"<b>Profit: ${real_profit:+.2f}</b> (vs settlement ${settle_profit:+.2f})"
                    )
                    await tg_send_fn(chat_id, msg)

                    from bot.storage import get_connection
                    try:
                        conn = get_connection()
                        conn.execute("UPDATE pending_hedges SET status='sold_early' WHERE id=?", (hedge_id,))
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass

                    logger.info("EXIT DONE #%d: profit=$%.2f", hedge_id, real_profit)
                    return

    except asyncio.CancelledError:
        logger.info("EXIT MONITOR #%d cancelled", hedge_id)
    except Exception as e:
        logger.error("EXIT MONITOR #%d error: %s — fallback to settlement", hedge_id, e)
    finally:
        _active_monitors.pop(hedge_id, None)


def start_exit_monitor(
    hedge_id: int,
    token_up: str, token_down: str,
    shares_up: float, shares_down: float,
    total_cost: float,
    exec_client,
    chat_id: str,
    tg_send_fn,
):
    """Start background exit monitor for a hedged position."""
    if hedge_id in _active_monitors:
        return
    task = asyncio.create_task(
        monitor_and_exit(
            hedge_id, token_up, token_down,
            shares_up, shares_down, total_cost,
            exec_client, chat_id, tg_send_fn,
        )
    )
    _active_monitors[hedge_id] = task


ARB_LIMIT_OFFSET = 0.04  # leg1: ask - 4c (passive maker, catch dips)
HEDGE_LIMIT_OFFSET = 0.03  # hedge: ask - 3c (aggressive maker, catch bounce)
HEDGE_GTC_TIMEOUT = 2.0  # seconds to wait for GTC fill before FAK fallback


async def do_arb_buy(
    asset: str, window: str, stake_usd: float,
    exec_client, wallet_key: str, chat_id: str,
    tg_send_fn=None,
) -> str:
    """Buy cheap side with GTC limit (ask - offset). Save pending hedge for later."""
    from bot.storage import save_pending_hedge

    market = await find_current_market(asset, window)
    if not market:
        return f"❌ Не знайдено активний {asset} {window} маркет"

    # Check if already have open leg1 on this market
    from bot.storage import get_latest_pending_hedge
    existing = get_latest_pending_hedge(chat_id, wallet_key)
    if existing and existing.get("market_id") == market["market_id"]:
        return f"⚠️ Вже є leg1 на цьому ринку (#{existing['id']}). /hedge або чекай новий ринок."

    ask_up, ask_down = await get_clob_asks(market["token_up"], market["token_down"])
    if ask_up <= 0 or ask_down <= 0:
        return f"❌ Немає ліквідності (ask_up={ask_up:.2f}, ask_down={ask_down:.2f})"

    # Pick cheap side
    if ask_up <= ask_down:
        side, token_id, price, opposite_ask = "UP", market["token_up"], ask_up, ask_down
    else:
        side, token_id, price, opposite_ask = "DOWN", market["token_down"], ask_down, ask_up

    # Entry filters
    if price < 0.35:
        return f"⚠️ Skip: leg1 {price:.2f} too cheap (min 0.35)"
    if market['time_left_sec'] < 120:
        return f"⚠️ Skip: {market['time_left_sec']}s left — not enough time"

    # GTC limit: ask - offset
    ARB_LIMIT_UP_OFFSET = 0.02  # second limit above ask
    limit_low = max(0.01, round(price - ARB_LIMIT_OFFSET, 2))
    limit_high = round(price + ARB_LIMIT_UP_OFFSET, 2)

    logger.info("ARB BUY BRACKET: %s %s cheap=%s ask=%.3f low=%.3f high=%.3f opp=%.3f stake=$%.2f",
                asset, window, side, price, limit_low, limit_high, opposite_ask, stake_usd)

    # Buy 5.5 shares (5 + fee buffer) so after CLOB fee we have >= 5.0
    ARB_SHARES = 5.5

    # Sequential bracket: place low first, if instant fill → done. If live → place high too.
    result = None
    ep = 0
    shares = 0
    order_id = ""

    # Step 1: try low limit
    res_low = await _buy_with_retry(
        exec_client, token_id=token_id, price=limit_low,
        size=ARB_SHARES, neg_risk=market["neg_risk"],
        market_slug=market["slug"], market_id=market["market_id"],
        order_type="GTC", use_exact_price=True,
    )

    st_low = ((res_low or {}).get("status") or "").lower() if isinstance(res_low, dict) else ""
    oid_low = str((res_low or {}).get("orderID") or (res_low or {}).get("order_id") or "") if isinstance(res_low, dict) else ""

    if st_low == "matched":
        result = res_low
        logger.info("ARB leg1 low FILLED instantly @ %.3f", limit_low)
    elif st_low == "live":
        # Step 2: low is in book, place high too
        res_high = await _buy_with_retry(
            exec_client, token_id=token_id, price=limit_high,
            size=ARB_SHARES, neg_risk=market["neg_risk"],
            market_slug=market["slug"], market_id=market["market_id"],
            order_type="GTC", use_exact_price=True,
        )
        st_high = ((res_high or {}).get("status") or "").lower() if isinstance(res_high, dict) else ""
        oid_high = str((res_high or {}).get("orderID") or (res_high or {}).get("order_id") or "") if isinstance(res_high, dict) else ""

        if st_high == "matched":
            # High filled — cancel low
            result = res_high
            logger.info("ARB leg1 high FILLED, cancelling low %s", oid_low[:12])
            try: await exec_client.cancel_order(oid_low)
            except Exception: pass
        else:
            # Both live — poll for fill
            live_orders = [("low", oid_low, limit_low)]
            if st_high == "live" and oid_high:
                live_orders.append(("high", oid_high, limit_high))

            logger.info("ARB leg1: %d live orders, waiting for fill", len(live_orders))
            filled = False
            for _ in range(int(min(market['time_left_sec'] - 15, 60))):
                await asyncio.sleep(1)
                for tag, oid, lim in live_orders:
                    info = await exec_client.get_order_status(oid)
                    s = (info.get("status") or "").upper() if info else ""
                    if s == "MATCHED":
                        result = info
                        result["_effective_price"] = float(info.get("price") or lim)
                        result["_order_size"] = float(info.get("size_matched") or ARB_SHARES)
                        order_id = oid
                        filled = True
                        logger.info("ARB leg1 %s FILLED: %.3f", tag, lim)
                        for t2, o2, _ in live_orders:
                            if t2 != tag and o2:
                                try: await exec_client.cancel_order(o2)
                                except Exception: pass
                        break
                if filled:
                    break
            if not filled:
                for _, oid, _ in live_orders:
                    try: await exec_client.cancel_order(oid)
                    except Exception: pass
                return f"⏱ Leg1 не виконано за таймаут — скасовано"
    else:
        # Low failed
        err = (res_low or {}).get("error", "?") if isinstance(res_low, dict) else "?"
        return f"❌ Buy failed: {err}"

    if result:
        ep = float(result.get("_effective_price", limit_low))
        shares = float(result.get("_order_size", ARB_SHARES))
        order_id = order_id or str(result.get("orderID") or result.get("order_id") or "")

    if shares <= 0 and ep > 0:
        shares = round(stake_usd / ep, 2)
    cost = round(shares * ep, 2)

    # Determine opposite token for safety order
    opp_token = market["token_down"] if side == "UP" else market["token_up"]
    spread = round(ep + opposite_ask, 3)

    # SOL price analytics — disabled in arb-only build
    sol_price = None

    safety_oid = ""
    safety_line = ""
    max_leg2_for_breakeven = (shares - cost) / shares if shares > 0 else 0
    MAX_HEDGE_LOSS_PER_SHARE = 0.10
    max_hedge_price = min(0.99, round(max_leg2_for_breakeven + MAX_HEDGE_LOSS_PER_SHARE, 2))

    from bot.storage import save_pending_hedge
    hedge_id = save_pending_hedge(
        market_id=market["market_id"], market_slug=market["slug"],
        asset=asset, window=window,
        leg1_side=side, leg1_shares=shares, leg1_price=ep, leg1_cost=cost,
        leg1_order_id=order_id, wallet_key=wallet_key, chat_id=chat_id,
        opp_ask_at_entry=opposite_ask, spread_at_entry=spread,
        sol_price_at_entry=sol_price, time_left_at_entry=market['time_left_sec'],
    )

    # Start trailing hedge monitor in background
    start_trailing_hedge(
        hedge_id=hedge_id,
        opp_token=opp_token,
        leg1_token=token_id,
        shares=shares,
        max_price=max_hedge_price,
        initial_ask=opposite_ask,
        leg1_price=ep,
        time_left_sec=market['time_left_sec'],
        exec_client=exec_client,
        market=market,
        neg_risk=market["neg_risk"],
        chat_id=str(chat_id),
        tg_send_fn=tg_send_fn,
    )

    max_hedge_price_str = f"{max_hedge_price:.3f}"
    saved = round((price - ep) * shares, 2)
    saved_line = f"\n💰 Saved vs FAK: ${saved:.2f}" if saved > 0.005 else ""

    return (
        f"🎯 <b>Leg1 filled #{hedge_id}</b> (GTC)\n"
        f"{asset} {window} | {side} {shares:.1f}sh @ {ep:.3f} | ${cost:.2f}{saved_line}\n"
        f"Opposite ask: {opposite_ask:.3f}\n"
        f"BE trigger: {round(1.0 - ep - 0.05, 2):.3f} (auto-hedge on reversal)\n"
        f"TL: {market['time_left_sec']}s\n"
        f"\n/hedge — купити по кращій ціні"
    )


async def do_arb_hedge(
    exec_client, wallet_key: str, chat_id: str,
    tg_send_fn=None,
) -> str:
    """Buy opposite side of last pending hedge to lock profit."""
    from bot.storage import get_latest_pending_hedge, mark_hedge_filled

    pending = get_latest_pending_hedge(chat_id, wallet_key)
    if not pending:
        return "❌ Немає активного leg1 для хеджу"

    # Cancel trailing hedge monitor (user wants manual hedge)
    cancel_trailing_hedge(pending["id"])

    # Get market tokens
    market = await find_current_market(pending["asset"], pending["window"])
    if not market or market["market_id"] != pending["market_id"]:
        # Market may have changed — refetch by slug
        async with httpx.AsyncClient(timeout=5) as c:
            try:
                r = await c.get(f"{GAMMA}/markets/slug/{pending['market_slug']}")
                if r.status_code != 200:
                    return f"❌ Маркет {pending['market_slug']} недоступний"
                m = r.json()
                tokens = m.get("clobTokenIds") or []
                if isinstance(tokens, str):
                    tokens = json.loads(tokens)
                market = {
                    "market_id": pending["market_id"],
                    "slug": pending["market_slug"],
                    "token_up": str(tokens[0]),
                    "token_down": str(tokens[1]),
                    "neg_risk": bool(m.get("negRisk", False)),
                }
            except Exception as e:
                return f"❌ Fetch market: {e}"

    leg1_side = pending["leg1_side"]
    opp_side = "DOWN" if leg1_side == "UP" else "UP"
    opp_token = market["token_down"] if leg1_side == "UP" else market["token_up"]

    ask_up, ask_down = await get_clob_asks(market["token_up"], market["token_down"])
    opp_ask = ask_down if leg1_side == "UP" else ask_up
    leg1_ask = ask_up if leg1_side == "UP" else ask_down
    if opp_ask <= 0:
        return f"❌ Немає ліквідності на {opp_side}"

    leg1_shares = pending["leg1_shares"]
    leg1_cost = pending["leg1_cost"]
    leg1_token = market["token_up"] if leg1_side == "UP" else market["token_down"]

    # Auto-scale: buy more leg1 pairs if profitable
    # Check if we can add more leg1 at current ask + current opp_ask < 1.00
    extra_pairs = 0
    extra_leg1_cost = 0
    if leg1_ask > 0 and opp_ask > 0 and leg1_ask + opp_ask < 0.95:
        # How many extra pairs can we add?
        # Budget: keep it proportional to original (max 3x original)
        max_extra = int(leg1_shares * 2)  # up to 2x more
        pair_cost = leg1_ask + opp_ask
        for i in range(1, max_extra + 1):
            if pair_cost >= 0.95:  # margin too thin
                break
            extra_pairs = i
        extra_leg1_cost = round(extra_pairs * leg1_ask, 2)
        if extra_pairs > 0:
            logger.info("SCALE: +%d pairs, leg1_ask=%.3f opp_ask=%.3f pair_cost=%.3f",
                        extra_pairs, leg1_ask, opp_ask, pair_cost)
            # Buy extra leg1 shares
            scale_result = await _buy_with_retry(
                exec_client,
                token_id=leg1_token, price=leg1_ask,
                stake_usd=extra_leg1_cost,
                neg_risk=market["neg_risk"],
                market_slug=market["slug"], market_id=market["market_id"],
            )
            if scale_result and isinstance(scale_result, dict) and scale_result.get("success") is not False:
                sc_shares = float(scale_result.get("_order_size", 0))
                sc_price = float(scale_result.get("_effective_price", leg1_ask))
                if sc_shares <= 0 and sc_price > 0:
                    sc_shares = round(extra_leg1_cost / sc_price, 2)
                extra_leg1_cost = round(sc_shares * sc_price, 2)
                leg1_shares += sc_shares
                leg1_cost += extra_leg1_cost
                logger.info("SCALE OK: +%.1f shares @ %.3f = $%.2f, total leg1: %.1f shares $%.2f",
                            sc_shares, sc_price, extra_leg1_cost, leg1_shares, leg1_cost)
            else:
                extra_pairs = 0
                extra_leg1_cost = 0
                logger.info("SCALE FAIL: couldn't buy extra leg1")

    # Hedge: buy opposite side matching total leg1 shares
    hedge_shares = leg1_shares
    be_price = 1.0 - (leg1_cost / hedge_shares) if hedge_shares > 0 else 0
    # Max price: allow hedge at small loss to cap downside
    # max_loss_per_share = how much above breakeven we allow
    MAX_HEDGE_LOSS_PER_SHARE = 0.10  # allow up to $0.10/share loss = $0.50 on 5 shares
    max_price = min(0.99, round(be_price + MAX_HEDGE_LOSS_PER_SHARE, 2))

    # Bracket: two GTC limits — one below ask (catch dip), one above (catch jump)
    limit_low = max(0.01, round(opp_ask - HEDGE_LIMIT_OFFSET, 2))
    limit_high = min(round(max_price, 2), round(opp_ask + HEDGE_LIMIT_OFFSET, 2))
    if limit_high <= limit_low:
        limit_high = limit_low  # only one order

    logger.info("HEDGE BRACKET: opp=%s ask=%.3f low=%.3f high=%.3f be=%.3f shares=%.1f",
                opp_side, opp_ask, limit_low, limit_high, be_price, hedge_shares)

    # Place both orders in parallel — use exact size (not stake) to match leg1 shares
    results = await asyncio.gather(
        _buy_with_retry(exec_client, token_id=opp_token, price=limit_low,
                        size=hedge_shares, neg_risk=market["neg_risk"],
                        market_slug=market["slug"], market_id=market["market_id"],
                        order_type="GTC", use_exact_price=True),
        _buy_with_retry(exec_client, token_id=opp_token, price=limit_high,
                        size=hedge_shares, neg_risk=market["neg_risk"],
                        market_slug=market["slug"], market_id=market["market_id"],
                        order_type="GTC", use_exact_price=True) if limit_high > limit_low else asyncio.sleep(0),
        return_exceptions=True,
    )

    res_low = results[0] if not isinstance(results[0], Exception) else None
    res_high = results[1] if len(results) > 1 and not isinstance(results[1], Exception) and limit_high > limit_low else None

    oid_low = str((res_low or {}).get("orderID") or (res_low or {}).get("order_id") or "") if isinstance(res_low, dict) else ""
    oid_high = str((res_high or {}).get("orderID") or (res_high or {}).get("order_id") or "") if isinstance(res_high, dict) else ""

    status_low = ((res_low or {}).get("status") or "").lower() if isinstance(res_low, dict) else ""
    status_high = ((res_high or {}).get("status") or "").lower() if isinstance(res_high, dict) else ""

    # Check if any filled instantly
    result = None
    filled_tag = ""
    if status_low == "matched":
        result = res_low
        filled_tag = "low"
        if oid_high:
            await exec_client.cancel_order(oid_high)
    elif status_high == "matched":
        result = res_high
        filled_tag = "high"
        if oid_low:
            await exec_client.cancel_order(oid_low)

    # If neither filled instantly — poll both for HEDGE_GTC_TIMEOUT seconds
    if not result:
        live_orders = []
        if status_low == "live" and oid_low:
            live_orders.append(("low", oid_low, limit_low))
        if status_high == "live" and oid_high:
            live_orders.append(("high", oid_high, limit_high))

        if live_orders:
            logger.info("HEDGE BRACKET: %d live orders, polling %.0fs", len(live_orders), HEDGE_GTC_TIMEOUT)
            poll_steps = max(1, int(HEDGE_GTC_TIMEOUT / 0.5))
            for _ in range(poll_steps):
                await asyncio.sleep(0.5)
                for tag, oid, lim in live_orders:
                    info = await exec_client.get_order_status(oid)
                    s = (info.get("status") or "").upper() if info else ""
                    if s == "MATCHED":
                        result = info
                        result["_effective_price"] = float(info.get("price") or lim)
                        result["_order_size"] = float(info.get("size_matched") or hedge_shares)
                        filled_tag = tag
                        logger.info("HEDGE BRACKET %s FILLED @ %.3f", tag, lim)
                        break
                if result:
                    break

            # Cancel unfilled orders
            for tag, oid, lim in live_orders:
                if tag != filled_tag and oid:
                    try:
                        await exec_client.cancel_order(oid)
                    except Exception:
                        pass

        # If still no fill — FAK fallback
        if not result:
            for tag, oid, lim in live_orders:
                try:
                    await exec_client.cancel_order(oid)
                except Exception:
                    pass

            logger.info("HEDGE BRACKET timeout — FAK fallback")
            fresh_ask = await exec_client.get_token_price(opp_token, "SELL")
            if fresh_ask and fresh_ask > 0 and fresh_ask <= max_price:
                opp_ask = fresh_ask
            elif fresh_ask and fresh_ask > max_price:
                max_loss = round((fresh_ask - be_price) * hedge_shares, 2)
                return (
                    f"⚠️ Hedge ask {fresh_ask:.3f} > max {max_price:.3f}\n"
                    f"Loss would be ${max_loss:.2f}. Leg1 directional."
                )

            result = await _buy_with_retry(
                exec_client,
                token_id=opp_token, price=opp_ask, size=hedge_shares,
                neg_risk=market["neg_risk"],
                market_slug=market["slug"], market_id=market["market_id"],
                order_type="FAK",
            )
            filled_tag = "fak"

    if not result or result.get("success") is False:
        err = result.get("error", "?") if isinstance(result, dict) else "?"
        return f"❌ Hedge failed: {err}"

    ep = float(result.get("_effective_price", opp_ask))
    shares = float(result.get("_order_size", 0))
    if shares <= 0:
        shares = hedge_shares
    cost = round(shares * ep, 2)
    order_id = str(result.get("orderID") or result.get("order_id") or "")

    mark_hedge_filled(pending["id"], opp_side, shares, ep, cost, order_id)

    total = leg1_cost + cost
    min_payout = min(pending["leg1_shares"], shares) * 1.00
    max_payout = max(pending["leg1_shares"], shares) * 1.00

    # Start auto-exit monitor
    if tg_send_fn:
        # Determine which token has which shares
        if leg1_side == "UP":
            sh_up, sh_down = pending["leg1_shares"], shares
        else:
            sh_up, sh_down = shares, pending["leg1_shares"]
        start_exit_monitor(
            hedge_id=pending["id"],
            token_up=market["token_up"],
            token_down=market["token_down"],
            shares_up=sh_up, shares_down=sh_down,
            total_cost=total,
            exec_client=exec_client,
            chat_id=str(chat_id),
            tg_send_fn=tg_send_fn,
        )

    scale_line = ""
    if extra_pairs > 0:
        scale_line = f"\n📈 Scaled: +{extra_pairs} pairs (+${extra_leg1_cost:.2f} leg1)"

    return (
        f"🛡 <b>Hedge filled #{pending['id']}</b>\n"
        f"Leg1: {leg1_side} {leg1_shares:.1f}sh (orig {pending['leg1_shares']:.0f}+{extra_pairs}) = ${leg1_cost:.2f}\n"
        f"Leg2: {opp_side} {shares:.1f}sh @ {ep:.3f} = ${cost:.2f}\n"
        f"Total cost: ${total:.2f}{scale_line}\n"
        f"Expected: ${min_payout - total:+.2f} (worst) / ${max_payout - total:+.2f} (best)\n"
        f"👁 Auto-exit monitor ON (threshold {EXIT_THRESHOLD:.2f})"
    )


async def do_arb_sell(
    exec_client, wallet_key: str, chat_id: str,
    tg_send_fn=None,
) -> str:
    """Sell all legs of latest active hedge at current bid. 1 leg → 1 sell, 2 legs → 2 sells."""
    from bot.storage import get_latest_active_hedge, mark_hedge_sold

    hedge = get_latest_active_hedge(chat_id, wallet_key)
    if not hedge:
        return "❌ Немає активних позицій"

    hedge_id = hedge["id"]
    market_slug = hedge.get("market_slug") or ""

    # Stop background monitors
    cancel_trailing_hedge(hedge_id)
    cancel_exit_monitor(hedge_id)

    # Refetch market tokens via slug (market_id-based fetch requires live market)
    async with httpx.AsyncClient(timeout=5) as c:
        try:
            r = await c.get(f"{GAMMA}/markets/slug/{market_slug}")
            if r.status_code != 200:
                return f"❌ Маркет {market_slug} недоступний"
            m = r.json()
            tokens = m.get("clobTokenIds") or []
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            if len(tokens) < 2:
                return "❌ Маркет без токенів"
            token_up, token_down = str(tokens[0]), str(tokens[1])
        except Exception as e:
            return f"❌ Fetch market: {e}"

    leg1_side = hedge.get("leg1_side") or ""
    leg1_token = token_up if leg1_side == "UP" else token_down
    leg1_shares = float(hedge.get("leg1_shares") or 0)
    leg1_cost = float(hedge.get("leg1_cost") or 0)

    leg2_side = hedge.get("leg2_side")
    leg2_token = token_down if leg1_side == "UP" else token_up
    leg2_shares = float(hedge.get("leg2_shares") or 0)
    leg2_cost = float(hedge.get("leg2_cost") or 0)

    bid_up, bid_down = await get_clob_bids(token_up, token_down)
    leg1_bid = bid_up if leg1_side == "UP" else bid_down
    leg2_bid = bid_down if leg1_side == "UP" else bid_up

    async def _sell(token: str, bid: float, shares: float, label: str) -> tuple[float, str]:
        if shares <= 0:
            return 0.0, f"{label}: skip (0 shares)"
        if bid <= 0:
            return 0.0, f"{label}: skip (no bid)"
        res = await exec_client.sell_shares(token, bid, shares)
        if not isinstance(res, dict) or res.get("success") is False:
            err = res.get("error", "?") if isinstance(res, dict) else "?"
            return 0.0, f"❌ {label} FAIL: {err}"
        try:
            avg = float(res.get("average_price") or res.get("price") or bid)
            sz = float(res.get("size") or res.get("_order_size") or shares)
            got = round(avg * sz, 2)
        except Exception:
            avg, sz, got = bid, shares, round(bid * shares, 2)
        return got, f"{label} {sz:.1f}sh @ {avg:.3f} = ${got:.2f}"

    lines: list[str] = []
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
        f"💸 <b>Sell #{hedge_id}</b>\n"
        + "\n".join(lines)
        + f"\nReceived: ${total_received:.2f} | Cost: ${total_cost:.2f}\n"
          f"<b>PnL: ${pnl:+.2f}</b>"
    )
