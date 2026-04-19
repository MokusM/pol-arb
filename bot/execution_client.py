"""
Polymarket CLOB execution client — buy/sell shares on binary markets.

Uses py-clob-client for authenticated order placement on clob.polymarket.com.
Gamma API (gamma-api.polymarket.com) is read-only — used only for market data.
"""

import asyncio
import logging
import math
import os
from typing import Optional

from bot.config import (
    POLYMARKET_PRIVATE_KEY,
    POLYMARKET_CHAIN_ID,
    CLOB_CROSS_SPREAD_BUY,
    CLOB_MAX_BUY_SLIPPAGE_ABS,
    CLOB_BUY_BUFFER,
    CLOB_TRADE_HISTORY_LIMIT,
    CLOB_TRADE_HISTORY_MAX_PAGES,
)

logger = logging.getLogger(__name__)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import (
        OrderArgs, OrderType, BalanceAllowanceParams, AssetType,
        PartialCreateOrderOptions,
    )
    CLOB_AVAILABLE = True
except ImportError:
    CLOB_AVAILABLE = False
    logger.warning("py-clob-client не встановлено: pip install py-clob-client")

CLOB_HOST = "https://clob.polymarket.com"


def trade_timestamp(tr: dict) -> float:
    """Unix time для сортування угод CLOB (різні ключі в різних версіях API)."""
    for k in ("match_time", "timestamp", "created_at", "last_update"):
        v = tr.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


class ExecutionClient:
    """Обгортка CLOB client для розміщення ордерів на Polymarket."""

    def __init__(self, private_key: str | None = None, funder_address: str | None = None):
        self.client: Optional["ClobClient"] = None
        self._initialized = False
        self.not_ready_reason: str | None = None

        _key = private_key or POLYMARKET_PRIVATE_KEY

        if not CLOB_AVAILABLE:
            self.not_ready_reason = (
                "немає py-clob-client у цьому Python (pip install для того ж python, що запускає main.py)"
            )
            logger.error("py-clob-client не доступний — live trading вимкнено")
            return

        if not _key:
            self.not_ready_reason = "POLYMARKET_PRIVATE_KEY порожній у .env"
            logger.warning("POLYMARKET_PRIVATE_KEY не задано — live trading вимкнено")
            return

        try:
            self.sig_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "2"))
            funder = funder_address or os.environ.get("POLYMARKET_FUNDER_ADDRESS") or None

            temp = ClobClient(
                host=CLOB_HOST,
                chain_id=POLYMARKET_CHAIN_ID,
                key=_key,
            )
            creds = temp.create_or_derive_api_creds()
            logger.info("API creds derived: %s...", creds.api_key[:16])

            self.client = ClobClient(
                host=CLOB_HOST,
                chain_id=POLYMARKET_CHAIN_ID,
                key=_key,
                creds=creds,
                signature_type=self.sig_type,
                funder=funder,
            )
            self._initialized = True
            self.not_ready_reason = None
            logger.info(
                "ExecutionClient init OK (chain=%s, sig_type=%s, funder=%s)",
                POLYMARKET_CHAIN_ID, self.sig_type, funder or "N/A",
            )
        except Exception as e:
            self.not_ready_reason = f"init: {e}"[:300]
            logger.error("Помилка ініціалізації ExecutionClient: %s", e, exc_info=True)

    @property
    def ready(self) -> bool:
        return self._initialized and self.client is not None

    async def get_clob_minimum_order_size(
        self,
        market_slug: str = "",
        market_id: str | None = None,
    ) -> float:
        """
        Мінімальний розмір ордера в shares з CLOB (окремо від мінімуму ~$1 notional).
        """
        import httpx

        condition_id: str | None = None
        try:
            async with httpx.AsyncClient() as client:
                if market_slug:
                    r = await client.get(
                        f"https://gamma-api.polymarket.com/markets/slug/{market_slug}"
                    )
                    if r.status_code == 200:
                        condition_id = r.json().get("conditionId")
                if not condition_id and market_id:
                    r = await client.get(
                        f"https://gamma-api.polymarket.com/markets/{market_id}"
                    )
                    if r.status_code == 200:
                        condition_id = r.json().get("conditionId")
                if not condition_id:
                    return 1.0
                r2 = await client.get(f"{CLOB_HOST}/markets/{condition_id}")
                if r2.status_code != 200:
                    return 1.0
                mos = r2.json().get("minimum_order_size")
                return float(mos) if mos is not None else 1.0
        except Exception as e:
            logger.warning("get_clob_minimum_order_size: %s", e)
            return 1.0

    async def get_balance(self) -> float:
        """Повернути USDC баланс з CLOB (COLLATERAL)."""
        if not self.ready:
            return 0.0
        try:
            loop = asyncio.get_event_loop()
            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=self.sig_type,
            )
            result = await loop.run_in_executor(
                None, self.client.get_balance_allowance, params,
            )
            if isinstance(result, dict):
                raw = result.get("balance", "0")
                return float(raw) / 1e6
            return 0.0
        except Exception as e:
            logger.error("Помилка get_balance: %s", e)
            return 0.0

    def _fetch_trades_pages_sync(self, max_pages: int) -> list[dict]:
        """
        GET /data/trades з пагінацією (Level 2). Повний get_trades() у клієнті тягне всю історію — не використовуємо.
        """
        from py_clob_client.clob_types import RequestArgs
        from py_clob_client.headers.headers import create_level_2_headers
        from py_clob_client.http_helpers.helpers import get
        from py_clob_client.client import TRADES, add_query_trade_params, END_CURSOR

        c = self.client
        request_args = RequestArgs(method="GET", request_path=TRADES)
        headers = create_level_2_headers(c.signer, c.creds, request_args)
        all_rows: list[dict] = []
        next_cursor: str | None = "MA=="
        pages = 0
        while (
            next_cursor
            and next_cursor != END_CURSOR
            and pages < max(1, max_pages)
        ):
            url = add_query_trade_params(
                "{}{}".format(c.host, TRADES), None, next_cursor,
            )
            response = get(url, headers=headers)
            if not isinstance(response, dict):
                break
            all_rows.extend(response.get("data") or [])
            next_cursor = response.get("next_cursor")
            pages += 1
        return all_rows

    async def get_recent_trades(
        self,
        limit: int | None = None,
        max_pages: int | None = None,
    ) -> list[dict]:
        """
        Останні угоди акаунта з CLOB (як у веб-історії для цього гаманця / API keys).
        """
        if not self.ready:
            return []
        lim = limit if limit is not None else CLOB_TRADE_HISTORY_LIMIT
        if lim <= 0:
            return []
        pages = max_pages if max_pages is not None else CLOB_TRADE_HISTORY_MAX_PAGES
        try:
            loop = asyncio.get_event_loop()
            rows = await loop.run_in_executor(
                None,
                lambda: self._fetch_trades_pages_sync(pages),
            )
            rows.sort(key=trade_timestamp, reverse=True)
            return rows[:lim]
        except Exception as e:
            logger.error("Помилка get_recent_trades: %s", e, exc_info=True)
            return []

    def _resolve_buy_limit_price_sync(self, token_id: str, reference: float) -> tuple[float, str | None]:
        """
        Лімітна ціна BUY: ask не вище CONTRACT_PRICE_MAX (та сама верхня межа, що й у сканері).
        CLOB_MAX_BUY_SLIPPAGE_ABS — лише попередження в лог, якщо ринок пішов від сигналу, але ще в зоні курсу.
        """
        tick_s = self.client.get_tick_size(token_id)
        tick = float(tick_s) if tick_s else 0.01
        if tick <= 0:
            tick = 0.01
        self._last_tick_size = f"{tick:.10f}".rstrip("0").rstrip(".")

        def tick_up(p: float) -> float:
            steps = math.ceil(p / tick - 1e-12)
            return min(0.99, round(steps * tick, 6))

        ref_lim = tick_up(reference)

        if not CLOB_CROSS_SPREAD_BUY:
            return ref_lim, None

        from bot.state import state
        hard_cap = min(0.99, state.get_thresholds()["CONTRACT_PRICE_MAX"])

        # Safety check: reference already above cap — reject even before CLOB probe
        if reference > hard_cap + 1e-9:
            return (
                0.0,
                f"price_moved:{reference:.2f}:{hard_cap:.2f}",
            )

        try:
            raw = self.client.get_price(token_id, "SELL")
            ap = (
                float(raw.get("price", 0))
                if isinstance(raw, dict)
                else float(raw or 0)
            )
        except Exception as ex:
            logger.warning("cross-spread BUY: %s — fallback to reference (capped)", ex)
            # Fallback: use reference but enforce hard cap
            return min(ref_lim, hard_cap), None

        if ap <= 0:
            return min(ref_lim, hard_cap), None

        if ap > hard_cap + 1e-9:
            return (
                0.0,
                f"price_moved:{ap:.2f}:{hard_cap:.2f}",
            )

        if CLOB_MAX_BUY_SLIPPAGE_ABS > 0:
            soft = reference + CLOB_MAX_BUY_SLIPPAGE_ABS
            if ap > soft + 1e-9:
                logger.warning(
                    "BUY: ask %.4f далі від сигналу %.4f ніж +%.2f, але ask ≤ %.2f — ордер дозволено.",
                    ap, reference, CLOB_MAX_BUY_SLIPPAGE_ABS, hard_cap,
                )

        p = max(reference, min(ap + CLOB_BUY_BUFFER, hard_cap))
        return tick_up(p), None

    def _resolve_gtc_limit_price_sync(self, token_id: str, reference: float) -> tuple[float, str | None]:
        """
        GTC limit price: ask - GTC_PRICE_OFFSET.
        Skips if best ask > GTC_MAX_ENTRY_PRICE.
        Returns (limit_price, error_or_None).
        """
        from bot.config import GTC_PRICE_OFFSET, GTC_MAX_ENTRY_PRICE

        tick_s = self.client.get_tick_size(token_id)
        tick = float(tick_s) if tick_s else 0.01
        if tick <= 0:
            tick = 0.01
        self._last_tick_size = f"{tick:.10f}".rstrip("0").rstrip(".")

        def tick_down(p: float) -> float:
            steps = int(p / tick + 1e-12)
            return max(0.01, round(steps * tick, 6))

        try:
            raw = self.client.get_price(token_id, "SELL")
            ask = float(raw.get("price", 0)) if isinstance(raw, dict) else float(raw or 0)
        except Exception as ex:
            logger.warning("GTC: get_price failed: %s — using reference", ex)
            ask = reference

        if ask <= 0:
            ask = reference

        if ask > GTC_MAX_ENTRY_PRICE + 1e-9:
            return 0.0, f"gtc_skip:ask={ask:.3f}>max={GTC_MAX_ENTRY_PRICE:.2f}"

        # Limit = ask - offset, rounded down to tick
        limit_p = tick_down(ask - GTC_PRICE_OFFSET)
        if limit_p <= 0.01:
            return 0.0, f"gtc_skip:limit_too_low={limit_p:.3f}"

        logger.info("GTC price: ask=%.3f offset=%.3f -> limit=%.3f", ask, GTC_PRICE_OFFSET, limit_p)
        return limit_p, None

    async def buy_shares(
        self,
        token_id: str,
        price: float,
        size: float | None = None,
        stake_usd: float | None = None,
        neg_risk: bool = False,
        tick_size: str = "0.01",
        market_slug: str | None = None,
        market_id: str | None = None,
        order_type: str = "FAK",
        use_exact_price: bool = False,
    ) -> Optional[dict]:
        """
        Купити shares за лімітною ціною.
        order_type: "FAK" (fill-and-kill, taker) або "GTC" (good-till-cancelled, maker).
        use_exact_price: якщо True — використати price напряму, без resolver.
        price — опорна ціна з сигналу; ліміт не вище CONTRACT_PRICE_MAX; зсув від сигналу лише логується (див. CLOB_MAX_BUY_SLIPPAGE_ABS).
        Якщо передано stake_usd — кількість shares рахується від фінальної лімітної ціни (~та сама сума $).
        Два обмеження CLOB: мінімум notional ~$1 і minimum_order_size (shares) з /markets/{condition}.
        """
        if not self.ready:
            logger.error("ExecutionClient не готовий — ордер не розміщено")
            return {"success": False, "error": "ExecutionClient not ready"}

        if price <= 0:
            return {"success": False, "error": "Invalid price"}

        use_stake = stake_usd is not None and float(stake_usd) > 0
        if not use_stake:
            if size is None or float(size) <= 0:
                return {"success": False, "error": "Invalid size"}
            size = float(size)
        else:
            stake_usd = float(stake_usd)

        use_gtc = order_type.upper() == "GTC"

        try:
            loop = asyncio.get_event_loop()
            self._last_tick_size = tick_size
            if use_exact_price:
                # Use the price as-is, just resolve tick size
                limit_p = round(price, 2)
                slip_err = None
                try:
                    ts = await loop.run_in_executor(None, self.client.get_tick_size, token_id)
                    if ts:
                        self._last_tick_size = f"{float(ts):.10f}".rstrip("0").rstrip(".")
                except Exception:
                    pass
            elif use_gtc:
                limit_p, slip_err = await loop.run_in_executor(
                    None,
                    lambda: self._resolve_gtc_limit_price_sync(token_id, price),
                )
            else:
                limit_p, slip_err = await loop.run_in_executor(
                    None,
                    lambda: self._resolve_buy_limit_price_sync(token_id, price),
                )
            if slip_err:
                return {"success": False, "error": slip_err}
            tick_size = self._last_tick_size  # використовуємо реальний tick з CLOB

            if use_stake:
                size = round(stake_usd / limit_p, 2)
                if size <= 0:
                    return {"success": False, "error": "Розмір позиції після ціни = 0"}

            logger.info(
                "BUY ліміт: сигнал %.4f -> ліміт %.4f | stake_mode=%s",
                price, limit_p, use_stake,
            )

            if market_slug or market_id:
                min_sh = await self.get_clob_minimum_order_size(
                    market_slug or "", market_id,
                )
                if min_sh > 0:
                    if min_sh > size and limit_p * min_sh > stake_usd * 5:
                        # Мінімальний розмір більший ніж у 5 разів перевищує stake — відмовляємось
                        return {
                            "success": False,
                            "error": f"min_order_size {min_sh} shares (${limit_p * min_sh:.2f}) перевищує stake ${stake_usd:.2f} — ордер не розміщено",
                        }
                    size = max(size, min_sh)

            if limit_p * size < 1.0:
                need = math.ceil((1.0 / limit_p) * 100) / 100.0
                while need * limit_p < 1.0 - 1e-9:
                    need = round(need + 0.01, 2)
                size = max(size, need)

            if limit_p * size < 1.0:
                return {
                    "success": False,
                    "error": f"Сума ордера ${limit_p * size:.2f} < $1.00 (мінімум CLOB)",
                }

            # CLOB вимагає: maker (price × size) ≤ 2 decimal places, taker (size) ≤ 4 decimal.
            # Використовуємо Decimal для точних розрахунків без float noise.
            from decimal import Decimal as _Dec, ROUND_HALF_UP as _RHU
            from math import gcd as _gcd
            # Округляємо ціну до 2 знаків (tick_size=0.01) — і для GCD і для OrderArgs.
            # Без цього limit_p може мати float noise (0.5900000001) або 3+ знаки (0.591),
            # що робить GCD некоректним і бібліотека рахує maker amount з шумом.
            _price_2dp = round(limit_p, 2)
            _p = _Dec(str(_price_2dp))  # ціна рівно 2 знаки, без float noise
            _D = int(_p * 100)          # ціна в центах (ціле число, 59 для 0.59)
            if _D > 0:
                _divisor = _D // _gcd(_D, 10000)
                _target_cents = int((_p * _Dec(str(size)) * 100).to_integral_value(_RHU))
                _M_floor = (_target_cents // _divisor) * _divisor
                _M_ceil = _M_floor + _divisor
                _M = _M_ceil if abs(_M_ceil - _target_cents) < abs(_M_floor - _target_cents) else _M_floor
                if _M < 100:
                    _M = _M_ceil
                if _M >= 100:
                    size = float(_Dec(_M) / _Dec(_D))
            # Фінальне округлення до 4 знаків щоб py_clob_client не відправив float noise
            size = float(_Dec(str(size)).quantize(_Dec("0.0001"), rounding=_RHU))
            # Фінальна перевірка: maker (price × size) повинен мати ≤ 2 знаки.
            # Якщо після всіх округлень залишився шум — знайти найближчий size що задовольняє умову.
            _maker = _Dec(str(_price_2dp)) * _Dec(str(size))
            _maker_rounded = _maker.quantize(_Dec("0.01"), rounding=_RHU)
            if abs(_maker - _maker_rounded) > _Dec("0.001"):
                # Підібрати size так щоб price*size = ціле число центів
                _target_maker = int((_Dec(str(_price_2dp)) * _Dec(str(size)) * 100).to_integral_value(_RHU))
                size = float((_Dec(str(_target_maker)) / _Dec("100") / _Dec(str(_price_2dp))).quantize(_Dec("0.0001"), rounding=_RHU))

            logger.debug(
                "PRE-ORDER: price=%.4f → %.2f, size=%s, maker=%.10f",
                limit_p, _price_2dp, size, _price_2dp * size,
            )

            order_args = OrderArgs(
                token_id=token_id,
                price=_price_2dp,  # використовуємо округлену ціну — без float noise
                size=size,
                side="BUY",
            )
            options = PartialCreateOrderOptions(
                tick_size=tick_size,
                neg_risk=neg_risk,
            )

            _ot = OrderType.GTC if use_gtc else OrderType.FAK

            def _post():
                order = self.client.create_order(order_args, options)
                return self.client.post_order(order, orderType=_ot)

            signed = await loop.run_in_executor(None, _post)

            logger.info(
                "ORDER PLACED (%s): BUY %s shares @ %.2f | token=%s | result=%s",
                order_type.upper(), size, limit_p, token_id[:12], signed,
            )
            if isinstance(signed, dict):
                signed["_effective_price"] = limit_p
                signed["_order_size"] = size
                return signed
            return {
                "success": True,
                "order_id": str(signed),
                "_effective_price": limit_p,
                "_order_size": size,
            }

        except Exception as e:
            logger.error("Помилка buy_shares: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    async def sell_shares(
        self,
        token_id: str,
        price: float,
        size: float,
        neg_risk: bool = False,
        tick_size: str = "0.01",
        fallback_size: float | None = None,
    ) -> Optional[dict]:
        """Продати shares (для partial exit / stop-loss).

        fallback_size — якщо CLOB відхилив через мінімальний розмір,
        повторити з цією кількістю (зазвичай = всі remaining shares).
        """
        if not self.ready:
            logger.error("ExecutionClient не готовий — ордер не розміщено")
            return {"success": False, "error": "ExecutionClient not ready"}

        # CLOB не приймає ціну >= 1.0 — кепуємо до 0.99
        price = min(price, 0.99)

        async def _attempt(sell_size: float) -> Optional[dict]:
            try:
                loop = asyncio.get_event_loop()
                order_args = OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=sell_size,
                    side="SELL",
                )
                options = PartialCreateOrderOptions(
                    tick_size=tick_size,
                    neg_risk=neg_risk,
                )

                def _post():
                    return self.client.create_and_post_order(order_args, options)

                signed = await loop.run_in_executor(None, _post)
                logger.info(
                    "ORDER PLACED: SELL %s shares @ %.2f | token=%s | result=%s",
                    sell_size, price, token_id[:12], signed,
                )
                if isinstance(signed, dict):
                    return signed
                return {"success": True, "order_id": str(signed)}
            except Exception as e:
                return {"success": False, "error": str(e), "_exception": e}

        result = await _attempt(size)
        err_str = str(result.get("error", "")) if result else ""

        # Retry with fallback_size if CLOB minimum size rejected
        if result and result.get("success") is False and "lower than the min" in err_str:
            if fallback_size is not None and fallback_size > size:
                logger.warning(
                    "sell_shares: size %.2f нижче мінімуму CLOB — повторюємо з fallback %.2f",
                    size, fallback_size,
                )
                result = await _attempt(fallback_size)
                if result and "_exception" not in result:
                    result["_used_fallback_size"] = fallback_size
                err_str = str(result.get("error", "")) if result else ""
            else:
                # Вже продаємо все що є, але CLOB мінімум більший — settlement закриє позицію
                logger.warning(
                    "sell_shares: size %.2f нижче мінімуму CLOB і fallback недоступний — settlement закриє позицію",
                    size,
                )
                return {"success": False, "error": "below_clob_minimum", "_market_resolved": True}

        # Retry with actual on-chain balance if "not enough balance" error
        if result and result.get("success") is False and "not enough balance" in err_str:
            import re
            m = re.search(r"balance:\s*(\d+)", err_str)
            if m:
                import math as _math
                actual_size = _math.floor(int(m.group(1)) / 10_000) / 100  # floor до 2 знаків
                if actual_size < 0.5:
                    # Маркет вже резолвнувся і токени редімнули — settlement закриє позицію
                    logger.warning(
                        "sell_shares: on-chain balance %.4f < 0.5 — маркет вже резолвнувся, пропускаємо продаж",
                        actual_size,
                    )
                    return {"success": False, "error": "market_resolved", "_market_resolved": True}
                if actual_size > 0:
                    logger.warning(
                        "sell_shares: not enough balance — повторюємо з реальним балансом %.4f (запитували %.4f)",
                        actual_size, size,
                    )
                    result = await _attempt(actual_size)
                    if result and result.get("success") is True:
                        result["_used_actual_size"] = actual_size

        if result and result.get("success") is False:
            exc = result.pop("_exception", None)
            logger.error("Помилка sell_shares: %s", result.get("error"), exc_info=exc)

        return result

    async def get_market_token_ids(
        self,
        market_slug: str,
        market_id: str | None = None,
    ) -> Optional[tuple[str, str]]:
        """
        Отримати clobTokenIds (YES, NO) з Gamma API для маркету.

        Спочатку /markets/slug/{slug}; якщо slug порожній або 404 — /markets/{id}.
        """
        import httpx
        import json

        def _parse_tokens(data: dict) -> Optional[tuple[str, str]]:
            raw = data.get("clobTokenIds")
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except json.JSONDecodeError:
                    raw = None
            if isinstance(raw, list) and len(raw) >= 2:
                return (str(raw[0]), str(raw[1]))
            return None

        try:
            async with httpx.AsyncClient() as client:
                if market_slug:
                    r = await client.get(
                        f"https://gamma-api.polymarket.com/markets/slug/{market_slug}"
                    )
                    if r.status_code == 200:
                        data = r.json()
                        tokens = _parse_tokens(data)
                        if tokens:
                            return tokens

                if market_id:
                    r2 = await client.get(
                        f"https://gamma-api.polymarket.com/markets/{market_id}"
                    )
                    if r2.status_code == 200:
                        data2 = r2.json()
                        tokens = _parse_tokens(data2)
                        if tokens:
                            return tokens
                        logger.warning(
                            "Gamma markets/%s: немає clobTokenIds у відповіді",
                            market_id,
                        )
        except Exception as e:
            logger.error(
                "Помилка get_market_token_ids(slug=%s, id=%s): %s",
                market_slug, market_id, e,
            )
        return None

    async def get_token_price(self, token_id: str, side: str = "BUY") -> float | None:
        """Поточна ціна token через CLOB. Повертає None при помилці API, 0.0 якщо ціна справді 0."""
        if not self.ready:
            return None
        loop = asyncio.get_event_loop()
        for attempt in range(3):
            try:
                result = await loop.run_in_executor(
                    None, self.client.get_price, token_id, side,
                )
                if isinstance(result, dict):
                    return float(result.get("price", 0))
                return float(result) if result is not None else None
            except Exception as e:
                if attempt == 2:
                    logger.debug("get_token_price(%s) failed після 3 спроб: %s", token_id[:12], e)
                    return None
                await asyncio.sleep(1.0)

    async def get_order_status(self, order_id: str) -> dict:
        """Повертає статус ордера з CLOB. Поля: status, size_matched, price."""
        if not self.ready:
            return {}
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self.client.get_order, order_id)
            if isinstance(result, dict):
                return result
            return {}
        except Exception as e:
            logger.debug("get_order_status(%s): %s", order_id[:12], e)
            return {}

    async def cancel_order(self, order_id: str) -> bool:
        """Скасовує ордер у CLOB. Повертає True якщо успішно."""
        if not self.ready:
            return False
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.client.cancel, order_id)
            logger.info("Ордер %s скасовано", order_id[:12])
            return True
        except Exception as e:
            logger.warning("cancel_order(%s): %s", order_id[:12], e)
            return False
