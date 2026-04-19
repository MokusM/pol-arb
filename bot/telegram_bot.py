"""
Telegram bot for manual Arb/Hedge trading on Polymarket SOL 5m markets.

Commands:
  /arb    — buy cheap side of current SOL 5m market (leg1)
  /hedge  — buy opposite side of last pending leg1 (leg2)
  /balance — Polymarket wallet USDC balance
  /status  — bot status
  /start   — resume trading
  /stop    — pause trading
"""

import asyncio
import html
import logging

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

from bot.config import (
    ARB_WALLET_KEY,
    CHAT_ID,
    TELEGRAM_ENABLED,
    TELEGRAM_TOKEN,
)
logger = logging.getLogger(__name__)

bot = Bot(token=TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None
dp = Dispatcher()

_execution_client = None
_execution_clients_ref: dict = {}
_hedge_lock = asyncio.Lock()


def set_execution_client(client) -> None:
    global _execution_client
    _execution_client = client


def set_execution_clients(clients: dict) -> None:
    global _execution_clients_ref
    _execution_clients_ref = clients


def _arb_client():
    return _execution_clients_ref.get(ARB_WALLET_KEY) or _execution_client


async def _tg_send(chat_id, text: str) -> None:
    if bot:
        try:
            await bot.send_message(chat_id, text, parse_mode="HTML")
        except Exception as e:
            logger.error("tg_send: %s", e)


# ── Commands ──

@dp.message(Command("list"))
async def cmd_list(message: types.Message):
    await message.answer(
        "<b>📋 Команди</b>\n"
        "\n"
        "/arb — купити дешеву сторону SOL 5m ($1, leg1)\n"
        "/hedge — купити протилежну сторону (leg2)\n"
        "/sell — продати все відкрите (leg1 або leg1+leg2)\n"
        "\n"
        "/balance — баланс USDC\n"
        "/status — стан бота\n"
        "/list — ця довідка",
        parse_mode="HTML",
    )


@dp.message(Command("balance"))
async def cmd_balance(message: types.Message):
    ec = _arb_client()
    if not ec or not ec.ready:
        await message.answer("⚠️ ExecutionClient не налаштований.")
        return
    try:
        bal = await ec.get_balance()
        await message.answer(f"💰 Баланс: <b>${bal:.2f} USDC</b>", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ {html.escape(str(e))}")


@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    ec = _arb_client()
    client_ready = bool(ec and ec.ready)

    client_icon = "🟢" if client_ready else "🔴"

    lines = [
        "🤖 <b>Arb/Hedge Bot</b>",
        "",
        f"{client_icon} Client: <b>{'READY' if client_ready else 'NOT READY'}</b>",
        f"🔑 Arb wallet key: <code>{html.escape(ARB_WALLET_KEY)}</code>",
    ]

    if client_ready:
        try:
            bal = await ec.get_balance()
            lines.append(f"💰 Wallet: <b>${bal:.2f} USDC</b>")
        except Exception as e:
            lines.append(f"💰 Wallet: — ({html.escape(str(e))})")

    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("arb"))
async def cmd_arb(message: types.Message):
    """Buy cheap side of current SOL 5m market for $1."""
    ec = _arb_client()
    if not ec or not ec.ready:
        await message.answer("❌ ExecutionClient не готовий")
        return

    from bot.arb_handler import do_arb_buy

    try:
        result = await do_arb_buy(
            "SOL", "5m", 1.0, ec, ARB_WALLET_KEY,
            str(message.chat.id), tg_send_fn=_tg_send,
        )
    except Exception as e:
        logger.error("arb_buy: %s", e, exc_info=True)
        result = f"❌ Exception: {html.escape(str(e))}"

    builder = InlineKeyboardBuilder()
    builder.button(text="🛡 Hedge", callback_data="hedge|now")
    await message.answer(result, parse_mode="HTML", reply_markup=builder.as_markup())


@dp.message(Command("hedge"))
async def cmd_hedge(message: types.Message):
    await _do_hedge(message.chat.id)


@dp.message(Command("sell"))
async def cmd_sell(message: types.Message):
    """Sell all open legs of latest active hedge at current bid."""
    ec = _arb_client()
    if not ec or not ec.ready:
        await message.answer("❌ ExecutionClient не готовий")
        return

    from bot.arb_handler import do_arb_sell

    try:
        result = await do_arb_sell(ec, ARB_WALLET_KEY, str(message.chat.id), tg_send_fn=_tg_send)
    except Exception as e:
        logger.error("sell: %s", e, exc_info=True)
        result = f"❌ Exception: {html.escape(str(e))}"
    await message.answer(result, parse_mode="HTML")


@dp.callback_query(lambda c: c.data == "hedge|now")
async def process_hedge(callback_query: types.CallbackQuery):
    try:
        await bot.answer_callback_query(callback_query.id, "Hedging...")
    except Exception:
        pass
    await _do_hedge(callback_query.message.chat.id)


async def _do_hedge(chat_id) -> None:
    if _hedge_lock.locked():
        await bot.send_message(chat_id, "⏳ Hedge вже виконується...")
        return

    async with _hedge_lock:
        ec = _arb_client()
        if not ec or not ec.ready:
            await bot.send_message(chat_id, "❌ ExecutionClient не готовий")
            return

        from bot.arb_handler import do_arb_hedge

        try:
            result = await do_arb_hedge(ec, ARB_WALLET_KEY, str(chat_id), tg_send_fn=_tg_send)
        except Exception as e:
            logger.error("hedge: %s", e, exc_info=True)
            result = f"❌ Exception: {html.escape(str(e))}"
        await bot.send_message(chat_id, result, parse_mode="HTML")


async def start_telegram_polling():
    if not TELEGRAM_ENABLED:
        logger.info("Telegram вимкнено (TELEGRAM_ENABLED=false) — polling не запускається.")
        return
    if not bot:
        logger.warning("TELEGRAM_TOKEN порожній — polling не запускається.")
        return
    while True:
        try:
            logger.info("Запуск Telegram бота (polling)...")
            await dp.start_polling(bot)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            logger.error("Telegram polling впав: %s — перезапуск через 10с", e)
            await asyncio.sleep(10)
