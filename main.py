import asyncio
import logging
import logging.handlers
import os
import sys

_log_file = os.path.join(os.path.dirname(__file__), "bot.log")
_file_handler = logging.handlers.RotatingFileHandler(
    _log_file, maxBytes=5 * 1024 * 1024, backupCount=2, encoding="utf-8"
)
_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), _file_handler],
)
logger = logging.getLogger(__name__)

from bot.config import ARB_WALLET_KEY, LIVE_TRADING
from bot.storage import init_pending_hedges_table
from bot.telegram_bot import (
    set_execution_client,
    set_execution_clients,
    start_telegram_polling,
)


async def main():
    logger.info("Python: %s", sys.executable)

    logger.info("Ініціалізація БД (live.db)...")
    init_pending_hedges_table()

    execution_client = None
    execution_clients: dict = {}

    if LIVE_TRADING:
        from bot.execution_client import ExecutionClient

        # Main wallet (default POLYMARKET_PRIVATE_KEY) — optional
        execution_client = ExecutionClient()
        set_execution_client(execution_client)
        if execution_client.ready:
            logger.info("🟢 Main wallet ready")
            execution_clients["POLYMARKET_PRIVATE_KEY"] = execution_client
        else:
            logger.warning(
                "⚠️ Main wallet not ready: %s",
                getattr(execution_client, "not_ready_reason", "unknown"),
            )

        # Arb wallet (V2 by default)
        arb_key = os.getenv(ARB_WALLET_KEY)
        arb_funder = os.getenv(ARB_WALLET_KEY.replace("PRIVATE_KEY", "FUNDER_ADDRESS"))
        if arb_key:
            try:
                ec_arb = ExecutionClient(private_key=arb_key, funder_address=arb_funder)
                if ec_arb.ready:
                    execution_clients[ARB_WALLET_KEY] = ec_arb
                    logger.info("🟢 Arb wallet (%s) ready", ARB_WALLET_KEY)
                else:
                    logger.warning("⚠️ Arb wallet not ready: %s", ec_arb.not_ready_reason)
            except Exception as e:
                logger.warning("⚠️ Arb wallet init error: %s", e)
        else:
            logger.warning("⚠️ %s not set in .env — /arb and /hedge disabled", ARB_WALLET_KEY)
    else:
        logger.info("📋 LIVE_TRADING=false — ордери не розміщуються")

    set_execution_clients(execution_clients)
    logger.info("🚀 Запуск Arb/Hedge Bot...")

    tasks = [asyncio.create_task(start_telegram_polling())]

    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                logger.error("Задача %d впала: %s", i, res, exc_info=res)
    except KeyboardInterrupt:
        logger.info("Зупинка бота (KeyboardInterrupt).")
    finally:
        for t in tasks:
            t.cancel()
        logger.info("Бот зупинено.")


if __name__ == "__main__":
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
