import logging

from bot.config import LIVE_TRADING

logger = logging.getLogger(__name__)


class BotState:
    """Minimal state for manual arb/hedge bot: pause/resume only."""

    def __init__(self):
        self._live_enabled = LIVE_TRADING
        self._paused = False

    @property
    def is_live_allowed(self) -> bool:
        return self._live_enabled and not self._paused

    def pause(self):
        self._paused = True
        logger.info("Trading PAUSED via /stop")

    def resume(self):
        self._paused = False
        logger.info("Trading RESUMED via /start")

    @property
    def is_paused(self) -> bool:
        return self._paused

    def get_thresholds(self) -> dict:
        """Used by ExecutionClient cross-spread cap. Single fixed profile."""
        return {"CONTRACT_PRICE_MAX": 0.99}


state = BotState()
