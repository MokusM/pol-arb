import logging

logger = logging.getLogger(__name__)


class BotState:
    """Minimal state holder for ExecutionClient cross-spread cap."""

    def get_thresholds(self) -> dict:
        return {"CONTRACT_PRICE_MAX": 0.99}


state = BotState()
