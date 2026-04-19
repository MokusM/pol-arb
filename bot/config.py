import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# ── Secrets ───────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN            = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID                   = os.getenv("CHAT_ID", "")
POLYMARKET_PRIVATE_KEY    = os.getenv("POLYMARKET_PRIVATE_KEY", "")
POLYMARKET_CHAIN_ID       = int(os.getenv("POLYMARKET_CHAIN_ID", "137"))
POLYMARKET_SIGNATURE_TYPE = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "2"))
POLYMARKET_FUNDER_ADDRESS = os.getenv("POLYMARKET_FUNDER_ADDRESS", "")

# ── Instance settings ─────────────────────────────────────────────────────────
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() in ("1", "true", "yes")
LIVE_TRADING     = os.getenv("LIVE_TRADING", "false").lower() in ("1", "true", "yes")

# ── Arb wallet (defaults to V2) ───────────────────────────────────────────────
ARB_WALLET_KEY = os.getenv("ARB_WALLET_KEY", "POLYMARKET_PRIVATE_KEY_V2")

# ── CLOB execution (used by ExecutionClient) ──────────────────────────────────
CLOB_CROSS_SPREAD_BUY        = os.getenv("CLOB_CROSS_SPREAD_BUY", "true").lower() in ("1", "true", "yes")
CLOB_MAX_BUY_SLIPPAGE_ABS    = float(os.getenv("CLOB_MAX_BUY_SLIPPAGE_ABS", "0.05"))
CLOB_BUY_BUFFER              = float(os.getenv("CLOB_BUY_BUFFER", "0.02"))
CLOB_TRADE_HISTORY_LIMIT     = int(os.getenv("CLOB_TRADE_HISTORY_LIMIT", "10"))
CLOB_TRADE_HISTORY_MAX_PAGES = int(os.getenv("CLOB_TRADE_HISTORY_MAX_PAGES", "3"))

# ── GTC (limit) orders — used by ExecutionClient GTC path ────────────────────
GTC_PRICE_OFFSET      = float(os.getenv("GTC_PRICE_OFFSET", "0.02"))
GTC_MAX_ENTRY_PRICE   = float(os.getenv("GTC_MAX_ENTRY_PRICE", "0.95"))
GTC_ORDER_TTL_SECONDS = int(os.getenv("GTC_ORDER_TTL_SECONDS", "300"))

# ── DB paths ──────────────────────────────────────────────────────────────────
_ROOT        = Path(__file__).parent.parent
DB_PATH_LIVE = str(_ROOT / "live.db")
