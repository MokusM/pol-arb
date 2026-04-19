import logging
import sqlite3
from datetime import datetime, timezone

from bot.config import DB_PATH_LIVE

logger = logging.getLogger(__name__)


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path if db_path is not None else DB_PATH_LIVE)
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_pending_hedges_table(db_path: str | None = None) -> None:
    """Table for arb/hedge strategy: track leg1 waiting for leg2."""
    path = db_path if db_path is not None else DB_PATH_LIVE
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_hedges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT DEFAULT (datetime('now')),
                market_id TEXT NOT NULL,
                market_slug TEXT,
                asset TEXT,
                window TEXT,
                leg1_side TEXT,
                leg1_shares REAL,
                leg1_price REAL,
                leg1_cost REAL,
                leg1_order_id TEXT,
                leg2_side TEXT,
                leg2_shares REAL,
                leg2_price REAL,
                leg2_cost REAL,
                leg2_order_id TEXT,
                hedged_at TEXT,
                status TEXT DEFAULT 'open',
                wallet_key TEXT,
                chat_id TEXT,
                safety_order_id TEXT,
                safety_token_id TEXT,
                opp_ask_at_entry REAL,
                spread_at_entry REAL,
                sol_price_at_entry REAL,
                atr_at_entry REAL,
                time_left_at_entry REAL
            )
            """
        )
        conn.commit()
    except Exception as e:
        logger.error("init_pending_hedges_table: %s", e)
    finally:
        conn.close()


def save_pending_hedge(
    market_id: str, market_slug: str, asset: str, window: str,
    leg1_side: str, leg1_shares: float, leg1_price: float, leg1_cost: float,
    leg1_order_id: str, wallet_key: str, chat_id: str,
    safety_order_id: str | None = None, safety_token_id: str | None = None,
    opp_ask_at_entry: float | None = None, spread_at_entry: float | None = None,
    sol_price_at_entry: float | None = None, atr_at_entry: float | None = None,
    time_left_at_entry: float | None = None,
) -> int | None:
    conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO pending_hedges "
            "(market_id, market_slug, asset, window, leg1_side, leg1_shares, "
            "leg1_price, leg1_cost, leg1_order_id, wallet_key, chat_id, "
            "safety_order_id, safety_token_id, opp_ask_at_entry, spread_at_entry, "
            "sol_price_at_entry, atr_at_entry, time_left_at_entry) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (market_id, market_slug, asset, window, leg1_side, leg1_shares,
             leg1_price, leg1_cost, leg1_order_id, wallet_key, chat_id,
             safety_order_id, safety_token_id, opp_ask_at_entry, spread_at_entry,
             sol_price_at_entry, atr_at_entry, time_left_at_entry),
        )
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        logger.error("save_pending_hedge: %s", e)
        return None
    finally:
        conn.close()


def update_hedge_safety_order(hedge_id: int, safety_order_id: str, safety_token_id: str) -> None:
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE pending_hedges SET safety_order_id=?, safety_token_id=? WHERE id=?",
            (safety_order_id, safety_token_id, hedge_id),
        )
        conn.commit()
    except Exception as e:
        logger.error("update_hedge_safety_order: %s", e)
    finally:
        conn.close()


def get_latest_pending_hedge(chat_id: str, wallet_key: str | None = None) -> dict | None:
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        where = "status='open' AND chat_id=?"
        params: list = [str(chat_id)]
        if wallet_key:
            where += " AND wallet_key=?"
            params.append(wallet_key)
        row = conn.execute(
            f"SELECT * FROM pending_hedges WHERE {where} ORDER BY id DESC LIMIT 1",
            params,
        ).fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.error("get_latest_pending_hedge: %s", e)
        return None
    finally:
        conn.close()


def get_latest_active_hedge(chat_id: str, wallet_key: str | None = None) -> dict | None:
    """Latest hedge in 'open' (leg1 only) or 'hedged' (leg1+leg2) state."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        where = "status IN ('open','hedged') AND chat_id=?"
        params: list = [str(chat_id)]
        if wallet_key:
            where += " AND wallet_key=?"
            params.append(wallet_key)
        row = conn.execute(
            f"SELECT * FROM pending_hedges WHERE {where} ORDER BY id DESC LIMIT 1",
            params,
        ).fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.error("get_latest_active_hedge: %s", e)
        return None
    finally:
        conn.close()


def mark_hedge_sold(hedge_id: int) -> None:
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE pending_hedges SET status='sold_early' WHERE id=?",
            (hedge_id,),
        )
        conn.commit()
    except Exception as e:
        logger.error("mark_hedge_sold: %s", e)
    finally:
        conn.close()


def mark_hedge_filled(
    hedge_id: int, leg2_side: str, leg2_shares: float,
    leg2_price: float, leg2_cost: float, leg2_order_id: str,
) -> None:
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE pending_hedges SET leg2_side=?, leg2_shares=?, leg2_price=?, "
            "leg2_cost=?, leg2_order_id=?, hedged_at=?, status='hedged' WHERE id=?",
            (leg2_side, leg2_shares, leg2_price, leg2_cost, leg2_order_id,
             datetime.now(timezone.utc).isoformat(), hedge_id),
        )
        conn.commit()
    except Exception as e:
        logger.error("mark_hedge_filled: %s", e)
    finally:
        conn.close()
