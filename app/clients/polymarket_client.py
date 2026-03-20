"""
Project: QF5214 Polymarket Data Pipeline
File: polymarket_client.py
Author: Xu
Created: 2026-03-14

Description:
Polymarket client using PMXT SDK
"""

import pmxt

from app.logging.logger import setup_logger


logger = setup_logger(__name__)


class PolymarketClient:
    """
    Polymarket client using PMXT SDK.
    """

    def __init__(self) -> None:
        logger.info("Initializing PolymarketClient with PMXT SDK")
        self.client = pmxt.Polymarket()

    def fetch_markets(self, **kwargs):
        """
        Fetch markets from Polymarket.

        Examples of kwargs:
            query="Trump"
            limit=100
            offset=0
            slug="will-trump-win"
        """
        logger.info("Fetching markets from Polymarket")
        return self.client.fetch_markets(**kwargs)

    def fetch_market(self, **kwargs):
        """
        Fetch a single market from Polymarket.

        Examples of kwargs:
            market_id="663583"
            slug="will-trump-win"
            outcome_id="..."
        """
        logger.info("Fetching single market from Polymarket")
        return self.client.fetch_market(**kwargs)

    def fetch_order_book(self, outcome_id: str):
        """
        Fetch current order book for one outcome.
        """
        logger.info("Fetching order book for outcome_id=%s", outcome_id)
        return self.client.fetch_order_book(outcome_id)

    def fetch_ohlcv(self, outcome_id: str, **kwargs):
        """
        Fetch historical OHLCV for one outcome.

        Examples of kwargs:
            resolution="1m"
            limit=100
        """
        logger.info("Fetching OHLCV for outcome_id=%s", outcome_id)
        return self.client.fetch_ohlcv(outcome_id, **kwargs)

    def fetch_trades(self, outcome_id: str, **kwargs):
        """
        Fetch trade history for one outcome.
        """
        logger.info("Fetching trades for outcome_id=%s", outcome_id)
        return self.client.fetch_trades(outcome_id, **kwargs)

    def watch_order_book(self, outcome_id: str, limit: float | None = None):
        """
        Watch order book updates in real time.
        """
        logger.info("Watching order book for outcome_id=%s", outcome_id)
        return self.client.watch_order_book(outcome_id, limit=limit)

    def watch_trades(self, outcome_id: str, **kwargs):
        """
        Watch trade updates in real time.
        """
        logger.info("Watching trades for outcome_id=%s", outcome_id)
        return self.client.watch_trades(outcome_id, **kwargs)


polymarket_client = PolymarketClient()


if __name__ == "__main__":
    markets = polymarket_client.fetch_markets(query="Trump", limit=5)
    print(markets)
