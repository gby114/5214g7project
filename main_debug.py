"""
Project: QF5214 Polymarket Data Pipeline
File: main_debug.py

Description:
Unified debug entry point for manual testing.
"""

import pandas as pd
from datetime import datetime
from app.logging.logger import setup_logger
from app.clients.polymarket_client import polymarket_client
from app.clients.kafka_client import KafkaProducerClient, KafkaConsumerClient
from app.services.aggregation_service import aggregation_service
from app.clients.backfill_client import backfill_client
from app.services.ingestion_service import ingestion_service

logger = setup_logger(__name__)


logger.info(f"Starting debug at {datetime.utcnow()}")

# markets = polymarket_client.fetch_markets(query="Trump", limit=5)
# print(markets)


# logger.info("Kafka client demo started")
# sample_topic = "polymarket.market_metadata"
# sample_payloads = [
#     {
#         "market_id": "m_001",
#         "price": 0.52,
#         "event_time": "2026-03-20T12:00:00Z",
#     },
#     {
#         "market_id": "m_002",
#         "price": 0.61,
#         "event_time": "2026-03-20T12:00:05Z",
#     },
# ]

# # Producer sample
# with KafkaProducerClient() as producer:
#     producer.send(
#         topic=sample_topic,
#         value=sample_payloads[0],
#         key="m_001",
#     )
#     producer.send_batch(
#         topic=sample_topic,
#         values=sample_payloads,
#         key_field="market_id",
#     )
#     producer.flush()

# with KafkaConsumerClient(
#     topic="polymarket.market_metadata",
#     group_id="polymarket_market_consumer",
#     auto_offset_reset="earliest",
#     enable_auto_commit=False,
# ) as consumer:
#     result = consumer.consume(
#         max_messages_per_partition=100,
#         max_workers=3,
#     )
#     print(result)

# logger.info("Kafka client demo finished")

# aggregation_service.aggregate_test_raw_data()


# start = datetime(2026, 3, 19, 22, 0, 0)
# end = datetime(2026, 3, 20, 2, 0, 0)

# urls = backfill_client.build_orderbook_urls(
#     start_time=start,
#     end_time=end,
# )

# print("Generated URLs:")
# for u in urls:
#     print(u)

# first_url = urls[0]

# with backfill_client.download_as_tempfile(first_url) as temp_path:
#     print(f"Temp parquet file: {temp_path}")
#     df = pd.read_parquet(temp_path)
#     print(df.head())


ingestion_service.injest_backfill_data()
