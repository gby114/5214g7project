import sys
sys.path.insert(0, '.')
import pandas as pd
from app.clients.clickhouse_client import ClickHouseClient
from app.services.aggregation_service import AggregationService

client = ClickHouseClient()

# ─────────────────────────────────────────
# STEP 1 - Insert fake raw market data
# ─────────────────────────────────────────
print("=" * 60)
print("STEP 1 - Inserting fake market snapshot data...")
from datetime import datetime

fake_market_rows = [
    {"source": "pmxt", "exchange": "polymarket", "market_id": "market_001", "captured_at": datetime(2026, 3, 22, 8, 0, 0), "resolution_date": "2026-12-31", "volume24h": 10000.0, "volume": 50000.0, "liquidity": 25000.0, "open_interest": 0.0, "fetch_params_json": "{}"},
    {"source": "pmxt", "exchange": "polymarket", "market_id": "market_001", "captured_at": datetime(2026, 3, 22, 8, 30, 0), "resolution_date": "2026-12-31", "volume24h": 12000.0, "volume": 52000.0, "liquidity": 26000.0, "open_interest": 0.0, "fetch_params_json": "{}"},
    {"source": "pmxt", "exchange": "polymarket", "market_id": "market_002", "captured_at": datetime(2026, 3, 22, 8, 0, 0), "resolution_date": "2026-12-31", "volume24h": 5000.0, "volume": 20000.0, "liquidity": 10000.0, "open_interest": 0.0, "fetch_params_json": "{}"},
    {"source": "pmxt", "exchange": "polymarket", "market_id": "market_002", "captured_at": datetime(2026, 3, 22, 8, 30, 0), "resolution_date": "2026-12-31", "volume24h": 6000.0, "volume": 22000.0, "liquidity": 11000.0, "open_interest": 0.0, "fetch_params_json": "{}"},
]
client.insert_rows(table="polymarket_market_snapshot", rows=fake_market_rows, column_names=["source", "exchange", "market_id", "captured_at", "resolution_date", "volume24h", "volume", "liquidity", "open_interest", "fetch_params_json"])
print("Done!")

# ─────────────────────────────────────────
# STEP 2 - Insert fake outcome data
# ─────────────────────────────────────────
print("\nSTEP 2 - Inserting fake outcome snapshot data...")
fake_outcome_rows = [
    {"source": "pmxt", "exchange": "polymarket", "market_id": "market_001", "outcome_id": "outcome_yes_001", "label": "Yes", "captured_at": datetime(2026, 3, 22, 8, 0, 0), "price": 0.60, "price_change24h": 0.05, "metadata_json": "{}"},
    {"source": "pmxt", "exchange": "polymarket", "market_id": "market_001", "outcome_id": "outcome_yes_001", "label": "Yes", "captured_at": datetime(2026, 3, 22, 8, 30, 0), "price": 0.65, "price_change24h": 0.05, "metadata_json": "{}"},
    {"source": "pmxt", "exchange": "polymarket", "market_id": "market_001", "outcome_id": "outcome_no_001", "label": "No", "captured_at": datetime(2026, 3, 22, 8, 0, 0), "price": 0.40, "price_change24h": -0.05, "metadata_json": "{}"},
    {"source": "pmxt", "exchange": "polymarket", "market_id": "market_001", "outcome_id": "outcome_no_001", "label": "No", "captured_at": datetime(2026, 3, 22, 8, 30, 0), "price": 0.35, "price_change24h": -0.05, "metadata_json": "{}"},
    {"source": "pmxt", "exchange": "polymarket", "market_id": "market_002", "outcome_id": "outcome_yes_002", "label": "Yes", "captured_at": datetime(2026, 3, 22, 8, 0, 0), "price": 0.75, "price_change24h": 0.10, "metadata_json": "{}"},
    {"source": "pmxt", "exchange": "polymarket", "market_id": "market_002", "outcome_id": "outcome_yes_002", "label": "Yes", "captured_at": datetime(2026, 3, 22, 8, 30, 0), "price": 0.80, "price_change24h": 0.10, "metadata_json": "{}"},
]
client.insert_rows(table="polymarket_outcome_snapshot", rows=fake_outcome_rows, column_names=["source", "exchange", "market_id", "outcome_id", "label", "captured_at", "price", "price_change24h", "metadata_json"])
print("Done!")

# ─────────────────────────────────────────
# STEP 3 - Run aggregations
# ─────────────────────────────────────────
print("\nSTEP 3 - Running aggregations...")
AggregationService.aggregate_placeholder1_data()
AggregationService.aggregate_placeholder2_data()
AggregationService.aggregate_placeholder3_data()
AggregationService.aggregate_placeholder4_data()
print("Done!")

# ─────────────────────────────────────────
# STEP 4 - Show results as tables
# ─────────────────────────────────────────
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.float_format', '{:.3f}'.format)

print("\n" + "=" * 60)
print("FACT TABLE: fact_market_snapshot_hourly")
print("=" * 60)
rows = client.query_rows("""
    SELECT market_id, hour_bucket, avg_volume24h, max_volume24h,
           min_volume24h, avg_volume, avg_liquidity, snapshot_count
    FROM fact_market_snapshot_hourly
    WHERE market_id IN ('market_001', 'market_002')
    ORDER BY market_id, hour_bucket DESC
""")
df = pd.DataFrame(rows)
print(df.to_string(index=False))

print("\n" + "=" * 60)
print("FACT TABLE: fact_outcome_snapshot_hourly")
print("=" * 60)
rows2 = client.query_rows("""
    SELECT market_id, outcome_id, label, hour_bucket,
           open_price, close_price, high_price, low_price,
           avg_price, snapshot_count
    FROM fact_outcome_snapshot_hourly
    WHERE market_id IN ('market_001', 'market_002')
    ORDER BY market_id, outcome_id, hour_bucket DESC
""")
df2 = pd.DataFrame(rows2)
print(df2.to_string(index=False))

print("\n" + "=" * 60)
print("FACT TABLE: fact_market_snapshot_daily")
print("=" * 60)
rows3 = client.query_rows("""
    SELECT market_id, day_bucket, avg_volume24h,
           max_volume24h, avg_liquidity, snapshot_count
    FROM fact_market_snapshot_daily
    WHERE market_id IN ('market_001', 'market_002')
    ORDER BY market_id, day_bucket DESC
""")
df3 = pd.DataFrame(rows3)
print(df3.to_string(index=False))

print("\n" + "=" * 60)
print("DIM TABLE: dim_outcome (fake data only)")
print("=" * 60)
rows4 = client.query_rows("""
    SELECT outcome_id, market_id, label
    FROM dim_outcome
    WHERE market_id IN ('market_001', 'market_002')
""")
df4 = pd.DataFrame(rows4)
print(df4.to_string(index=False))

print("\n" + "=" * 60)
print("DIM TABLE: dim_source")
print("=" * 60)
rows5 = client.query_rows("""
    SELECT source_id, source, exchange
    FROM dim_source
    LIMIT 10
""")
df5 = pd.DataFrame(rows5)
print(df5.to_string(index=False))

print("\n" + "=" * 60)
print("ALL TESTS COMPLETE!")
print("=" * 60)
