import sys
sys.path.insert(0, '.')
import pandas as pd
from app.clients.clickhouse_client import ClickHouseClient
from app.services.aggregation_service import AggregationService

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.float_format', '{:.3f}'.format)

client = ClickHouseClient()

# ─────────────────────────────────────────
# STEP 1 - Check real raw data
# ─────────────────────────────────────────
print("=" * 60)
print("STEP 1 - Real raw data summary")
print("=" * 60)

rows = client.query_rows("""
    SELECT
        'polymarket_market_snapshot' AS table_name,
        count()                      AS total_rows,
        min(captured_at)             AS earliest,
        max(captured_at)             AS latest
    FROM polymarket_market_snapshot
    UNION ALL
    SELECT
        'polymarket_outcome_snapshot' AS table_name,
        count()                       AS total_rows,
        min(captured_at)              AS earliest,
        max(captured_at)              AS latest
    FROM polymarket_outcome_snapshot
""")
df = pd.DataFrame(rows)
print(df.to_string(index=False))

# ─────────────────────────────────────────
# STEP 2 - Run aggregations on real data
# ─────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 2 - Running aggregations on real data...")
print("=" * 60)
AggregationService.aggregate_placeholder1_data()
AggregationService.aggregate_placeholder2_data()
AggregationService.aggregate_placeholder3_data()
AggregationService.aggregate_placeholder4_data()
AggregationService.populate_dim_market()
AggregationService.populate_dim_outcome()
AggregationService.populate_dim_source()
print("Done!")

# ─────────────────────────────────────────
# STEP 3 - Show fact_market_snapshot_hourly
# ─────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 3 - FACT: fact_market_snapshot_hourly (real data)")
print("=" * 60)
rows = client.query_rows("""
    SELECT
        market_id,
        hour_bucket,
        round(avg_volume24h, 2)     AS avg_volume24h,
        round(max_volume24h, 2)     AS max_volume24h,
        round(avg_liquidity, 2)     AS avg_liquidity,
        snapshot_count
    FROM fact_market_snapshot_hourly
    WHERE market_id NOT IN ('market_001', 'market_002')
    ORDER BY snapshot_count DESC, hour_bucket DESC
    LIMIT 10
""")
df = pd.DataFrame(rows)
print(df.to_string(index=False))

# ─────────────────────────────────────────
# STEP 4 - Show fact_outcome_snapshot_hourly
# ─────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 4 - FACT: fact_outcome_snapshot_hourly (real data)")
print("=" * 60)
rows2 = client.query_rows("""
    SELECT
        market_id,
        label,
        hour_bucket,
        round(open_price, 4)        AS open_price,
        round(close_price, 4)       AS close_price,
        round(high_price, 4)        AS high_price,
        round(low_price, 4)         AS low_price,
        round(avg_price, 4)         AS avg_price,
        snapshot_count
    FROM fact_outcome_snapshot_hourly
    WHERE market_id NOT IN ('market_001', 'market_002')
    ORDER BY snapshot_count DESC, hour_bucket DESC
    LIMIT 10
""")
df2 = pd.DataFrame(rows2)
print(df2.to_string(index=False))

# ─────────────────────────────────────────
# STEP 5 - Show fact_market_snapshot_daily
# ─────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 5 - FACT: fact_market_snapshot_daily (real data)")
print("=" * 60)
rows3 = client.query_rows("""
    SELECT
        market_id,
        day_bucket,
        round(avg_volume24h, 2)     AS avg_volume24h,
        round(max_volume24h, 2)     AS max_volume24h,
        round(avg_liquidity, 2)     AS avg_liquidity,
        snapshot_count
    FROM fact_market_snapshot_daily
    WHERE market_id NOT IN ('market_001', 'market_002')
    ORDER BY snapshot_count DESC
    LIMIT 10
""")
df3 = pd.DataFrame(rows3)
print(df3.to_string(index=False))

# ─────────────────────────────────────────
# STEP 6 - Show dimension tables
# ─────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 6 - DIM: dim_market (real data)")
print("=" * 60)
rows4 = client.query_rows("""
    SELECT
        market_id,
        title,
        category
    FROM dim_market
    WHERE market_id NOT IN ('market_001', 'market_002')
    LIMIT 10
""")
df4 = pd.DataFrame(rows4)
print(df4.to_string(index=False))

print("\n" + "=" * 60)
print("STEP 7 - DIM: dim_outcome (real data)")
print("=" * 60)
rows5 = client.query_rows("""
    SELECT
        outcome_id,
        market_id,
        label
    FROM dim_outcome
    WHERE market_id NOT IN ('market_001', 'market_002')
    LIMIT 10
""")
df5 = pd.DataFrame(rows5)
print(df5.to_string(index=False))

print("\n" + "=" * 60)
print("STEP 8 - DIM: dim_source")
print("=" * 60)
rows6 = client.query_rows("""
    SELECT source_id, source, exchange
    FROM dim_source
    LIMIT 10
""")
df6 = pd.DataFrame(rows6)
print(df6.to_string(index=False))

print("\n" + "=" * 60)
print("ALL REAL DATA TESTS COMPLETE!")
print("=" * 60)
