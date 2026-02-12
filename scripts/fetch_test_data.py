from polygon import RESTClient
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
import os
from dotenv import load_dotenv
from datetime import datetime
import time

load_dotenv('../.env')

client = RESTClient(api_key=os.getenv('MASSIVE_API_KEY'))
engine = sa.create_engine(
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

# Skip already loaded tickers
loaded = pd.read_sql(
    "SELECT DISTINCT ticker FROM market_data_eod",
    engine
)['ticker'].tolist()

all_tickers = pd.read_sql(
    "SELECT ticker FROM sp500_constituents ORDER BY ticker LIMIT 10",
    engine
)['ticker'].tolist()

remaining = [t for t in all_tickers if t not in loaded]

print(f"Already loaded: {loaded}")
print(f"Remaining: {remaining}\n")

if not remaining:
    print("All tickers already loaded.")
    verify = pd.read_sql("SELECT COUNT(*) as count FROM market_data_eod", engine)
    print(f"DB total: {verify['count'][0]:,} records")
    exit()

all_data = []
errors = []

for i, ticker in enumerate(remaining, 1):
    print(f"[{i}/{len(remaining)}] {ticker}...", end=" ", flush=True)
    try:
        aggs = client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan='day',
            from_='2024-02-01',
            to='2025-02-11',
            limit=50000
        )
        count = 0
        for agg in aggs:
            all_data.append({
                'ticker': ticker,
                'date': datetime.fromtimestamp(agg.timestamp / 1000).date(),
                'open': agg.open,
                'high': agg.high,
                'low': agg.low,
                'close': agg.close,
                'volume': agg.volume,
                'adjusted_close': agg.close,
                'provider': 'massive'
            })
            count += 1
        print(f"{count} records")
        time.sleep(13)  # Free tier: 5 calls/min

    except Exception as e:
        print(f"Error: {e}")
        errors.append(ticker)
        time.sleep(30)

if all_data:
    df = pd.DataFrame(all_data)
    
    # Use INSERT ON CONFLICT DO NOTHING (upsert)
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO market_data_eod 
                    (ticker, date, open, high, low, close, volume, adjusted_close, provider)
                VALUES 
                    (:ticker, :date, :open, :high, :low, :close, :volume, :adjusted_close, :provider)
                ON CONFLICT (ticker, date, provider) DO NOTHING
            """), row.to_dict())
        conn.commit()
    
    print(f"\nLoaded {len(df):,} records (skipped duplicates)")

verify = pd.read_sql(
    "SELECT ticker, COUNT(*) as records FROM market_data_eod GROUP BY ticker ORDER BY ticker",
    engine
)
print(f"\nDB Summary:")
print(verify.to_string(index=False))
