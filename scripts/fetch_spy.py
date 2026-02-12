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

# Fetch SPY + QQQ as benchmark ETFs
etfs = ['SPY', 'QQQ', 'IWM']

for ticker in etfs:
    print(f"Fetching {ticker}...", end=" ", flush=True)
    try:
        aggs = client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan='day',
            from_='2020-01-01',
            to='2025-02-12',
            limit=50000
        )

        batch = []
        for agg in aggs:
            batch.append({
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

        if batch:
            df = pd.DataFrame(batch)
            df.to_sql('market_data_eod', engine,
                     if_exists='append', index=False,
                     method='multi')
            print(f"{len(df):,} records")

        time.sleep(0.2)

    except Exception as e:
        print(f"Error: {e}")

# Verify
verify = pd.read_sql(
    "SELECT ticker, COUNT(*) as records, MIN(date) as from_date, MAX(date) as to_date FROM market_data_eod WHERE ticker IN ('SPY','QQQ','IWM') GROUP BY ticker",
    engine
)
print("\nETF data loaded:")
print(verify.to_string(index=False))
