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

# These tickers use different formats in Polygon
ticker_map = {
    'BF-B': 'BF.B',
    'BRK-B': 'BRK.B',
}

for our_ticker, polygon_ticker in ticker_map.items():
    print(f"Fetching {polygon_ticker} (stored as {our_ticker})...")
    try:
        aggs = client.get_aggs(
            ticker=polygon_ticker,
            multiplier=1,
            timespan='day',
            from_='2020-01-01',
            to='2025-02-12',
            limit=50000
        )
        
        batch = []
        for agg in aggs:
            batch.append({
                'ticker': our_ticker,  # Store with our format
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
            print(f"  Loaded {len(df):,} records for {our_ticker}")
        
        time.sleep(0.2)
        
    except Exception as e:
        print(f"  Error: {e}")

print("\nDone!")
