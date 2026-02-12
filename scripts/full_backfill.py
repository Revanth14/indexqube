from polygon import RESTClient
import pandas as pd
import sqlalchemy as sa
import os
from dotenv import load_dotenv
from datetime import datetime
import time

load_dotenv('../.env')

client = RESTClient(api_key=os.getenv('MASSIVE_API_KEY'))
engine = sa.create_engine(
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

# Get all tickers
all_tickers = pd.read_sql(
    "SELECT ticker FROM sp500_constituents ORDER BY ticker",
    engine
)['ticker'].tolist()

# Get already loaded tickers (resume capability)
loaded = pd.read_sql(
    "SELECT DISTINCT ticker FROM market_data_eod",
    engine
)['ticker'].tolist()

remaining = [t for t in all_tickers if t not in loaded]

print("Full backfill: 500 tickers x 5 years")
print(f"Total:          {len(all_tickers)} tickers")
print(f"Already loaded: {len(loaded)} tickers")
print(f"Remaining:      {len(remaining)} tickers")
print(f"Est. time:      ~{len(remaining) * 0.3 / 60:.0f} minutes")
print("")

errors = []
batch = []
BATCH_SIZE = 25  # Save every 25 tickers

start_time = time.time()

for i, ticker in enumerate(remaining, 1):
    print(f"[{i}/{len(remaining)}] {ticker}...", end=" ", flush=True)
    try:
        aggs = client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan='day',
            from_='2020-01-01',
            to='2025-02-12',
            limit=50000
        )
        count = 0
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
            count += 1
        print(f"{count} records")

        # Save batch every 25 tickers
        if i % BATCH_SIZE == 0:
            df = pd.DataFrame(batch)
            df.to_sql('market_data_eod', engine, 
                     if_exists='append', index=False,
                     method='multi')
            elapsed = (time.time() - start_time) / 60
            remaining_tickers = len(remaining) - i
            eta = (elapsed / i) * remaining_tickers
            print(f"\nSaved {len(df):,} records | "
                  f"Elapsed: {elapsed:.1f}min | "
                  f"ETA: {eta:.0f}min\n")
            batch = []

        time.sleep(0.12)  # Unlimited calls but be nice

    except Exception as e:
        print(f"{e}")
        errors.append(ticker)
        time.sleep(1)

# Save final batch
if batch:
    df = pd.DataFrame(batch)
    df.to_sql('market_data_eod', engine,
             if_exists='append', index=False,
             method='multi')
    print(f"\nSaved final batch: {len(df):,} records")

# Final summary
total = pd.read_sql(
    "SELECT COUNT(*) as count, COUNT(DISTINCT ticker) as tickers FROM market_data_eod",
    engine
)
elapsed = (time.time() - start_time) / 60

print("\nBackfill complete")
print(f"Total records:  {total['count'][0]:,}")
print(f"Total tickers:  {total['tickers'][0]}")
print(f"Time taken:     {elapsed:.1f} minutes")
print(f"Failed:         {len(errors)} tickers")
if errors:
    print(f"Errors: {', '.join(errors)}")
