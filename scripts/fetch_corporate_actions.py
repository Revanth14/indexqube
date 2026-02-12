
from polygon import RESTClient
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
import os
import numpy as np
from dotenv import load_dotenv
import time

load_dotenv('../.env')

client = RESTClient(api_key=os.getenv('MASSIVE_API_KEY'))
engine = sa.create_engine(
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

tickers = pd.read_sql(
    "SELECT ticker FROM sp500_constituents ORDER BY ticker",
    engine
)['ticker'].tolist()

print(f"{'='*60}")
print(f"FETCHING CORPORATE ACTIONS")
print(f"{'='*60}")

splits = []
dividends = []
errors = []

for i, ticker in enumerate(tickers, 1):
    print(f"[{i}/{len(tickers)}] {ticker}...", end=" ", flush=True)
    try:
        # Splits
        s_count = 0
        for split in client.list_splits(ticker=ticker, execution_date_gte='2020-01-01'):
            splits.append({
                'ticker': ticker,
                'action_type': 'split',
                'ex_date': str(split.execution_date),
                'payment_date': None,  # Splits don't have payment dates
                'ratio': float(split.split_from / split.split_to) if split.split_to else None,
                'amount': None,
                'provider': 'massive'
            })
            s_count += 1

        # Dividends
        d_count = 0
        for div in client.list_dividends(ticker=ticker, ex_dividend_date_gte='2020-01-01'):
            splits.append({
                'ticker': ticker,
                'action_type': 'dividend',
                'ex_date': str(div.ex_dividend_date),
                'payment_date': str(div.pay_date) if div.pay_date else None,
                'ratio': None,
                'amount': float(div.cash_amount) if div.cash_amount else None,
                'provider': 'massive'
            })
            d_count += 1

        print(f"{s_count} splits, {d_count} divs")
        time.sleep(0.12)

    except Exception as e:
        print(f"{e}")
        errors.append(ticker)

# Save all actions
all_actions = splits + dividends

if all_actions:
    df = pd.DataFrame(all_actions)

    # Clean NaN → None
    df = df.replace({np.nan: None})
    df = df.where(pd.notnull(df), None)

    print(f"\nSaving {len(df):,} corporate actions...")

    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO corporate_actions
                    (ticker, action_type, ex_date, payment_date, ratio, amount, provider)
                VALUES
                    (:ticker, :action_type, :ex_date, :payment_date, :ratio, :amount, :provider)
                ON CONFLICT DO NOTHING
            """), {
                'ticker': row['ticker'],
                'action_type': row['action_type'],
                'ex_date': row['ex_date'],
                'payment_date': row['payment_date'] if row['payment_date'] else None,
                'ratio': row['ratio'] if row['ratio'] else None,
                'amount': row['amount'] if row['amount'] else None,
                'provider': row['provider']
            })
        conn.commit()

# Summary
verify = pd.read_sql(
    "SELECT action_type, COUNT(*) as count FROM corporate_actions GROUP BY action_type ORDER BY action_type",
    engine
)
print(f"\n{'='*60}")
print(f"CORPORATE ACTIONS COMPLETE")
print(f"{'='*60}")
print(verify.to_string(index=False))
print(f"Errors: {len(errors)}")
