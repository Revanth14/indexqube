import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy import text
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv('../.env')

engine = sa.create_engine(
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

print("Indexqube backtest engine")

# Load all market data
print("\nLoading market data...")

prices = pd.read_sql("""
    SELECT ticker, date, close, volume
    FROM market_data_eod
    WHERE provider = 'massive'
    ORDER BY ticker, date
""", engine)

prices['date'] = pd.to_datetime(prices['date'])

print(f"Loaded {len(prices):,} records")
print(f"   Tickers: {prices['ticker'].nunique()}")
print(f"   Date range: {prices['date'].min().date()} to {prices['date'].max().date()}")

# Pivot to wide format
price_matrix = prices.pivot(index='date', columns='ticker', values='close')
volume_matrix = prices.pivot(index='date', columns='ticker', values='volume')

print(f"   Price matrix: {price_matrix.shape}")

# Get trading days
trading_days = price_matrix.index.tolist()
print(f"   Trading days: {len(trading_days)}")

# Index 1: IQTVC (vol control)
print("\nCalculating IQTVC (Vol Control Index)")

spy_prices = price_matrix['SPY'].dropna()
spy_returns = spy_prices.pct_change()

TARGET_VOL = 0.20
MAX_LEVERAGE = 1.5
LOOKBACK = 60  # days for vol calculation

iqtvc_values = []
base_value = 1000.0
prev_value = base_value

for i, date in enumerate(spy_returns.index):
    if i < LOOKBACK:
        continue

    # 60-day realized vol
    recent_returns = spy_returns.iloc[i-LOOKBACK:i]
    realized_vol = recent_returns.std() * np.sqrt(252)

    if realized_vol == 0 or np.isnan(realized_vol):
        continue

    # Calculate leverage
    leverage = min(TARGET_VOL / realized_vol, MAX_LEVERAGE)

    # Today's SPY return
    spy_return = spy_returns.iloc[i]

    if np.isnan(spy_return):
        continue

    # Apply leverage
    index_return = spy_return * leverage
    new_value = round(prev_value * (1 + index_return), 4)

    iqtvc_values.append({
        'index_ticker': 'IQTVC',
        'date': date.date(),
        'value': new_value,
        'return_pct': round(index_return * 100, 4),
        'metadata': {
            'leverage': round(float(leverage), 4),
            'realized_vol': round(float(realized_vol), 4),
            'spy_return': round(float(spy_return), 4)
        }
    })

    prev_value = new_value

iqtvc_df = pd.DataFrame(iqtvc_values)
print(f"IQTVC calculated: {len(iqtvc_df)} data points")
print(f"   Start value:  1000.00")
print(f"   End value:    {iqtvc_df['value'].iloc[-1]:,.2f}")
print(f"   Total return: {((iqtvc_df['value'].iloc[-1] / 1000) - 1) * 100:.1f}%")
print(f"   Max value:    {iqtvc_df['value'].max():,.2f}")
print(f"   Min value:    {iqtvc_df['value'].min():,.2f}")

# Index 2: IQSPB10 (defined outcome)
print("\nCalculating IQSPB10 (Defined Outcome - 10% Buffer)")

BUFFER = 0.10   # 10% annual buffer
CAP = 0.20      # 20% annual cap

iqspb10_values = []
base_value = 1000.0
prev_value = base_value

# Track annual outcome period (reset yearly)
period_start_spy = spy_prices.iloc[0]
period_start_value = base_value
period_start_date = spy_prices.index[0]

for i, date in enumerate(spy_prices.index):
    if i < 1:
        continue

    spy_return = spy_returns.iloc[i]
    if np.isnan(spy_return):
        continue

    # Cumulative SPY return since period start
    cumulative_spy = (spy_prices.iloc[i] / period_start_spy) - 1

    # Apply buffer and cap to CUMULATIVE return
    if cumulative_spy < -BUFFER:
        # Investor absorbs loss beyond buffer
        adjusted_cumulative = cumulative_spy + BUFFER
    elif cumulative_spy > CAP:
        # Investor capped at cap
        adjusted_cumulative = CAP
    else:
        # Full participation
        adjusted_cumulative = cumulative_spy

    # Index value based on adjusted cumulative
    new_value = round(period_start_value * (1 + adjusted_cumulative), 4)

    # Reset period annually
    days_in_period = (date - period_start_date).days
    if days_in_period >= 365:
        period_start_spy = spy_prices.iloc[i]
        period_start_value = new_value
        period_start_date = date

    iqspb10_values.append({
        'index_ticker': 'IQSPB10',
        'date': date.date(),
        'value': new_value,
        'return_pct': round(spy_return * 100, 4),
        'metadata': {
            'cumulative_spy': round(float(cumulative_spy), 4),
            'buffered': cumulative_spy < -BUFFER,
            'capped': cumulative_spy > CAP
        }
    })

    prev_value = new_value

iqspb10_df = pd.DataFrame(iqspb10_values)
print(f"IQSPB10 calculated: {len(iqspb10_df)} data points")
print(f"   Start value:  1000.00")
print(f"   End value:    {iqspb10_df['value'].iloc[-1]:,.2f}")
print(f"   Total return: {((iqspb10_df['value'].iloc[-1] / 1000) - 1) * 100:.1f}%")
print(f"   Buffered days: {iqspb10_df['metadata'].apply(lambda x: x['buffered']).sum()}")
print(f"   Capped days:   {iqspb10_df['metadata'].apply(lambda x: x['capped']).sum()}")

# Index 3: IQQM25 (factor index)
print("\nCalculating IQQM25 (Quality-Momentum Factor Index)")

# Get SP500 tickers
sp500_tickers = pd.read_sql(
    "SELECT ticker FROM sp500_constituents WHERE is_active = TRUE",
    engine
)['ticker'].tolist()

# Filter to tickers we have data for
available = [t for t in sp500_tickers if t in price_matrix.columns]
print(f"Available tickers: {len(available)}/{len(sp500_tickers)}")

# Calculate 6-month momentum for each rebalance date
# Rebalance quarterly: March, June, September, December
rebalance_months = [3, 6, 9, 12]

MOMENTUM_DAYS = 126  # ~6 months
TOP_N = 25  # Top 25 stocks

iqqm25_values = []
base_value = 1000.0
prev_value = base_value
current_weights = None
current_constituents = []
last_rebalance = None

all_dates = price_matrix.index.tolist()

for i, date in enumerate(all_dates):
    if i < MOMENTUM_DAYS:
        continue

    # Check if rebalance needed (quarterly)
    is_rebalance = (
        date.month in rebalance_months and
        (last_rebalance is None or
         (date - last_rebalance).days >= 60)
    )

    if is_rebalance:
        # Calculate momentum scores
        momentum_scores = {}

        for ticker in available:
            ticker_prices = price_matrix[ticker].iloc[i-MOMENTUM_DAYS:i]

            if ticker_prices.isna().sum() > 10:
                continue

            ticker_prices = ticker_prices.dropna()

            if len(ticker_prices) < 100:
                continue

            # Momentum = 6-month return (skip last month)
            momentum = (
                ticker_prices.iloc[-21] / ticker_prices.iloc[0]
            ) - 1

            # Volume factor (liquidity proxy for quality)
            avg_volume = volume_matrix[ticker].iloc[i-MOMENTUM_DAYS:i].mean()

            if np.isnan(momentum) or np.isnan(avg_volume):
                continue

            momentum_scores[ticker] = {
                'momentum': momentum,
                'volume': avg_volume
            }

        if len(momentum_scores) < TOP_N:
            continue

        scores_df = pd.DataFrame(momentum_scores).T
        scores_df['momentum_rank'] = scores_df['momentum'].rank(ascending=False)
        scores_df['volume_rank'] = scores_df['volume'].rank(ascending=False)
        scores_df['composite_rank'] = (
            scores_df['momentum_rank'] * 0.7 +
            scores_df['volume_rank'] * 0.3
        )

        # Select top 25
        top25 = scores_df.nsmallest(TOP_N, 'composite_rank')
        current_constituents = top25.index.tolist()
        current_weights = {t: 1/TOP_N for t in current_constituents}
        last_rebalance = date

        print(f"   Rebalanced on {date.date()}: {current_constituents[:5]}...")

    if current_weights is None:
        continue

    # Calculate index return
    if i == 0:
        continue

    weighted_return = 0
    valid_count = 0

    for ticker, weight in current_weights.items():
        if ticker not in price_matrix.columns:
            continue

        today_price = price_matrix[ticker].iloc[i]
        prev_price = price_matrix[ticker].iloc[i-1]

        if np.isnan(today_price) or np.isnan(prev_price) or prev_price == 0:
            continue

        stock_return = (today_price - prev_price) / prev_price
        weighted_return += stock_return * weight
        valid_count += 1

    if valid_count < TOP_N * 0.8:
        continue

    new_value = round(prev_value * (1 + weighted_return), 4)

    iqqm25_values.append({
        'index_ticker': 'IQQM25',
        'date': date.date(),
        'value': new_value,
        'return_pct': round(weighted_return * 100, 4)
    })

    prev_value = new_value

iqqm25_df = pd.DataFrame(iqqm25_values)
print(f"IQQM25 calculated: {len(iqqm25_df)} data points")
print(f"   Start value:  1000.00")
print(f"   End value:    {iqqm25_df['value'].iloc[-1]:,.2f}")
print(f"   Total return: {((iqqm25_df['value'].iloc[-1] / 1000) - 1) * 100:.1f}%")

# Save all to database
print("\nSaving to database")

with engine.connect() as conn:
    # Clear existing backtest data
    conn.execute(text("DELETE FROM index_values"))
    conn.execute(text("DELETE FROM index_constituents"))
    conn.commit()

    # Save IQTVC
    print("Saving IQTVC...", end=" ", flush=True)
    for _, row in iqtvc_df.iterrows():
        conn.execute(text("""
            INSERT INTO index_values (index_ticker, date, value, return_pct)
            VALUES (:index_ticker, :date, :value, :return_pct)
            ON CONFLICT (index_ticker, date) DO UPDATE SET
                value = EXCLUDED.value,
                return_pct = EXCLUDED.return_pct
        """), {
            'index_ticker': row['index_ticker'],
            'date': row['date'],
            'value': row['value'],
            'return_pct': row['return_pct']
        })
    conn.commit()
    print(f"{len(iqtvc_df)} rows")

    # Save IQSPB10
    print("Saving IQSPB10...", end=" ", flush=True)
    for _, row in iqspb10_df.iterrows():
        conn.execute(text("""
            INSERT INTO index_values (index_ticker, date, value, return_pct)
            VALUES (:index_ticker, :date, :value, :return_pct)
            ON CONFLICT (index_ticker, date) DO UPDATE SET
                value = EXCLUDED.value,
                return_pct = EXCLUDED.return_pct
        """), {
            'index_ticker': row['index_ticker'],
            'date': row['date'],
            'value': row['value'],
            'return_pct': row['return_pct']
        })
    conn.commit()
    print(f"{len(iqspb10_df)} rows")

    # Save IQQM25
    print("Saving IQQM25...", end=" ", flush=True)
    for _, row in iqqm25_df.iterrows():
        conn.execute(text("""
            INSERT INTO index_values (index_ticker, date, value, return_pct)
            VALUES (:index_ticker, :date, :value, :return_pct)
            ON CONFLICT (index_ticker, date) DO UPDATE SET
                value = EXCLUDED.value,
                return_pct = EXCLUDED.return_pct
        """), {
            'index_ticker': row['index_ticker'],
            'date': row['date'],
            'value': row['value'],
            'return_pct': row['return_pct']
        })
    conn.commit()
    print(f"{len(iqqm25_df)} rows")

    # Save IQQM25 constituents (latest rebalance)
    if current_constituents:
        print("Saving IQQM25 constituents...", end=" ", flush=True)
        for ticker in current_constituents:
            conn.execute(text("""
                INSERT INTO index_constituents
                    (index_ticker, ticker, weight, effective_date)
                VALUES
                    ('IQQM25', :ticker, :weight, :effective_date)
                ON CONFLICT DO NOTHING
            """), {
                'ticker': ticker,
                'weight': round(1/TOP_N * 100, 4),
                'effective_date': last_rebalance.date()
            })
        conn.commit()
        print(f"{len(current_constituents)} constituents")

# Final summary
print("\nBacktest complete")

summary = pd.read_sql("""
    SELECT 
        index_ticker,
        COUNT(*) as data_points,
        MIN(date) as from_date,
        MAX(date) as to_date,
        MIN(value) as min_value,
        MAX(value) as max_value,
        ROUND(((MAX(value) / 1000.0) - 1) * 100, 1) as approx_total_return_pct
    FROM index_values
    GROUP BY index_ticker
    ORDER BY index_ticker
""", engine)

print(summary.to_string(index=False))
print(f"\nTotal index values in DB: {summary['data_points'].sum():,}")
