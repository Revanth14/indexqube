from __future__ import annotations

from datetime import timedelta
import os
import time

import pandas as pd
import pendulum
import sqlalchemy as sa
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from sqlalchemy import text
from polygon import RESTClient


default_args = {
    "owner": "indexqube",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DB_URL = (
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}"
    f"@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

# tasks
def get_engine() -> sa.Engine:
    return sa.create_engine(DB_URL)


@task
def fetch_daily_prices() -> dict:
    """Fetch EOD prices for all S&P 500 tickers."""
    context = get_current_context()
    execution_date = context["logical_date"].to_date_string()
    print(f"Fetching prices for {execution_date}")

    client = RESTClient(api_key=os.getenv("MASSIVE_API_KEY"))
    engine = get_engine()

    tickers = pd.read_sql(
        "SELECT ticker FROM sp500_constituents WHERE is_active = TRUE",
        engine,
    )["ticker"].tolist()

    print(f"Fetching {len(tickers)} tickers for {execution_date}")

    batch = []
    errors = []

    for i, ticker in enumerate(tickers, 1):
        try:
            aggs = client.get_aggs(
                ticker=ticker,
                multiplier=1,
                timespan="day",
                from_=execution_date,
                to=execution_date,
                limit=1,
            )
            for agg in aggs:
                batch.append(
                    {
                        "ticker": ticker,
                        "date": execution_date,
                        "open": agg.open,
                        "high": agg.high,
                        "low": agg.low,
                        "close": agg.close,
                        "volume": agg.volume,
                        "adjusted_close": agg.close,
                        "provider": "massive",
                    }
                )

            if i % 100 == 0:
                print(f"Progress: {i}/{len(tickers)}")

            time.sleep(0.12)

        except Exception as exc:
            print(f"Error {ticker}: {exc}")
            errors.append(ticker)

    if batch:
        df = pd.DataFrame(batch)
        with engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(
                    text(
                        """
                        INSERT INTO market_data_eod
                            (ticker, date, open, high, low, close,
                             volume, adjusted_close, provider)
                        VALUES
                            (:ticker, :date, :open, :high, :low, :close,
                             :volume, :adjusted_close, :provider)
                        ON CONFLICT (ticker, date, provider) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            adjusted_close = EXCLUDED.adjusted_close
                        """
                    ),
                    row.to_dict(),
                )

    print(f"Loaded {len(batch)} records, {len(errors)} errors")
    return {"loaded": len(batch), "errors": len(errors)}


@task
def fetch_daily_corporate_actions() -> dict:
    """Fetch corporate actions for execution date."""
    context = get_current_context()
    execution_date = context["logical_date"].to_date_string()
    print(f"Fetching corporate actions for {execution_date}")

    client = RESTClient(api_key=os.getenv("MASSIVE_API_KEY"))
    engine = get_engine()

    tickers = pd.read_sql(
        "SELECT ticker FROM sp500_constituents WHERE is_active = TRUE",
        engine,
    )["ticker"].tolist()

    splits = []
    dividends = []

    for ticker in tickers:
        try:
            for split in client.list_splits(
                ticker=ticker,
                execution_date_gte=execution_date,
                execution_date_lte=execution_date,
            ):
                splits.append(
                    {
                        "ticker": ticker,
                        "action_type": "split",
                        "ex_date": str(split.execution_date),
                        "payment_date": None,
                        "ratio": float(split.split_from / split.split_to)
                        if split.split_to
                        else None,
                        "amount": None,
                        "provider": "massive",
                    }
                )

            for div in client.list_dividends(
                ticker=ticker,
                ex_dividend_date_gte=execution_date,
                ex_dividend_date_lte=execution_date,
            ):
                dividends.append(
                    {
                        "ticker": ticker,
                        "action_type": "dividend",
                        "ex_date": str(div.ex_dividend_date),
                        "payment_date": str(div.pay_date)
                        if div.pay_date
                        else None,
                        "ratio": None,
                        "amount": float(div.cash_amount)
                        if div.cash_amount
                        else None,
                        "provider": "massive",
                    }
                )

            time.sleep(0.12)

        except Exception as exc:
            print(f"Error {ticker}: {exc}")

    print(f"Found {len(splits)} splits, {len(dividends)} dividends")
    return {"splits": len(splits), "dividends": len(dividends)}


@task
def run_daily_validation() -> dict:
    """Run validation checks for execution date."""
    context = get_current_context()
    execution_date = context["logical_date"].to_date_string()
    print(f"Running validation for {execution_date}")

    engine = get_engine()

    with engine.connect() as conn:
        anomalies = pd.read_sql(
            text(
                """
                WITH daily_changes AS (
                    SELECT
                        ticker, date, close,
                        LAG(close) OVER (PARTITION BY ticker ORDER BY date) as prev_close
                    FROM market_data_eod
                    WHERE provider = 'massive'
                    AND date >= CAST(:exec_date AS date) - INTERVAL '2 days'
                )
                SELECT
                    ticker, date, close, prev_close,
                    ROUND(((close - prev_close) / prev_close * 100)::numeric, 2) as pct_change
                FROM daily_changes
                WHERE date = CAST(:exec_date AS date)
                AND prev_close > 0
                AND ABS((close - prev_close) / prev_close * 100) > 20
                """
            ),
            conn,
            params={"exec_date": execution_date},
        )

        print(f"Price anomalies today: {len(anomalies)}")

        expected = conn.execute(
            text("SELECT COUNT(*) FROM sp500_constituents WHERE is_active = TRUE")
        ).scalar()

        actual = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM market_data_eod
                WHERE date = CAST(:exec_date AS date)
                AND provider = 'massive'
                """
            ),
            {"exec_date": execution_date},
        ).scalar()

        completeness = round(actual / expected * 100, 2) if expected else 0
        print(f"Today's completeness: {actual}/{expected} = {completeness}%")

    print(f"Validation complete for {execution_date}")
    return {"completeness": completeness}


@task
def update_provider_metadata() -> None:
    """Update provider last_update timestamp."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE provider_metadata
                SET last_update = NOW()
                WHERE provider_name = 'massive'
                """
            )
        )
    print("Provider metadata updated")


# dag definition
@dag(
    dag_id="daily_ingestion",
    default_args=default_args,
    description="Daily market data ingestion pipeline",
    schedule="0 18 * * 1-5",
    start_date=pendulum.datetime(2025, 2, 12, tz="America/New_York"),
    catchup=False,
    tags=["ingestion", "daily"],
)
def daily_ingestion():
    prices = fetch_daily_prices()
    corporate = fetch_daily_corporate_actions()
    validation = run_daily_validation()
    metadata = update_provider_metadata()

    prices >> corporate >> validation >> metadata


dag = daily_ingestion()
