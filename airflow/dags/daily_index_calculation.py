from __future__ import annotations

from datetime import timedelta
import os

import numpy as np
import pandas as pd
import pendulum
import sqlalchemy as sa
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from sqlalchemy import text

default_args = {
    "owner": "indexqube",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DB_URL = (
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}"
    f"@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

def get_engine() -> sa.Engine:
    return sa.create_engine(DB_URL)


@task
def calculate_factor_index() -> dict:
    """Calculate IQQM25 Factor Index."""
    context = get_current_context()
    execution_date = context["logical_date"].to_date_string()
    print(f"Calculating IQQM25 for {execution_date}")

    engine = get_engine()

    # Get latest index value
    with engine.begin() as conn:
        last_value = conn.execute(text("""
            SELECT value FROM index_values
            WHERE index_ticker = 'IQQM25'
            ORDER BY date DESC LIMIT 1
        """)).fetchone()

        # Get today's market data
        today_data = pd.read_sql(
            text(
                """
                SELECT ticker, close
                FROM market_data_eod
                WHERE date = CAST(:exec_date AS date)
                AND provider = 'massive'
                """
            ),
            conn,
            params={"exec_date": execution_date},
        )

        if today_data.empty:
            print(f"No data for {execution_date}, skipping")
            return

        # Get previous day data
        prev_data = pd.read_sql(
            text(
                """
                SELECT ticker, close as prev_close
                FROM market_data_eod
                WHERE date = (
                    SELECT MAX(date) FROM market_data_eod
                    WHERE date < CAST(:exec_date AS date)
                    AND provider = 'massive'
                )
                AND provider = 'massive'
                """
            ),
            conn,
            params={"exec_date": execution_date},
        )

        # Get current constituents
        constituents = pd.read_sql(
            text(
                """
                SELECT ticker, weight
                FROM index_constituents
                WHERE index_ticker = 'IQQM25'
                AND effective_date = (
                    SELECT MAX(effective_date)
                    FROM index_constituents
                    WHERE index_ticker = 'IQQM25'
                )
                """
            ),
            conn,
        )

        if constituents.empty:
            print("No IQQM25 constituents found, skipping calculation")
            return

        # Calculate weighted return
        merged = today_data.merge(prev_data, on='ticker')
        merged = merged.merge(constituents, on='ticker')
        merged["return"] = (merged["close"] - merged["prev_close"]) / merged[
            "prev_close"
        ]
        merged["weighted_return"] = merged["return"] * merged["weight"] / 100

        index_return = merged['weighted_return'].sum()
        base_value = float(last_value[0]) if last_value else 1000.0
        new_value = round(base_value * (1 + index_return), 4)

        # Save index value
        conn.execute(
            text(
                """
                INSERT INTO index_values (index_ticker, date, value, return_pct)
                VALUES ('IQQM25', :date, :value, :return_pct)
                ON CONFLICT (index_ticker, date) DO UPDATE SET
                    value = EXCLUDED.value,
                    return_pct = EXCLUDED.return_pct
                """
            ),
            {
                "date": execution_date,
                "value": new_value,
                "return_pct": round(index_return * 100, 4),
            },
        )


    print(f"IQQM25: {new_value} ({index_return*100:.2f}%)")
    return {"index": "IQQM25", "value": new_value}


@task
def calculate_vol_control_index() -> dict:
    """Calculate IQTVC Vol Control Index."""
    context = get_current_context()
    execution_date = context["logical_date"].to_date_string()
    print(f"Calculating IQTVC for {execution_date}")

    engine = get_engine()

    with engine.begin() as conn:
        # Get 60-day returns for vol calculation
        returns = pd.read_sql(
            text(
                """
                SELECT date, close,
                    (close - LAG(close) OVER (ORDER BY date)) /
                    LAG(close) OVER (ORDER BY date) as daily_return
                FROM market_data_eod
                WHERE ticker = 'SPY'
                AND provider = 'massive'
                AND date <= CAST(:exec_date AS date)
                ORDER BY date DESC
                LIMIT 62
                """
            ),
            conn,
            params={"exec_date": execution_date},
        )

        if len(returns) < 20:
            print("Not enough data for vol calculation")
            return

        # Calculate realized volatility (60-day annualized)
        realized_vol = returns["daily_return"].dropna().std() * np.sqrt(252)

        # Target vol = 20%, calculate leverage
        target_vol = 0.20
        leverage = min(target_vol / realized_vol, 1.5)  # Max 1.5x leverage

        # Get last index value
        last_value = conn.execute(text("""
            SELECT value FROM index_values
            WHERE index_ticker = 'IQTVC'
            ORDER BY date DESC LIMIT 1
        """)).fetchone()

        # Get SPY return today
        spy_today = pd.read_sql(
            text(
                """
                SELECT close FROM market_data_eod
                WHERE ticker = 'SPY'
                AND date = CAST(:exec_date AS date)
                AND provider = 'massive'
                """
            ),
            conn,
            params={"exec_date": execution_date},
        )

        spy_prev = pd.read_sql(
            text(
                """
                SELECT close FROM market_data_eod
                WHERE ticker = 'SPY'
                AND date = (
                    SELECT MAX(date) FROM market_data_eod
                    WHERE date < CAST(:exec_date AS date)
                    AND provider = 'massive'
                )
                AND provider = 'massive'
                """
            ),
            conn,
            params={"exec_date": execution_date},
        )

        if spy_today.empty or spy_prev.empty:
            print("No SPY data for today")
            return

        spy_return = (
            float(spy_today["close"].iloc[0]) - float(spy_prev["close"].iloc[0])
        ) / float(spy_prev["close"].iloc[0])

        # Apply leverage to return
        leveraged_return = spy_return * leverage

        base_value = float(last_value[0]) if last_value else 1000.0
        new_value = round(base_value * (1 + leveraged_return), 4)

        conn.execute(
            text(
                """
                INSERT INTO index_values (index_ticker, date, value, return_pct)
                VALUES ('IQTVC', :date, :value, :return_pct)
                ON CONFLICT (index_ticker, date) DO UPDATE SET
                    value = EXCLUDED.value,
                    return_pct = EXCLUDED.return_pct
                """
            ),
            {
                "date": execution_date,
                "value": new_value,
                "return_pct": round(leveraged_return * 100, 4),
            },
        )


    print(f"IQTVC: {new_value} | Vol: {realized_vol:.1%} | Leverage: {leverage:.2f}x")
    return {"index": "IQTVC", "value": new_value, "leverage": leverage}


@task
def calculate_defined_outcome_index() -> dict:
    """Calculate IQSPB10 Defined Outcome Index."""
    context = get_current_context()
    execution_date = context["logical_date"].to_date_string()
    print(f"Calculating IQSPB10 for {execution_date}")

    engine = get_engine()

    with engine.begin() as conn:
        # Get SPY data
        spy_data = pd.read_sql(
            text(
                """
                SELECT date, close
                FROM market_data_eod
                WHERE ticker = 'SPY'
                AND provider = 'massive'
                AND date <= CAST(:exec_date AS date)
                ORDER BY date DESC
                LIMIT 2
                """
            ),
            conn,
            params={"exec_date": execution_date},
        )

        if len(spy_data) < 2:
            print("Not enough SPY data")
            return

        spy_return = (
            float(spy_data["close"].iloc[0]) - float(spy_data["close"].iloc[1])
        ) / float(spy_data["close"].iloc[1])

        # Buffer = 10%, Cap = 20% (annual)
        # Daily equivalent
        buffer_daily = 0.10 / 252
        cap_daily = 0.20 / 252

        # Apply buffer and cap
        if spy_return < -buffer_daily:
            # Below buffer: absorb loss beyond buffer
            index_return = spy_return + buffer_daily
        elif spy_return > cap_daily:
            # Above cap: capped at cap
            index_return = cap_daily
        else:
            # Within range: full participation
            index_return = spy_return

        last_value = conn.execute(text("""
            SELECT value FROM index_values
            WHERE index_ticker = 'IQSPB10'
            ORDER BY date DESC LIMIT 1
        """)).fetchone()

        base_value = float(last_value[0]) if last_value else 1000.0
        new_value = round(base_value * (1 + index_return), 4)

        conn.execute(
            text(
                """
                INSERT INTO index_values (index_ticker, date, value, return_pct)
                VALUES ('IQSPB10', :date, :value, :return_pct)
                ON CONFLICT (index_ticker, date) DO UPDATE SET
                    value = EXCLUDED.value,
                    return_pct = EXCLUDED.return_pct
                """
            ),
            {
                "date": execution_date,
                "value": new_value,
                "return_pct": round(index_return * 100, 4),
            },
        )


    print(f"IQSPB10: {new_value} ({index_return*100:.4f}%)")
    return {"index": "IQSPB10", "value": new_value}


@dag(
    dag_id="daily_index_calculation",
    default_args=default_args,
    description="Daily index calculation pipeline",
    schedule="0 19 * * 1-5",
    start_date=pendulum.datetime(2025, 2, 12, tz="America/New_York"),
    catchup=False,
    tags=["calculation", "daily"],
)
def daily_index_calculation():
    factor = calculate_factor_index()
    vol = calculate_vol_control_index()
    outcome = calculate_defined_outcome_index()

    factor >> vol >> outcome


dag = daily_index_calculation()
