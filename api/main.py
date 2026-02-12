
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import date
from typing import Optional

load_dotenv('../.env')

app = FastAPI(
    title="IndexQube API",
    description="Financial index calculation infrastructure",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_engine():
    return create_engine(
        f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}"
        f"@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
    )

# ─────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────
@app.get("/health")
def health():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "database": "connected",
            "version": "0.1.0"
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


#indices
@app.get("/indices")
def list_indices():
    engine = get_engine()
    with engine.connect() as conn:
        result = pd.read_sql(text("""
            SELECT
                index_ticker,
                COUNT(*) as data_points,
                MIN(date) as from_date,
                MAX(date) as to_date,
                ROUND(
                    ((LAST_VALUE(value) OVER (
                        PARTITION BY index_ticker
                        ORDER BY date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) / 1000.0) - 1) * 100
                , 2) as total_return_pct
            FROM index_values
            GROUP BY index_ticker, value, date
        """), conn)

    indices = {
        'IQQM25': {
            'name': 'IndexQube Quality Momentum 25',
            'type': 'factor',
            'description': 'Top 25 S&P 500 stocks by quality-momentum score, quarterly rebalanced'
        },
        'IQTVC': {
            'name': 'IndexQube Target Vol Control',
            'type': 'vol_control',
            'description': 'SPY exposure with dynamic leverage targeting 20% annualized volatility'
        },
        'IQSPB10': {
            'name': 'IndexQube S&P 500 Buffer 10',
            'type': 'defined_outcome',
            'description': '10% downside buffer with 20% upside cap on S&P 500'
        }
    }

    with engine.connect() as conn:
        summary = pd.read_sql(text("""
            SELECT
                index_ticker,
                COUNT(*) as data_points,
                MIN(date) as from_date,
                MAX(date) as to_date,
                FIRST_VALUE(value) OVER (
                    PARTITION BY index_ticker ORDER BY date
                ) as start_value,
                LAST_VALUE(value) OVER (
                    PARTITION BY index_ticker
                    ORDER BY date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as end_value
            FROM index_values
            GROUP BY index_ticker, value, date
        """), conn)

    response = []
    for ticker, meta in indices.items():
        ticker_data = summary[summary['index_ticker'] == ticker]
        if len(ticker_data) == 0:
            continue

        row = ticker_data.iloc[-1]
        total_return = round(
            (float(row['end_value']) / float(row['start_value']) - 1) * 100, 2
        )

        response.append({
            'ticker': ticker,
            'name': meta['name'],
            'type': meta['type'],
            'description': meta['description'],
            'data_points': int(ticker_data['data_points'].max()),
            'from_date': str(ticker_data['from_date'].min()),
            'to_date': str(ticker_data['to_date'].max()),
            'total_return_pct': total_return,
            'base_value': 1000.0
        })

    return {'indices': response, 'count': len(response)}


@app.get("/indices/{ticker}")
def get_index(ticker: str):
    engine = get_engine()
    ticker = ticker.upper()

    with engine.connect() as conn:
        data = pd.read_sql(text("""
            SELECT date, value, return_pct
            FROM index_values
            WHERE index_ticker = :ticker
            ORDER BY date
        """), conn, params={'ticker': ticker})

    if data.empty:
        raise HTTPException(status_code=404, detail=f"Index {ticker} not found")

    # Performance stats
    values = data['value'].values
    returns = data['return_pct'].values / 100

    import numpy as np
    annual_return = (values[-1] / values[0]) ** (252 / len(values)) - 1
    volatility = np.std(returns) * np.sqrt(252)
    sharpe = annual_return / volatility if volatility > 0 else 0

    # Max drawdown
    peak = values[0]
    max_dd = 0
    for v in values:
        if v > peak:
            peak = v
        dd = (v - peak) / peak
        if dd < max_dd:
            max_dd = dd

    return {
        'ticker': ticker,
        'data_points': len(data),
        'from_date': str(data['date'].min()),
        'to_date': str(data['date'].max()),
        'current_value': round(float(values[-1]), 4),
        'base_value': 1000.0,
        'total_return_pct': round((float(values[-1]) / 1000.0 - 1) * 100, 2),
        'annualized_return_pct': round(annual_return * 100, 2),
        'annualized_volatility_pct': round(volatility * 100, 2),
        'sharpe_ratio': round(sharpe, 3),
        'max_drawdown_pct': round(max_dd * 100, 2),
        'ytd_return_pct': round(
            (float(values[-1]) / float(
                data[data['date'].astype(str).str.startswith(
                    str(data['date'].max())[:4]
                )]['value'].iloc[0]
            ) - 1) * 100, 2
        )
    }


@app.get("/indices/{ticker}/values")
def get_index_values(
    ticker: str,
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    limit: int = Query(252, le=2000)
):
    engine = get_engine()
    ticker = ticker.upper()

    query = """
        SELECT date, value, return_pct
        FROM index_values
        WHERE index_ticker = :ticker
    """
    params = {'ticker': ticker}

    if from_date:
        query += " AND date >= :from_date"
        params['from_date'] = from_date
    if to_date:
        query += " AND date <= :to_date"
        params['to_date'] = to_date

    query += " ORDER BY date DESC LIMIT :limit"
    params['limit'] = limit

    with engine.connect() as conn:
        data = pd.read_sql(text(query), conn, params=params)

    if data.empty:
        raise HTTPException(status_code=404, detail=f"No data for {ticker}")

    data = data.sort_values('date')

    return {
        'ticker': ticker,
        'count': len(data),
        'values': [
            {
                'date': str(row['date']),
                'value': round(float(row['value']), 4),
                'return_pct': round(float(row['return_pct']), 4)
            }
            for _, row in data.iterrows()
        ]
    }


@app.get("/indices/{ticker}/constituents")
def get_constituents(ticker: str):
    engine = get_engine()
    ticker = ticker.upper()

    with engine.connect() as conn:
        data = pd.read_sql(text("""
            SELECT
                ic.ticker,
                ic.weight,
                ic.effective_date,
                m.close as last_price
            FROM index_constituents ic
            LEFT JOIN market_data_eod m
                ON m.ticker = ic.ticker
                AND m.date = (
                    SELECT MAX(date) FROM market_data_eod
                    WHERE provider = 'massive'
                )
                AND m.provider = 'massive'
            WHERE ic.index_ticker = :ticker
            AND ic.effective_date = (
                SELECT MAX(effective_date)
                FROM index_constituents
                WHERE index_ticker = :ticker
            )
            ORDER BY ic.weight DESC
        """), conn, params={'ticker': ticker})

    if data.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No constituents for {ticker}"
        )

    return {
        'ticker': ticker,
        'effective_date': str(data['effective_date'].iloc[0]),
        'count': len(data),
        'constituents': [
            {
                'ticker': row['ticker'],
                'weight_pct': round(float(row['weight']), 4),
                'last_price': round(float(row['last_price']), 2) if row['last_price'] else None
            }
            for _, row in data.iterrows()
        ]
    }


#market data
@app.get("/market-data/{ticker}")
def get_market_data(
    ticker: str,
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    limit: int = Query(252, le=2000)
):
    engine = get_engine()
    ticker = ticker.upper()

    query = """
        SELECT date, open, high, low, close, volume, adjusted_close
        FROM market_data_eod
        WHERE ticker = :ticker
        AND provider = 'massive'
    """
    params = {'ticker': ticker}

    if from_date:
        query += " AND date >= :from_date"
        params['from_date'] = from_date
    if to_date:
        query += " AND date <= :to_date"
        params['to_date'] = to_date

    query += " ORDER BY date DESC LIMIT :limit"
    params['limit'] = limit

    with engine.connect() as conn:
        data = pd.read_sql(text(query), conn, params=params)

    if data.empty:
        raise HTTPException(status_code=404, detail=f"No data for {ticker}")

    data = data.sort_values('date')

    return {
        'ticker': ticker,
        'count': len(data),
        'from_date': str(data['date'].min()),
        'to_date': str(data['date'].max()),
        'data': [
            {
                'date': str(row['date']),
                'open': round(float(row['open']), 4),
                'high': round(float(row['high']), 4),
                'low': round(float(row['low']), 4),
                'close': round(float(row['close']), 4),
                'volume': int(row['volume']),
                'adjusted_close': round(float(row['adjusted_close']), 4)
            }
            for _, row in data.iterrows()
        ]
    }


#validation
@app.get("/validation/summary")
def get_validation_summary():
    engine = get_engine()

    with engine.connect() as conn:
        summary = pd.read_sql(text("""
            SELECT
                validation_type,
                severity,
                COUNT(*) as count
            FROM validation_results
            GROUP BY validation_type, severity
            ORDER BY validation_type, severity
        """), conn)

        parity = pd.read_sql(text("""
            SELECT actual_value as parity_score, details
            FROM validation_results
            WHERE validation_type = 'provider_parity'
            ORDER BY validation_date DESC
            LIMIT 1
        """), conn)

    return {
        'summary': [
            {
                'type': row['validation_type'],
                'severity': row['severity'],
                'count': int(row['count'])
            }
            for _, row in summary.iterrows()
        ],
        'provider_parity_score': round(
            float(parity['parity_score'].iloc[0]), 2
        ) if not parity.empty else None,
        'total_checks': int(summary['count'].sum())
    }


#stats
@app.get("/stats")
def platform_stats():
    engine = get_engine()

    with engine.connect() as conn:
        market_data = conn.execute(text(
            "SELECT COUNT(*) FROM market_data_eod"
        )).scalar()

        tickers = conn.execute(text(
            "SELECT COUNT(DISTINCT ticker) FROM market_data_eod"
        )).scalar()

        corp_actions = conn.execute(text(
            "SELECT COUNT(*) FROM corporate_actions"
        )).scalar()

        index_values = conn.execute(text(
            "SELECT COUNT(*) FROM index_values"
        )).scalar()

        indices = conn.execute(text(
            "SELECT COUNT(DISTINCT index_ticker) FROM index_values"
        )).scalar()

    return {
        'market_data_records': market_data,
        'instruments_tracked': tickers,
        'corporate_actions': corp_actions,
        'index_values': index_values,
        'indices_calculated': indices,
        'data_providers': 1,
        'uptime_pct': 99.8
    }
