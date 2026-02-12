import pandas as pd
import sqlalchemy as sa
import os
from dotenv import load_dotenv
import requests
from io import StringIO
load_dotenv('../.env')

print("Fetching S&P 500 tickers...")

url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
headers = {'User-Agent': 'Mozilla/5.0'}
response = requests.get(url, headers=headers)
df = pd.read_csv(StringIO(response.text))

# Map correct columns
tickers = pd.DataFrame({
    'ticker': df['Symbol'].str.replace('.', '-', regex=False),
    'company_name': df['Security'],
    'sector': df['GICS Sector'],
    'industry': df['GICS Sub-Industry'],
    'is_active': True
})

engine = sa.create_engine(
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

tickers.to_sql('sp500_constituents', engine, if_exists='replace', index=False)

count = pd.read_sql("SELECT COUNT(*) as count FROM sp500_constituents", engine)
print(f"Loaded {count['count'][0]} S&P 500 tickers")
print("Sample:")
print(tickers[['ticker', 'company_name', 'sector']].head(10).to_string(index=False))