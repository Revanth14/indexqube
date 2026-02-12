from polygon import RESTClient
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv('../.env')

client = RESTClient(api_key=os.getenv('MASSIVE_API_KEY'))


print("Testing with 2-year range (free tier)...\n")

try:
    aggs = client.get_aggs(
        ticker="AAPL",
        multiplier=1,
        timespan='day',
        from_='2023-01-01',
        to='2024-12-31',
        limit=1000
    )
    
    print("Date         Close      Volume")
    print("-" * 40)
    count = 0
    for agg in aggs:
        if count < 5:  # Show first 5
            date = datetime.fromtimestamp(agg.timestamp / 1000).strftime('%Y-%m-%d')
            print(f"{date}   ${agg.close:<8.2f}  {agg.volume:,}")
        count += 1
    
    print(f"...\nTotal records: {count}")
    print(f"\nFree tier works! {count} days of data")

except Exception as e:
    print(f"Error: {e}")