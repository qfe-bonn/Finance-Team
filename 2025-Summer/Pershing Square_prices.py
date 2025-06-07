import pandas as pd
import yfinance as yf
from datetime import timedelta
import os
import time

# --- Load holdings data ---
input_file = '/Users/admin/Library/CloudStorage/OneDrive-Persönlich/Desktop/QFE/Projects/13f/Data/Pershing_all_holdings.csv'  # Input file
output_file = '/Users/admin/Library/CloudStorage/OneDrive-Persönlich/Desktop/QFE/Projects/13f/Data/Pershing_holdings_price_changes.csv'  # Output file

df = pd.read_csv(input_file, parse_dates=['filing_date'])

# --- Add empty columns for price data ---
df['price_before'] = None
df['price_after'] = None
df['price_change_pct'] = None

# --- Cache for results to avoid redundant requests ---
price_cache = {}

# --- Group entries by unique (symbol, filing_date) ---
unique_requests = df[['symbol', 'filing_date']].drop_duplicates().reset_index(drop=True)

# --- Helper: fetch price for a single symbol-date pair ---
def get_price_change(symbol, filing_date):
    if not isinstance(symbol, str) or pd.isna(symbol):
        print(f"Invalid symbol: {symbol} on {filing_date}")
        return None, None, None
    yf_symbol = symbol  # Modify only if needed
    start_date = filing_date - timedelta(days=5)
    end_date = filing_date + timedelta(days=5)

    try:
        data = yf.download(yf_symbol, start=start_date, end=end_date, progress=False)
        if data.empty:
            return None, None, None

        # Normalize index and sort to avoid hidden time components
        data.index = pd.to_datetime(data.index).normalize()
        data = data.sort_index()

        filing_date = pd.to_datetime(filing_date).normalize()

        trading_days = data.index.date
        before_days = [d for d in trading_days if d < filing_date.date()]
        after_days = [d for d in trading_days if d > filing_date.date()]

        if not before_days or not after_days:
            return None, None, None

        date_before = pd.Timestamp(before_days[-1])
        date_after = pd.Timestamp(after_days[0])

        # Extract scalar safely using .loc + .item()
        price_before = data.loc[date_before, 'Close']
        price_after = data.loc[date_after, 'Close']

        # If multiple rows (shouldn't happen), reduce to scalar
        if isinstance(price_before, pd.Series):
            price_before = price_before.iloc[0]
        if isinstance(price_after, pd.Series):
            price_after = price_after.iloc[0]

        if pd.isna(price_before) or price_before == 0:
            return price_before, price_after, None

        price_change_pct = ((price_after - price_before) / price_before) * 100
        return price_before, price_after, price_change_pct

    except Exception as e:
        print(f"Error for {symbol} on {filing_date.date()}: {e}")
        return None, None, None




# --- Process in batches of 5 ---
batch_size = 5
for i in range(0, len(unique_requests), batch_size):
    batch = unique_requests.iloc[i:i+batch_size]
    print(f"Processing batch {i // batch_size + 1}/{(len(unique_requests) - 1) // batch_size + 1}")

    for _, row in batch.iterrows():
        symbol, filing_date = row['symbol'], row['filing_date']
        cache_key = (symbol, filing_date.date())

        if cache_key not in price_cache:
            price_before, price_after, pct = get_price_change(symbol, filing_date)
            price_cache[cache_key] = (price_before, price_after, pct)

    time.sleep(1)  # Pause between batches

# --- Fill in DataFrame using cached results ---
for idx, row in df.iterrows():
    key = (row['symbol'], row['filing_date'].date())
    if key in price_cache:
        price_before, price_after, pct = price_cache[key]
        df.at[idx, 'price_before'] = price_before
        df.at[idx, 'price_after'] = price_after
        df.at[idx, 'price_change_pct'] = pct

# --- Save final result ---
df.to_csv(output_file, index=False)
print(f"\nDone. Saved price-enriched file to: {output_file}")
