import pandas as pd

# --- Load holdings data ---
input_file = '/Users/albertredmann/Library/CloudStorage/OneDrive-Persönlich/Desktop/QFE/Projects/13f/Data/TigerGlobal_holdings_price_changes.csv'
output_file = '/Users/albertredmann/Library/CloudStorage/OneDrive-Persönlich/Desktop/QFE/Projects/13f/Data/TigerGlobal_holdings_all.csv'

df = pd.read_csv(input_file, parse_dates=['filing_date'])
df = df.sort_values(['symbol', 'filing_date'])
df['hld_pct_change'] = df.groupby('symbol')['value'].pct_change() * 100

# control
print(df.head(10))

df.to_csv(output_file, index=False)
