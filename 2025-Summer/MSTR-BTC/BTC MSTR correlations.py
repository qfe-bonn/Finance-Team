
import yfinance as yf
import pandas as pd
import numpy as np

# yfinance price data Download
btc = yf.download("BTC-USD", auto_adjust=True)
mstr = yf.download("MSTR", auto_adjust=True)

# Align the dates and combine the series dropping rows for which not both datapoints are present (especially weekends since dealing with crypto)
prices = pd.concat([mstr['Close'], btc['Close']], axis=1, join='inner')
prices.columns = ['MSTR', 'BTC']

# Ensure prices index is timezone-naive
if prices.index.tz is not None:
    prices.index = prices.index.tz_localize(None)

# Enter earnings dates manually. As per documentation on Strategy's website, they first started buying on August 20 2020
manual_earnings_dates = [
    # 2020
    '2020-07-28',  
    '2020-10-27',  
    
    # 2021
    '2021-01-28',  
    '2021-04-29',  
    '2021-07-29',  
    '2021-10-28',  
    
    # 2022
    '2022-02-01',  
    '2022-05-03',  
    '2022-08-02',  
    '2022-11-01',  
    
    # 2023
    '2023-02-02',  
    '2023-05-01',  
    '2023-08-01',  
    '2023-11-01',  
    
    # 2024
    '2024-02-06',  
    '2024-04-29',  
    '2024-08-01',  
    '2024-10-30',  
    
    # 2025
    '2025-02-05',  
    '2025-05-01',  
    '2025-07-30',  
]

# Convert to datetime and ensure timezone-naive
earnings_dates = pd.to_datetime(manual_earnings_dates).tz_localize(None).sort_values()

# Remove future dates and keep only those within price data range
earnings_dates = earnings_dates[earnings_dates <= prices.index.max()]
earnings_dates = earnings_dates.to_list()

# Add the start of price data to capture the initial period
all_periods = [prices.index.min()] + earnings_dates + [pd.to_datetime('today')]

# Remove first row, because its before bitcoin buying started
all_periods = all_periods[1:]

# Compute correlation between earnings dates
results = []

for i in range(len(all_periods) - 1):
    start = all_periods[i]
    end = all_periods[i + 1]
    mask = (prices.index > start) & (prices.index <= end)
    subset = prices.loc[mask]

    if len(subset) > 1:
        corr = subset.corr().iloc[0, 1]
        results.append({
            "Start": start.date(),
            "End": end.date(),
            "Correlation": corr,
            "Num Days": len(subset)
        })

# Add quarter names in left column
for row in results:
    year = row["Start"].year
    month = row["Start"].month
    quarter = (month - 1) // 3 + 1
    row["Quarter"] = f"{year}Q{quarter}"

# Output results
correlation_df = pd.DataFrame(results)

# Reorder columns to put Quarter first
cols = ['Quarter'] + [col for col in correlation_df.columns if col != 'Quarter']
correlation_df = correlation_df[cols]

print(correlation_df)
print('Current quarter Q2/2025 is not concluded, final value will change')

# Save resulting df as csv
out_dir = '~/Downloads/mstr_btc_earnings_correlation.csv'
# Specify the downlaod path according to your needs. Note that I wrote this script on MacOS, so this specific path wont work if you are on Windows
correlation_df.to_csv(out_dir, index=False) 
print(f'Correlation Dataframe saved under: ' + out_dir)