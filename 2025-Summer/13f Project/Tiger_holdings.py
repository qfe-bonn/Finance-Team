import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import time

base_url = "https://13f.info"
manager_url = f"{base_url}/manager/0001167483-tiger-global-management-llc"

start_time = time.time()

# Step 1: Get all quarter links and associated data
response = requests.get(manager_url)
response.raise_for_status()
soup = BeautifulSoup(response.text, 'html.parser')

quarter_links = []
quarter_names = []
filing_dates = []

table = soup.find('table')
if table:
    rows = table.find_all('tr')[1:]  # skip header
    for row in rows:
        quarter_cell = row.find('td')
        if quarter_cell:
            quarter_name = quarter_cell.get_text(strip=True)
            link = quarter_cell.find('a')
            if link and 'href' in link.attrs:
                href = link['href']
                if "new-holdings" not in href.lower():
                    quarter_links.append(href)
                    quarter_names.append(quarter_name)
                    date_cell = row.find_all('td')[5]
                    if date_cell:
                        filing_date_str = date_cell.get_text(strip=True)
                        try:
                            filing_date = datetime.strptime(filing_date_str, '%m/%d/%Y').date()
                        except ValueError:
                            print(f"Warning: Could not parse filing date: {filing_date_str}")
                            filing_date = None
                        filing_dates.append(filing_date)
                    else:
                        filing_dates.append(None)

# Step 2: Function to get all holdings from a single quarter
def get_all_holdings(quarter_href):
    page_url = base_url + quarter_href
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    response = requests.get(page_url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch page: {page_url}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'id': 'filingAggregated'})
    if not table:
        print(f"No aggregated table found in {page_url}")
        return None

    data_url = table.get('data-url')
    if not data_url:
        print(f"No data-url found at {page_url}")
        return None

    json_url = base_url + data_url
    json_response = requests.get(json_url, headers=headers)
    if json_response.status_code != 200:
        print(f"Failed to fetch JSON from: {json_url}")
        return None

    data_json = json_response.json()
    columns = ['symbol', 'issuer_name', 'class', 'cusip', 'value', 'percentage', 'shares', 'principal', 'option_type']
    df = pd.DataFrame(data_json['data'], columns=columns)
    df['symbol'] = df['symbol'].str.strip()

    # Return selected columns
    return df[['symbol', 'percentage', 'value', 'option_type']]

# Step 3: Loop over all quarters and build dataset
all_data = []

for href, quarter, f_date in zip(quarter_links, quarter_names, filing_dates):
    print(f"Processing {quarter}...")
    holdings_df = get_all_holdings(href)
    if holdings_df is not None:
        holdings_df['filing_date'] = f_date
        holdings_df['quarter'] = quarter
        all_data.append(holdings_df)

# Step 4: Combine and save
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)

    # Reorder columns
    cols = ['filing_date', 'quarter', 'symbol', 'percentage', 'value']
    combined_df = combined_df[cols]

    # Save to CSV
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, 'TigerGlobal_all_holdings.csv')
    combined_df.to_csv(csv_path, index=False)

    end_time = time.time()
    print(f"\nData pull completed in {end_time - start_time:.2f} seconds.")
    print(f"All data saved to {csv_path}")
else:
    print("No data collected.")

