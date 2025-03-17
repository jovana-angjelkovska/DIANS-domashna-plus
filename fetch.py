import requests
import pandas as pd

API_KEY = 'T6WQ6IBZTH0215V9'
BASE_URL = 'https://www.alphavantage.co/query?'

def fetch_stock_data(symbol, interval='TIME_SERIES_WEEKLY'):
    parameters = {
        'function': interval,
        'symbol': symbol,
        'apikey': API_KEY
    }
    
    response = requests.get(BASE_URL, params=parameters)
    data = response.json()
    
    if 'Weekly Time Series' not in data:
        raise ValueError('Invalid API response. Make sure the symbol is correct and try again.')
        
    ts_data = data['Weekly Time Series']
    
    # Convert to DataFrame
    df = pd.DataFrame(ts_data).T
    df.index = pd.to_datetime(df.index)
    
    # Rename columns for better readability
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    
    return df

# Fetch weekly data for Microsoft
msft_data = fetch_stock_data('MSFT')

# Define the date range
start_date = '2024-03-01'
end_date = '2025-03-01'

# Filter data by date
filtered_data = msft_data[(msft_data.index >= start_date) & (msft_data.index <= end_date)]

# Save the filtered DataFrame as a CSV file
output_file_path = r"C:\Users\Jovana\Desktop\DIANS domashna plus\alpha_vantage.csv"
filtered_data.to_csv(output_file_path, index=True)

print(f"Filtered weekly data from {start_date} to {end_date} saved to {output_file_path}")
