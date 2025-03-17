import requests
import pandas as pd
import time

def fetch_stock_data(api_key, symbol, output_file):
    base_url = 'https://www.alphavantage.co/query?'
    params = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': symbol,
        'interval': '1min',
        'apikey': api_key
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors

        data = response.json()
        # Check if the API returned an error message
        if "Error Message" in data:
            print(f"API Error: {data['Error Message']}")
            return None

        time_series_data = data.get('Time Series (1min)')

        if time_series_data is None:
            print("No Time Series data found in the response.")
            print("Full API Response:", data)  # Print the full response for debugging
            return None

        df = pd.DataFrame.from_dict(time_series_data, orient='index')
        df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        df = df.astype(float)  # Convert columns to numeric type

        # Save to CSV file
        df.to_csv(output_file, index_label='Timestamp')  # Save to CSV with Timestamp as index
        print(f"Stock data saved to {output_file}")
        return df

    except requests.exceptions.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')

    return None

# Alpha Vantage API key and symbol
ALPHA_VANTAGE_API_KEY = 'T6WQ6IBZTH0215V9'  # Replace with your actual API key
STOCK_SYMBOL = 'MSFT'  # Example: Microsoft stock symbol

# Output file path
output_file = 'alpha_vantage_download.csv'  # Path where the CSV file will be saved

# Fetch and save the stock data
df = fetch_stock_data(ALPHA_VANTAGE_API_KEY, STOCK_SYMBOL, output_file)

if df is not None:
    print(f"Data saved in CSV: {output_file}")
else:
    print("Failed to fetch stock data.")
