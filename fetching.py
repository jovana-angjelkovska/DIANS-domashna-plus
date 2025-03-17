import requests
import pandas as pd
from kafka import KafkaProducer
import json
import time

def fetch_stock_data(api_key, symbols, kafka_producer, topic):
    base_url = 'https://www.alphavantage.co/query?'
    
    for symbol in symbols:
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
                print(f"API Error for {symbol}: {data['Error Message']}")
                continue # Skip to the next symbol
        
            time_series_data = data.get('Time Series (1min)')
        
            if time_series_data is None:
                print(f"No Time Series data found for {symbol}.")
                print("Full API Response:", data)  # Print the full response for debugging
                continue # Skip to the next symbol
        
            df = pd.DataFrame.from_dict(time_series_data, orient='index')
            df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            df = df.astype(float)  # Convert columns to numeric type

            # Produce data to Kafka
            for index, row in df.iterrows():
                record = {
                    'timestamp': index,
                    'symbol': symbol,
                    'open': float(row['Open']),  
                    'high': float(row['High']),
                    'low': float(row['Low']),
                    'close': float(row['Close']),
                    'volume': int(row['Volume'])  
                }
                kafka_producer.send(topic, record)
                #print(f"Sent record to topic {topic}: {record}")
                time.sleep(1)  # To avoid hitting API limits

        except requests.exceptions.HTTPError as http_err:
            print(f'HTTP error occurred for {symbol}: {http_err}')
        except Exception as err:
            print(f'Other error occurred for {symbol}: {err}')
    
        # After processing each symbol, print the DataFrame (if any)
        if df is not None:
            print(df)
        else:
            print(f"Failed to fetch stock data for {symbol}.")

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'stock-data'

# Alpha Vantage API key and symbol
ALPHA_VANTAGE_API_KEY = 'G7YX5BO1DKR2NX00'  # Replace with your actual API key
STOCK_SYMBOLS = ['MSFT', 'AAPL', 'GOOGL'] # Define a list of symbols

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fetch and display the stock data
fetch_stock_data(ALPHA_VANTAGE_API_KEY, STOCK_SYMBOLS, producer, KAFKA_TOPIC) # Pass the list of symbols

producer.flush()  # Ensure all messages are sent
producer.close()
