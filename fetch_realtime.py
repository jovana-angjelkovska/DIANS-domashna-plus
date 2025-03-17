from kafka import KafkaProducer
from alpha_vantage.timeseries import TimeSeries
import json
import time

# Alpha Vantage API Key
API_KEY = 'T6WQ6IBZTH0215V9'

# Initialize Alpha Vantage TimeSeries client
ts = TimeSeries(key=API_KEY, output_format='json')

# Fetch weekly stock data for a specific symbol
def fetch_weekly_data(symbol):
    try:
        data, meta_data = ts.get_weekly(symbol=symbol)
        print("Full API Response:", json.dumps(data, indent=4))  # Debugging: Print full response
        
        # Handle Errors in Response
        if 'Error Message' in data:
            print("Error Message from API:", data['Error Message'])
            return None
        elif 'Note' in data:
            print("Note from API:", data['Note'])  # Likely a rate limit issue
            return None
        elif 'Weekly Time Series' not in data:
            print(f"Expected key 'Weekly Time Series' not found for {symbol}.")
            return None
        
        # Return Valid Data
        return data['Weekly Time Series']
    except Exception as e:
        print(f"Error fetching weekly data: {e}")
        return None

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON messages for Kafka
)

# List of stock symbols
stock_symbols = ['MSFT', 'ORCL', 'CRM', 'SNPS', 'PANW']

# Fetch stock data for each symbol
for stock_symbol in stock_symbols:
    stock_data = fetch_weekly_data(stock_symbol)
    
    if stock_data is not None:
        for timestamp, values in stock_data.items():
            try:
                message = {
                    'timestamp': timestamp,
                    'symbol': stock_symbol,
                    'open': values['1. open'],
                    'high': values['2. high'],
                    'low': values['3. low'],
                    'close': values['4. close'],
                    'volume': values['5. volume']
                }
                print(f"Sending message: {message}")
                producer.send('stock-data', value=message)
                time.sleep(12)  # Sleep to avoid hitting rate limits
            except Exception as e:
                print(f"Error sending message to Kafka: {e}")
    else:
        print(f"No valid weekly stock data fetched for {stock_symbol}.")

print("All messages sent successfully.")
