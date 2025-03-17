import requests
import pandas as pd
from kafka import KafkaProducer
import json
import time
import psycopg2

# PostgreSQL Database Credentials
POSTGRES_DB = 'postgres2'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'RatedR.2002'
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5434'

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'stock-data'

# Alpha Vantage API key and symbols
ALPHA_VANTAGE_API_KEY = 'G7YX5BO1DKR2NX00'  # Replace with your actual API key
STOCK_SYMBOLS = ['MSFT', 'AAPL', 'GOOGL']  # Define a list of symbols

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_and_store_stock_data(api_key, symbols, kafka_producer, topic):
    base_url = 'https://www.alphavantage.co/query?'
    
    # Establish database connection once outside the loop
    conn = psycopg2.connect(
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cur = conn.cursor()

    # Create table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(255),
            timestamp TIMESTAMP,
            open_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            close_price FLOAT,
            volume INT,
            UNIQUE (symbol, timestamp) -- This line enforces uniqueness
        )
    """)

    for symbol in symbols:  # Iterate through each symbol in the list
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
                continue  # Skip to the next symbol

            time_series_data = data.get('Time Series (1min)')

            if time_series_data is None:
                print(f"No Time Series data found for {symbol}.")
                print("Full API Response:", data)  # Print the full response for debugging
                continue  # Skip to the next symbol

            df = pd.DataFrame.from_dict(time_series_data, orient='index')
            df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            df = df.astype(float)  # Convert columns to numeric type

            # Insert data into the table
            for index, row in df.iterrows():
                if symbol not in STOCK_SYMBOLS:  # Check if the symbol is valid
                    print(f"Skipping invalid symbol: {symbol}")
                    continue  # Skip any unintended combined symbols
                cur.execute("""
                    INSERT INTO stock_prices (symbol, timestamp, open_price, high_price, low_price, close_price, volume) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (symbol, index, float(row['Open']), float(row['High']), float(row['Low']), float(row['Close']), int(row['Volume'])))

            conn.commit()  # Commit after processing each symbol

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
                print(f"Sent record to topic {topic} for {symbol}: {record}")
                time.sleep(1)  # To avoid hitting API limits

        except requests.exceptions.HTTPError as http_err:
            print(f'HTTP error occurred for {symbol}: {http_err}')
        except Exception as err:
            print(f'Other error occurred for {symbol}: {err}')

    cur.close()  # Close cursor after all operations are done
    conn.close()  # Close connection after all operations are done

# Fetch and display the stock data for multiple symbols
fetch_and_store_stock_data(ALPHA_VANTAGE_API_KEY, STOCK_SYMBOLS, producer, KAFKA_TOPIC)

producer.flush()  # Ensure all messages are sent
producer.close()
