from flask import Flask, jsonify
from kafka import KafkaConsumer
import json
import threading
import requests
import pandas as pd
import psycopg2

app = Flask(__name__)

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'stock-data'

# PostgreSQL Database Credentials
POSTGRES_DB = 'postgres'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'RatedR.2002'
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5433'

# Alpha Vantage API key and symbols
ALPHA_VANTAGE_API_KEY = 'HCOXSFIJBJP2SDIA'  # Replace with your actual API key
STOCK_SYMBOLS = ['MSFT', 'AAPL', 'GOOGL']  # Define a list of symbols

# Initialize lists to store data
stock_prices = []  # Changed from stock_data to stock_prices

def consume_kafka_messages():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='data-analysis-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    print(f"Connected to Kafka broker at {KAFKA_BROKER}. Listening to topic '{KAFKA_TOPIC}'...")

    for message in consumer:
        try:
            data = message.value
            stock_prices.append(data)  # Changed from stock_data to stock_prices
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Skipping invalid message: {message.value}. Error: {e}")

# Start Kafka consumer in a separate thread
threading.Thread(target=consume_kafka_messages, daemon=True).start()

def fetch_and_store_stock_data(api_key, symbols):
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
            volume INT
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
                cur.execute("""
                    INSERT INTO stock_prices (symbol, timestamp, open_price, high_price, low_price, close_price, volume) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (symbol, index, float(row['Open']), float(row['High']), float(row['Low']), float(row['Close']), int(row['Volume'])))

            conn.commit()  # Commit after processing each symbol

        except requests.exceptions.HTTPError as http_err:
            print(f'HTTP error occurred for {symbol}: {http_err}')
        except Exception as err:
            print(f'Other error occurred for {symbol}: {err}')

    cur.close()  # Close cursor after all operations are done
    conn.close()  # Close connection after all operations are done

@app.route('/')
def home():
    return "Welcome to the Stock Data API! Visit /api/analyzed-data to view the data."

@app.route('/favicon.ico')
def favicon():
    return '', 204  # Optional: Handle favicon requests

@app.route('/api/analyzed-data', methods=['GET'])
def get_analyzed_data():
    return jsonify(stock_prices)  # Return stock_prices instead of stock_data

if __name__ == '__main__':
    fetch_and_store_stock_data(ALPHA_VANTAGE_API_KEY, STOCK_SYMBOLS)
    app.run(host='0.0.0.0', port=4000)
