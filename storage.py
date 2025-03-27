import requests
import pandas as pd
from kafka import KafkaProducer
import json
import time
import psycopg2
from psycopg2 import pool

DB_CONFIG = {
    'primary': {
        'database': 'primary_postgres',
        'user': 'postgres',
        'password': 'diansdomashna',
        'host': 'localhost',
        'port': '5440'
    },
    'secondary': {
        'database': 'secondary_postgres',
        'user': 'postgres',
        'password': 'diansdomashna',
        'host': 'localhost',
        'port': '5441'
    }
}

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'stock-data'

ALPHA_VANTAGE_API_KEY = 'G7YX5BO1DKR2NX00'
STOCK_SYMBOLS = ['MSFT', 'AAPL', 'GOOGL']

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class DatabaseManager:
    def __init__(self):
        self.primary_pool = self._create_pool(DB_CONFIG['primary'])
        self.secondary_pool = self._create_pool(DB_CONFIG['secondary'])
        
    def _create_pool(self, config):
        return psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            **config
        )
    
    def get_connections(self):
        try:
            conn_primary = self.primary_pool.getconn()
            conn_secondary = self.secondary_pool.getconn()
            
            with conn_primary.cursor() as cursor:
                cursor.execute('SELECT 1')
            with conn_secondary.cursor() as cursor:
                cursor.execute('SELECT 1')
            
            return conn_primary, conn_secondary
        except Exception as e:
            print(f"Connection failed: {str(e)}")
            raise

def create_table(conn):
    cur = conn.cursor()
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
            UNIQUE (symbol, timestamp)
        )
    """)
    return cur

def fetch_and_store_stock_data(api_key, symbols, kafka_producer, topic):
    base_url = 'https://www.alphavantage.co/query?'
    db_manager = DatabaseManager()
    
    for symbol in symbols:
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': symbol,
            'interval': '60min',
            'apikey': api_key
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()

            data = response.json()
            if "Error Message" in data:
                print(f"API Error for {symbol}: {data['Error Message']}")
                continue

            time_series_data = data.get('Time Series (60min)')
            if time_series_data is None:
                print(f"No Time Series data found for {symbol}.")
                print("Full API Response:", data)
                continue

            df = pd.DataFrame.from_dict(time_series_data, orient='index')
            df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            df = df.astype(float)

            try:
                conn_primary, conn_secondary = db_manager.get_connections()
                cur_primary = create_table(conn_primary)
                cur_secondary = create_table(conn_secondary)

                for index, row in df.iterrows():

                    try:
                        cur_primary.execute("""
                            INSERT INTO stock_prices 
                            (symbol, timestamp, open_price, high_price, low_price, close_price, volume) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (symbol, index, float(row['Open']), float(row['High']), 
                             float(row['Low']), float(row['Close']), int(row['Volume'])))
                        conn_primary.commit()
                    except Exception as e:
                        print(f"Primary DB insert failed: {str(e)}")

                    try:
                        cur_secondary.execute("""
                            INSERT INTO stock_prices 
                            (symbol, timestamp, open_price, high_price, low_price, close_price, volume) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (symbol, index, float(row['Open']), float(row['High']), 
                             float(row['Low']), float(row['Close']), int(row['Volume'])))
                        conn_secondary.commit()
                    except Exception as e:
                        print(f"Secondary DB insert failed: {str(e)}")

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
                    time.sleep(1)

            finally:
                if conn_primary:
                    cur_primary.close()
                    db_manager.primary_pool.putconn(conn_primary)
                if conn_secondary:
                    cur_secondary.close()
                    db_manager.secondary_pool.putconn(conn_secondary)

        except requests.exceptions.HTTPError as http_err:
            print(f'HTTP error occurred for {symbol}: {http_err}')
        except Exception as err:
            print(f'Other error occurred for {symbol}: {err}')

fetch_and_store_stock_data(ALPHA_VANTAGE_API_KEY, STOCK_SYMBOLS, producer, KAFKA_TOPIC)

producer.flush()
producer.close()
