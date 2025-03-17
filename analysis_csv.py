import pandas as pd
import json
from kafka import KafkaProducer
from sklearn.linear_model import LinearRegression
from sqlalchemy import create_engine
import psycopg2

# PostgreSQL Database Credentials
POSTGRES_DB = 'postgres'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'RatedR.2002'
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5433'

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC_ANALYSIS = 'stock-analysis'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Connect to PostgreSQL and fetch stock data using SQLAlchemy
def fetch_stock_data():
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    query = """
        SELECT symbol, timestamp, open_price, high_price, low_price, close_price, volume
        FROM stock_prices
        ORDER BY timestamp DESC
    """
    df = pd.read_sql(query, engine)
    return df

# Calculate Moving Averages and Trends
def analyze_stock_data(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.sort_values(by='timestamp', inplace=True)

    results = {}
    for symbol in df['symbol'].unique():
        stock_df = df[df['symbol'] == symbol].copy()

        # Calculate Moving Averages
        stock_df['SMA_5'] = stock_df['close_price'].rolling(window=5).mean()
        stock_df['SMA_10'] = stock_df['close_price'].rolling(window=10).mean()

        # Linear Regression for Trend Prediction
        stock_df['timestamp_ordinal'] = stock_df['timestamp'].map(pd.Timestamp.toordinal)
        X = stock_df[['timestamp_ordinal']]
        y = stock_df['close_price']

        if len(X) > 1:  # Ensure we have enough data points
            model = LinearRegression()
            model.fit(X, y)
            predicted_price = model.predict([[X.iloc[-1, 0] + 1]])[0]
        else:
            predicted_price = None

        results[symbol] = {
            'latest_price': float(stock_df['close_price'].iloc[-1]),
            'SMA_5': float(stock_df['SMA_5'].iloc[-1]) if not pd.isna(stock_df['SMA_5'].iloc[-1]) else None,
            'SMA_10': float(stock_df['SMA_10'].iloc[-1]) if not pd.isna(stock_df['SMA_10'].iloc[-1]) else None,
            'predicted_next_close': float(predicted_price) if predicted_price is not None else None
        }

    return results

# Store Analysis Results in PostgreSQL
def store_analysis_results(results):
    conn = psycopg2.connect(
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_analysis (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(255),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            latest_price FLOAT,
            sma_5 FLOAT,
            sma_10 FLOAT,
            predicted_next_close FLOAT
        )
    """)

    for symbol, data in results.items():
        cur.execute("""
            INSERT INTO stock_analysis (symbol, latest_price, sma_5, sma_10, predicted_next_close)
            VALUES (%s, %s, %s, %s, %s)
        """, (symbol, data['latest_price'], data['SMA_5'], data['SMA_10'], data['predicted_next_close']))

    conn.commit()
    cur.close()
    conn.close()

# Send Analysis Results to Kafka
def send_results_to_kafka(results):
    for symbol, data in results.items():
        record = {
            'symbol': symbol,
            'latest_price': data['latest_price'],
            'SMA_5': data['SMA_5'],
            'SMA_10': data['SMA_10'],
            'predicted_next_close': data['predicted_next_close']
        }
        producer.send(KAFKA_TOPIC_ANALYSIS, record)
        print(f"Sent analysis to Kafka: {record}")

# Generate Report (CSV)
def generate_report(results):
    df_report = pd.DataFrame(results).T
    df_report.to_csv('stock_analysis_report.csv', index=True)
    print("Report generated: stock_analysis_report.csv")

# Main Function
def main():
    print("Fetching stock data...")
    stock_data = fetch_stock_data()
    
    if stock_data.empty:
        print("No stock data available.")
        return

    print("Analyzing stock data...")
    analysis_results = analyze_stock_data(stock_data)

    print("Storing analysis results in PostgreSQL...")
    store_analysis_results(analysis_results)

    print("Sending results to Kafka...")
    send_results_to_kafka(analysis_results)

    print("Generating report...")
    generate_report(analysis_results)

if __name__ == "__main__":
    main()
