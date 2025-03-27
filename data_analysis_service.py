import os
import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
import numpy as np
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

DB_CONFIG = {
    'primary': {
        'database': 'primary_postgres',
        'user': 'postgres',
        'password': 'diansdomashna',
        'host': '127.0.0.5',  
        'port': '5440'               
    }
}

postgres_connection = f'postgresql://{DB_CONFIG["primary"]["user"]}:{DB_CONFIG["primary"]["password"]}@{DB_CONFIG["primary"]["host"]}:{DB_CONFIG["primary"]["port"]}/{DB_CONFIG["primary"]["database"]}'
engine = create_engine(postgres_connection)

kafka_producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BROKER', 'localhost:9092'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def analyze_stock_data(df):
    """Analyze stock data with moving averages and trend predictions"""
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.sort_values(by='timestamp', inplace=True)

    results = {}
    for symbol in df['symbol'].unique():
        stock_df = df[df['symbol'] == symbol].copy()
        
        if len(stock_df) < 10:
            continue  

        stock_df.set_index('timestamp', inplace=True)
        stock_df['SMA_5'] = stock_df['close_price'].rolling(window=5).mean()
        stock_df['SMA_10'] = stock_df['close_price'].rolling(window=10).mean()

        stock_df['timestamp_ordinal'] = stock_df.index.map(pd.Timestamp.toordinal)
        X = stock_df[['timestamp_ordinal']]
        y = stock_df['close_price']

        if len(X) > 1:
            model = LinearRegression()
            model.fit(X, y)
            next_day_ordinal = X.iloc[-1, 0] + 1
            predicted_price = model.predict([[next_day_ordinal]])[0]
        else:
            predicted_price = None

        buy_sell_hold = None
        if len(stock_df) >= 10:
            latest_sma_5 = stock_df['SMA_5'].iloc[-1]
            latest_sma_10 = stock_df['SMA_10'].iloc[-1]
            
            prev_sma_5 = stock_df['SMA_5'].iloc[-2] if len(stock_df) > 1 else None
            prev_sma_10 = stock_df['SMA_10'].iloc[-2] if len(stock_df) > 1 else None

            if prev_sma_5 is not None and prev_sma_10 is not None:
                if latest_sma_5 > latest_sma_10 and prev_sma_5 <= prev_sma_10:
                    buy_sell_hold = 'Buy'
                elif latest_sma_5 < latest_sma_10 and prev_sma_5 >= prev_sma_10:
                    buy_sell_hold = 'Sell'
                else:
                    buy_sell_hold = 'Hold'
            else:
                buy_sell_hold = 'Hold'

        results[symbol] = {
            'latest_price': float(stock_df['close_price'].iloc[-1]),
            'SMA_5': float(stock_df['SMA_5'].iloc[-1]) if not pd.isna(stock_df['SMA_5'].iloc[-1]) else None,
            'SMA_10': float(stock_df['SMA_10'].iloc[-1]) if not pd.isna(stock_df['SMA_10'].iloc[-1]) else None,
            'predicted_next_close': float(predicted_price) if predicted_price is not None else None,
            'buy_sell_hold': buy_sell_hold,
            'analysis_date': datetime.now().isoformat()
        }

    return results

def fetch_data_from_db():
    """Retrieve stock price data from PostgreSQL database"""
    query = """
        SELECT 
            symbol,
            timestamp,
            close_price
        FROM 
            stock_prices
        WHERE 
            timestamp >= NOW() - INTERVAL '30 days'
        ORDER BY 
            symbol, timestamp
    """
    return pd.read_sql(query, engine)

def store_data_to_db(analyzed_data):
    """Store analysis results in PostgreSQL database"""
    analyzed_df = pd.DataFrame.from_dict(analyzed_data, orient='index')
    analyzed_df.to_sql(
        'data_analysis_results',
        engine,
        if_exists='replace',
        index=True,
        index_label='symbol'
    )
    print(f"Analysis results stored for {len(analyzed_df)} symbols")

def analyze_and_store_data():
    """Main workflow for data analysis pipeline"""
    print("Starting stock analysis pipeline...")
    
    try:
        raw_data = fetch_data_from_db()
        
        if raw_data.empty:
            print("No data available for analysis")
            return
        
        analysis_results = analyze_stock_data(raw_data)
        
        store_data_to_db(analysis_results)
        
        for symbol, result in analysis_results.items():
            kafka_producer.send('analysis_results', result)
        
        print("Analysis completed successfully")
        
    except Exception as e:
        print(f"Analysis failed: {str(e)}")

if __name__ == '__main__':
    analyze_and_store_data()
