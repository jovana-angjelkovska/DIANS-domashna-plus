import os
import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
import numpy as np

# PostgreSQL connection setup
POSTGRES_HOST = os.getenv('POSTGRES_HOST', '127.0.0.1')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'RatedR.2002')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres1')

# Connect to PostgreSQL
postgres_connection = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5435/{POSTGRES_DB}'
engine = create_engine(postgres_connection)

# Function to calculate Moving Averages and Trend Predictions
def analyze_stock_data(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.sort_values(by='timestamp', inplace=True)

    results = {}
    for symbol in df['symbol'].unique():
        stock_df = df[df['symbol'] == symbol].copy()

        # Resample the data by day (no resampling needed since we already have daily data)
        stock_df.set_index('timestamp', inplace=True)

        # Calculate Moving Averages (5-day, 10-day)
        stock_df['SMA_5'] = stock_df['close_price'].rolling(window=5).mean()
        stock_df['SMA_10'] = stock_df['close_price'].rolling(window=10).mean()

        # Linear Regression for Trend Prediction (predict next day's close)
        stock_df['timestamp_ordinal'] = stock_df.index.map(pd.Timestamp.toordinal)
        X = stock_df[['timestamp_ordinal']]
        y = stock_df['close_price']

        if len(X) > 1:  # Ensure we have enough data points for regression
            model = LinearRegression()
            model.fit(X, y)
            predicted_price = model.predict([[X.iloc[-1, 0] + 1]])[0]  # Predict next day's price
        else:
            predicted_price = None

        # Generate Buy/Sell/Hold signals based on SMA crossover
        buy_sell_hold = None
        if len(stock_df) >= 10:  # Ensure there are enough data points for a crossover
            # Look at the last available data points for signals
            latest_sma_5 = stock_df['SMA_5'].iloc[-1]
            latest_sma_10 = stock_df['SMA_10'].iloc[-1]

            prev_sma_5 = stock_df['SMA_5'].iloc[-2] if len(stock_df) > 1 else None
            prev_sma_10 = stock_df['SMA_10'].iloc[-2] if len(stock_df) > 1 else None

            if prev_sma_5 is not None and prev_sma_10 is not None:
                # Buy when SMA_5 crosses above SMA_10
                if latest_sma_5 > latest_sma_10 and prev_sma_5 <= prev_sma_10:
                    buy_sell_hold = 'Buy'
                # Sell when SMA_5 crosses below SMA_10
                elif latest_sma_5 < latest_sma_10 and prev_sma_5 >= prev_sma_10:
                    buy_sell_hold = 'Sell'
                # Hold if no significant crossover
                else:
                    buy_sell_hold = 'Hold'
            else:
                buy_sell_hold = 'Hold'  # Not enough data for a valid crossover

        # Save results for the symbol
        results[symbol] = {
            'latest_price': float(stock_df['close_price'].iloc[-1]),
            'SMA_5': float(stock_df['SMA_5'].iloc[-1]) if not pd.isna(stock_df['SMA_5'].iloc[-1]) else None,
            'SMA_10': float(stock_df['SMA_10'].iloc[-1]) if not pd.isna(stock_df['SMA_10'].iloc[-1]) else None,
            'predicted_next_close': float(predicted_price) if predicted_price is not None else None,
            'buy_sell_hold': buy_sell_hold
        }

    return results

# Function to get data from the database
def fetch_data_from_db():
    query = "SELECT * FROM stock_prices"  # Modify this query according to your database schema
    df = pd.read_sql(query, engine)
    return df

# Function to store the analyzed data into the database
def store_data_to_db(analyzed_data):
    analyzed_data_df = pd.DataFrame.from_dict(analyzed_data, orient='index')
    analyzed_data_df.to_sql('data_analysis_results', engine, if_exists='replace', index=True)
    print("Data processed and stored in database: ", analyzed_data_df.head())

# Main function to fetch, analyze, and store data
def analyze_and_store_data():
    print("Starting the analysis...")

    # Fetch data from the database
    df = fetch_data_from_db()

    # Perform data analysis
    analyzed_data = analyze_stock_data(df)

    # Store results in PostgreSQL
    store_data_to_db(analyzed_data)

# Run the analysis
if __name__ == '__main__':
    analyze_and_store_data()
