import pandas as pd
from kafka import KafkaConsumer
import plotly.graph_objects as go
import json
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Kafka consumer setup
try:
    consumer = KafkaConsumer(
        'stock-data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: x.decode('utf-8'),  # Decode bytes to string
        max_poll_records=10,  # Maximum number of records to fetch in a single poll
        max_poll_interval_ms=30000,  # Maximum time to wait for new records
        request_timeout_ms=60000  # Timeout for requests to Kafka
    )
except NoBrokersAvailable:
    print("No Kafka brokers available. Please check your Kafka setup.")
    exit(1)

# Kafka consumer setup
consumer = KafkaConsumer(
    'stock-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8')  # Decode bytes to string
)

# Initialize lists to store data
dates = []
symbols = []
opens = []
highs = []
lows = []
closes = []
volumes = []

start_time = time.time()

# Consume messages from Kafka and store data
for message in consumer:
    try:
        data = json.loads(message.value)  # Attempt to parse JSON
        dates.append(data['timestamp'])
        symbols.append(data['symbol'])
        opens.append(float(data['open']))
        highs.append(float(data['high']))
        lows.append(float(data['low']))
        closes.append(float(data['close']))
        volumes.append(int(data['volume']))
    except json.JSONDecodeError as e:
        print(f"Skipping invalid JSON message: {message.value}. Error: {e}")
    except KeyError as e:
        print(f"Skipping message with missing keys: {message.value}. Error: {e}")

    # Break after consuming a certain number of messages or after a timeout
    if len(dates) > 100 or time.time() - start_time > 300:  # 5 minutes timeout
        break

# Convert lists to DataFrame
df = pd.DataFrame({
    'Date': dates,
    'Symbol': symbols,
    'Open': opens,
    'High': highs,
    'Low': lows,
    'Close': closes,
    'Volume': volumes
})

# Ensure 'Date' column is in datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Check for missing values
if df.isnull().values.any():
    print("DataFrame contains missing values. Please check data quality.")

# Check if DataFrame is not empty
if df.empty:
    print("DataFrame is empty. No data to plot.")
else:
    print("DataFrame contains data. Proceeding to plot.")

    # Create Candlestick chart
    fig = go.Figure(data=[go.Candlestick(
        x=df['Date'],
        open=df['Open'],
        high=df['High'],
        low=df['Low'],
        close=df['Close']
    )])

    # Customize layout
    fig.update_layout(
        title="Stock Price Movement",
        xaxis_title="Date",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False,  # Hide range slider
        dragmode='pan',  # Enable panning
        hovermode='x unified',  # Show hover text
        margin=dict(l=20, r=20, t=50, b=20)  # Adjust margins
    )

    fig.show()
