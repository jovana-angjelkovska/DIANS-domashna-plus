import pandas as pd
from kafka import KafkaConsumer
import plotly.graph_objects as go
import json
import time

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'stock-data'

# Initialize lists to store data
dates, symbols, opens, highs, lows, closes, volumes = [], [], [], [], [], [], []

start_time = time.time()
timeout_duration = 60  # Exit after 60 seconds if no messages are received

# Kafka consumer setup
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000  # Avoids infinite wait by timing out after 5 seconds
    )
    print(f"Connected to Kafka broker at {KAFKA_BROKER}. Listening to topic '{KAFKA_TOPIC}'...")
except Exception as e:
    print(f"Failed to connect to Kafka broker: {e}")
    exit(1)

# Consume messages from Kafka and store data
while True:
    msg_received = False
    for message in consumer:
        msg_received = True
        try:
            data = message.value
            dates.append(data['timestamp'])
            symbols.append(data['symbol'])
            opens.append(float(data['open']))
            highs.append(float(data['high']))
            lows.append(float(data['low']))
            closes.append(float(data['close']))
            volumes.append(int(data['volume']))
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Skipping invalid message: {message.value}. Error: {e}")

        if len(dates) > 100 or time.time() - start_time > timeout_duration:
            print("Collected enough data, exiting...")
            break

    if not msg_received:
        print("No messages received in the last 5 seconds. Exiting...")
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

df['Date'] = pd.to_datetime(df['Date'])

if df.empty:
    print("No data received. Exiting...")
    exit()

# Create and show the candlestick chart
fig = go.Figure(data=[go.Candlestick(
    x=df['Date'], open=df['Open'], high=df['High'], low=df['Low'], close=df['Close']
)])

fig.update_layout(
    title="Stock Price Movement",
    xaxis_title="Date",
    yaxis_title="Price",
    xaxis_rangeslider_visible=False,
    dragmode='pan',
    hovermode='x unified',
    margin=dict(l=20, r=20, t=50, b=20)
)

fig.show()
