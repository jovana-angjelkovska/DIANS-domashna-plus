from fastapi import FastAPI
from kafka import KafkaProducer
import requests
import json

app = FastAPI()

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# API Configuration
API_URL = "https://www.alphavantage.co/query"
API_KEY = "T6WQ6IBZTH0215V9"

@app.get("/fetch-data/{symbol}")
def fetch_data(symbol: str):
    response = requests.get(
        API_URL,
        params={
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": "1min",
            "apikey": API_KEY
        }
    )
    data = response.json()
    producer.send("financial_data", value=data)
    return {"status": "Data sent to Kafka"}

#T6WQ6IBZTH0215V9