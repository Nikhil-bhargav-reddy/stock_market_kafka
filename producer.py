import requests
import json
import time
import logging
from kafka import KafkaProducer
from prometheus_client import start_http_server, Counter

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
API_KEY = "<YOUR API KEY>"
BASE_URL = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
KAFKA_TOPIC = 'stock_market_data'
SYMBOLS = ["AAPL", "GOOGL", "MSFT"]
METRICS_PORT = 8000
FETCH_INTERVAL = 60  # seconds

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Prometheus metrics
messages_produced = Counter('messages_produced', 'Number of messages produced')
api_calls = Counter('api_calls', 'Number of API calls made')

def fetch_latest_stock_data(symbol):
    """Fetch the latest stock data for a given symbol."""
    url = BASE_URL.format(symbol=symbol, API_KEY=API_KEY)
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if "Time Series (Daily)" in data:
            latest_date = max(data["Time Series (Daily)"].keys())
            latest_data = data["Time Series (Daily)"][latest_date]
            
            return {
                "symbol": symbol,
                "date": latest_date,
                "open": latest_data["1. open"],
                "high": latest_data["2. high"],
                "low": latest_data["3. low"],
                "close": latest_data["4. close"],
                "volume": latest_data["5. volume"]
            }
        else:
            logger.error(f"Error fetching data for {symbol}: {data.get('Error Message', 'Unknown error')}")
            return None
    except requests.RequestException as e:
        logger.error(f"Request failed for {symbol}: {str(e)}")
        return None

def send_to_kafka(topic, data):
    """Send data to Kafka topic."""
    try:
        producer.send(topic, value=data)
        producer.flush()
        messages_produced.inc()
        logger.info(f"Sent data for {data['symbol']} to Kafka")
    except Exception as e:
        logger.error(f"Failed to send data to Kafka: {str(e)}")

def main():
    """Main function to run the stock data producer."""
    start_http_server(METRICS_PORT)
    logger.info("Starting the producer...")

    try:
        while True:
            for symbol in SYMBOLS:
                latest_data = fetch_latest_stock_data(symbol)
                if latest_data:
                    send_to_kafka(KAFKA_TOPIC, latest_data)
                api_calls.inc()
                time.sleep(FETCH_INTERVAL / len(SYMBOLS))
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.close()
        logger.info("Producer shut down successfully.")

if __name__ == "__main__":
    main()
