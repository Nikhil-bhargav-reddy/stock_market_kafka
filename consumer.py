import json
import logging
from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC = 'stock_market_data'
GROUP_ID = 'stock_data_group'

# Prometheus configuration
METRICS_PORT = 8001
messages_consumed = Counter('messages_consumed', 'Number of messages consumed')

def create_consumer():
    """Create and return a Kafka consumer instance."""
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def process_message(stock_data):
    """Process and log the received stock data."""
    logger.info(f"Received stock data: {stock_data}")
    logger.info(f"Symbol: {stock_data['symbol']}")
    logger.info(f"Date: {stock_data['date']}")
    logger.info(f"Close Price: {stock_data['close']}")
    logger.info(f"Volume: {stock_data['volume']}")
    logger.info("-------------------------")
    messages_consumed.inc()

def main():
    """Main function to run the Kafka consumer."""
    start_http_server(METRICS_PORT)
    logger.info("Starting the consumer...")
    logger.info("Waiting for messages...")

    consumer = create_consumer()
    
    try:
        for message in consumer:
            process_message(message.value)
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        logger.info("Consumer shut down successfully.")

if __name__ == "__main__":
    main()
