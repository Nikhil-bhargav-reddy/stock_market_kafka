# Basic Real-time Stock Market Data Pipeline with Kafka

This project demonstrates a real-time stock market data pipeline using Apache Kafka, with monitoring capabilities provided by Prometheus and Grafana.

## Project Overview

The pipeline fetches stock market data from the Alpha Vantage API, produces it to a Kafka topic, consumes it, and provides real-time monitoring of the system.

### Components

- Kafka Producer: Fetches stock data and sends it to Kafka
- Kafka Consumer: Reads data from Kafka and processes it
- Kafka & Zookeeper: Message broker and distributed systems coordinator
- Prometheus: Metrics collection and storage
- Grafana: Metrics visualization and dashboarding

## Prerequisites

- Docker and Docker Compose
- Python 3.7+
- Alpha Vantage API key or any other API 
