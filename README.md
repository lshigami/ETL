# BTC Price Analysis ETL Pipeline

A real-time Bitcoin price analysis pipeline using Apache Spark, Kafka, and MongoDB for Extract-Transform-Load operations.

## Project Structure

```
src/
├── Extract/
│   ├── 22120334.py                 # Main extraction script
│   └── code/
│       ├── binance_client.py       # Binance API client
│       ├── kafka_producer.py       # Kafka message producer
│       ├── timestamp_handler.py    # Timestamp utilities
│       └── config.py               # Configuration loader
├── Transform/
│   ├── 22120334_moving.py          # Moving averages calculation
│   └── 22120334_zscore.py          # Z-score calculation
├── Load/
│   ├── 22120334.py                 # Main loading script
│   └── code/
│       ├── stream_writer.py        # Kafka to MongoDB writer
│       ├── mongodb_connector.py    # MongoDB connection handler
│       ├── schema_handler.py       # Data schema definitions
│       └── config.py               # Configuration loader
├── config/
│   ├── app.yaml                    # Application settings
│   ├── kafka.yaml                  # Kafka configuration
│   ├── mongodb.yaml                # MongoDB configuration
│   └── spark.yaml                  # Spark configuration
├── docker-compose.yml              # Infrastructure services
├── requirements.txt                # Python dependencies
└── Makefile                        # Build and run commands
```

## Prerequisites

- **Python 3.8+**
- **Apache Spark 3.5.0+** with Kafka support
- **Docker & Docker Compose** (for infrastructure)
- **Java 8 or 11** (required for Spark)

## Quick Start

### 1. Start Infrastructure Services

Navigate to the `src/` directory and start Kafka, Zookeeper, and MongoDB:

```bash
cd src
docker-compose up -d
```

Verify services are running:
```bash
docker-compose ps
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Pipeline

The pipeline consists of multiple components that should be run in separate terminals:

#### Terminal 1: Extract (Data Producer)
```bash
cd src
python -m Extract.22120334
```
- Fetches BTC/USDT price data from Binance API every 100ms
- Publishes to Kafka topic `btc-price`
- Uses 5 concurrent threads for data fetching

#### Terminal 2: Transform - Moving Statistics
```bash
cd src/Transform
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 22120334_moving.py
```
- Calculates moving averages and standard deviations
- Processes multiple time windows: 30s, 1m, 5m, 15m, 30m, 1h
- Publishes results to `btc-price-moving` topic

#### Terminal 3: Transform - Z-Score Calculation
```bash
cd src/Transform
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 22120334_zscore.py
```
- Joins price data with moving statistics
- Calculates Z-scores for price deviations
- Publishes results to `btc-price-zscore` topic

#### Terminal 4: Load (Data Consumer)
```bash
cd src
python -m Load.22120334
```
- Consumes Z-score data from Kafka
- Writes to MongoDB collections by time window
- Real-time streaming using Spark Structured Streaming

## Configuration Files

### Application Settings (`config/app.yaml`)
```yaml
binance:
  api_url: "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
frequency_ms: 100  # Data fetch frequency
```

### Kafka Settings (`config/kafka.yaml`)
```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "btc-price"              # Raw price data
  topics:
    zscore: "btc-price-zscore"    # Z-score results
```

### MongoDB Settings (`config/mongodb.yaml`)
```yaml
mongodb:
  uri: "mongodb://admin:B1n4nc3@localhost:27017"
  database: "crypto_analysis"
  collections:
    - name: "btc-price-zscore-30s"
    - name: "btc-price-zscore-1m"
    # ... other time windows
```

## Using Makefile Commands

The project includes a Makefile for common operations:

```bash
# Install package in development mode
make install

# Run extraction module
make run-extract

# Run moving averages calculation
make run-moving

# Run Z-score calculation
make run-zscore

# Run data loading module
make run-load

# Clean up generated files
make clean
```

## Data Flow

1. **Extract**: [`Extract/22120334.py`](src/Extract/22120334.py) fetches BTC price → Kafka topic `btc-price`
2. **Transform**: 
   - [`Transform/22120334_moving.py`](src/Transform/22120334_moving.py) calculates statistics → `btc-price-moving`
   - [`Transform/22120334_zscore.py`](src/Transform/22120334_zscore.py) computes Z-scores → `btc-price-zscore`
3. **Load**: [`Load/22120334.py`](src/Load/22120334.py) writes to MongoDB collections

## MongoDB Collections

Data is stored in separate collections by time window:
- `btc-price-zscore-30s` - 30-second window Z-scores
- `btc-price-zscore-1m` - 1-minute window Z-scores
- `btc-price-zscore-5m` - 5-minute window Z-scores
- `btc-price-zscore-15m` - 15-minute window Z-scores
- `btc-price-zscore-30m` - 30-minute window Z-scores
- `btc-price-zscore-1h` - 1-hour window Z-scores

## Monitoring

### Check Kafka Topics
```bash
# List topics
docker exec -it $(docker ps -q --filter "name=kafka") kafka-topics --bootstrap-server localhost:9092 --list

# Monitor messages
docker exec -it $(docker ps -q --filter "name=kafka") kafka-console-consumer --bootstrap-server localhost:9092 --topic btc-price
```

### Check MongoDB Data
```bash
# Connect to MongoDB
docker exec -it mongo mongosh -u admin -p B1n4nc3

# Query data
use crypto_analysis
db["btc-price-zscore-1m"].find().limit(5)
```

## Dependencies

Key packages from [`requirements.txt`](src/requirements.txt):
- `pyspark==3.5.1` - Spark processing engine
- `confluent-kafka==2.3.0` - Kafka Python client
- `pymongo==4.3.3` - MongoDB Python driver
- `requests==2.28.1` - HTTP client for Binance API
- `pyyaml==6.0` - YAML configuration parsing