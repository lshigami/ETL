# Binance Data Processing Pipeline

## Prerequisites

Before you begin, ensure you have met the following requirements:

- Python 3.8+
- Pip & Virtualenv
- Access to a running Kafka cluster
- Access to a running Apache Spark cluster (or local Spark installation)
- Access to a running MongoDB instance
- Binance API Key and Secret (if `binance_client.py` requires authentication for the data you intend to fetch)

## How to Run

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/lshigami/ETL.git
    cd ETL
    ```

2.  **Create and activate a virtual environment:**

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure the application:**

    - Navigate to the `config/` directory.
    - Update `kafka.yaml`, `spark.yaml`, `mongodb.yaml`, and `app.yaml` with your specific broker addresses, cluster details, database credentials, API keys, and other relevant settings.
    - **Example:** You might need to set your Binance API key in `config/app.yaml` or an environment variable that `extract/config.py` reads.

5.  **Run the application components:**
    The project is modular. You'll likely need to run different main executables for different parts of the pipeline.

    - **To run the data extraction module (producer):**
      ```bash
      python -m src.extract.main
      ```
    - **To run a transformation job (e.g., moving statistics consumer & processor):**
      ```bash
      # This might involve submitting a Spark job.
      # The exact command will depend on your Spark setup and how moving_stats.py is structured.
      # Example for a local Spark run if moving_stats.py is a Spark application:
      # spark-submit src/transform/moving_stats.py
      # Or, if it's a Python script that initializes a Spark session:
      python -m src.transform.moving_stats
      ```
    - **To run the data loading module (consumer & writer):**
      ```bash
      python -m src.load.main
      ```
    - **To run the bonus module:**
      ```bash
      python -m src.bonus.main
      ```

## Project Structure

```bash
src/
├── extract/
│   ├── __init__.py
│   ├── main.py                            # Main executable
│   ├── binance_client.py                  # Binance API client
│   ├── kafka_producer.py                  # Kafka producer logic
│   ├── timestamp_handler.py               # Timestamp processing & rounding
│   └── config.py                          # Configuration settings
│
├── transform/
│   ├── __init__.py
│   ├── moving_stats.py                    # Moving statistics executable
│   ├── zscore_calc.py                     # Z-score calculation executable
│   ├── spark_session.py                  # Spark session setup
│   ├── kafka_consumer.py                 # Kafka consumer utilities
│   ├── moving_statistics.py              # Moving avg & std calculations
│   ├── zscore_calculator.py              # Z-score computation
│   ├── window_operations.py              # Sliding window operations
│   └── stream_joiner.py                  # Stream-to-stream join operations
│
├── load/
│   ├── __init__.py
│   ├── main.py                           # Main executable
│   ├── mongodb_connector.py              # MongoDB connection & operations
│   ├── stream_writer.py                  # Stream writing to MongoDB
│   └── schema_handler.py                 # MongoDB schema definitions
│
├── bonus/
│   ├── __init__.py
│   ├── main.py                           # Bonus executable
│   ├── price_window_finder.py            # Find higher/lower price windows
│   └── stateful_operations.py            # Stateful stream operations
│
├── shared/
│   ├── __init__.py
│   ├── kafka_config.py                   # Shared Kafka configurations
│   ├── spark_config.py                   # Shared Spark configurations
│   ├── utils.py                          # Common utility functions
│   └── constants.py                      # Application constants
│
├── config/
│   ├── kafka.yaml                       # Kafka configuration
│   ├── spark.yaml                       # Spark configuration
│   ├── mongodb.yaml                     # MongoDB configuration
│   └── app.yaml                         # Application configuration
│
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_load.py
│   └── test_bonus.py
│
└── requirements.txt                      # Python dependencies
```

## Configuration

All major configurations for the application services (Kafka, Spark, MongoDB) and general application settings are managed through YAML files located in the `config/` directory.

- `config/kafka.yaml`: Kafka broker details, topic names, consumer/producer settings.
- `config/spark.yaml`: Spark master URL, application name, executor memory, and other Spark configurations.
- `config/mongodb.yaml`: MongoDB connection URI, database name, collection names.
- `config/app.yaml`: Application-specific settings like Binance API keys (if not using environment variables), processing parameters, etc.

Make sure to review and update these files with your environment-specific details before running the application.
