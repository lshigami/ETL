from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from .schema_handler import get_zscore_schema
from .mongodb_connector import MongoDBConnector
from .config import get_kafka_config
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamWriter:
    def __init__(self, spark: SparkSession, kafka_bootstrap_servers: str, 
                 mongo_uri: str, database: str):
        self.spark = spark
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.mongo_connector = MongoDBConnector(spark, mongo_uri, database)
        self.kafka_config = get_kafka_config()
        logger.info("StreamWriter initialized with Kafka bootstrap servers: %s", kafka_bootstrap_servers)

    def start_streaming(self):
        """Start reading from Kafka and writing to MongoDB"""
        try:
            # Read from Kafka
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", self.kafka_config['kafka']['topics']['zscore']) \
                .option("startingOffsets", self.kafka_config['kafka']['consumer']['auto_offset_reset']) \
                .option("kafka.max.poll.records", self.kafka_config['kafka']['consumer']['max_poll_records']) \
                .option("kafka.session.timeout.ms", self.kafka_config['kafka']['consumer']['session_timeout_ms']) \
                .load()

            logger.info("Successfully connected to Kafka topic: %s", 
                       self.kafka_config['kafka']['topics']['zscore'])

            # Parse JSON and apply schema
            parsed_df = df.select(
                from_json(
                    col("value").cast("string"),
                    get_zscore_schema()
                ).alias("data")
            ).select("data.*")

            # Write to MongoDB using foreachBatch
            query = parsed_df.writeStream \
                .foreachBatch(self._write_batch) \
                .outputMode("append") \
                .start()

            logger.info("Streaming query started successfully")
            return query

        except Exception as e:
            logger.error("Error starting streaming: %s", str(e))
            raise

    def _write_batch(self, batch_df, batch_id):
        """Write a batch of data to MongoDB"""
        try:
            if batch_df.count() > 0:
                logger.info("Processing batch %d with %d records", batch_id, batch_df.count())
                self.mongo_connector.write_to_collections(batch_df)
                logger.info("Successfully wrote batch %d to MongoDB", batch_id)
            else:
                logger.debug("Batch %d is empty, skipping", batch_id)
        except Exception as e:
            logger.error("Error processing batch %d: %s", batch_id, str(e))
            raise
