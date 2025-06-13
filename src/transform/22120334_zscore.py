from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import json

KAFKA_SERVER_ADDRESS = "localhost:9092"
RAW_PRICE_TOPIC = "btc-price"
MOVING_STATS_TOPIC = "btc-price-moving"
ZSCORE_OUTPUT_TOPIC = "btc-price-zscore"

def init_spark_session() -> SparkSession:
    spark_session = (
        SparkSession.builder
        .appName("BTCZScoreCalculation")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.checkpointLocation", "./checkpoints/zscore_main")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )
    
    spark_session.sparkContext.setLogLevel("ERROR")
    
    return spark_session

def get_price_schema():
    price_schema = StructType([
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("timestamp", TimestampType())
    ])
    
    return price_schema

def get_moving_schema():
    window_struct = StructType([
        StructField("window", StringType()),
        StructField("avg_price", DoubleType()),
        StructField("std_price", DoubleType())
    ])
    
    stats_schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("symbol", StringType()),
        StructField("windows", ArrayType(window_struct))
    ])
    
    return stats_schema

def stream_to_kafka(final_data, checkpoint_dir, topic):
    return final_data.select("value").writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER_ADDRESS) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_dir) \
        .outputMode("append") \
        .start()

def stream_to_console(final_data):
    return final_data.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("append") \
        .start()

def load_price_stream(spark_session):
    raw_data = spark_session \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER_ADDRESS) \
        .option("subscribe", RAW_PRICE_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    price_schema = get_price_schema()
    
    parsed_data = raw_data.select(
        from_json(col("value").cast("string"), price_schema).alias("data")
    ).select("data.*")
    parsed_data = parsed_data.withWatermark("timestamp", "10 seconds")
    
    return parsed_data

def load_moving_stream(spark_session):
    stats_data = spark_session \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER_ADDRESS) \
        .option("subscribe", MOVING_STATS_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    
    stats_schema = get_moving_schema()

    parsed_data = stats_data.select(
        from_json(col("value").cast("string"), stats_schema).alias("data")
    ).select("data.*")
    parsed_data = parsed_data.withWatermark("timestamp", "10 seconds")
    
    flattened_data = parsed_data.select(
        col("timestamp"),
        col("symbol"),
        explode(col("windows")).alias("window_data")
    ).select(
        col("timestamp"),
        col("symbol"),
        col("window_data.window").alias("window"),
        col("window_data.avg_price").alias("avg_price"),
        col("window_data.std_price").alias("std_price")
    )
    
    return flattened_data

def compute_zscores(price_data, moving_data):
    price_data = price_data.select(
        col("timestamp").alias("price_timestamp"),
        col("symbol").alias("price_symbol"),
        col("price")
    )
    
    moving_data = moving_data.select(
        col("timestamp").alias("stats_timestamp"),
        col("symbol").alias("stats_symbol"),
        col("window"),
        col("avg_price"),
        col("std_price")
    )
    
    combined_data = price_data.join(
        moving_data,
        (price_data.price_timestamp == moving_data.stats_timestamp) & 
        (price_data.price_symbol == moving_data.stats_symbol),
        "inner"
    )

    zscore_data = combined_data.withColumn(
        "zscore_price",
        when(
            (col("std_price") > 0.0001), 
            (col("price") - col("avg_price")) / col("std_price")
        ).otherwise(lit(0.0))
    ).select(
        col("price_timestamp").alias("timestamp"),
        col("price_symbol").alias("symbol"),
        col("window"),
        col("zscore_price")
    )
    
    return zscore_data

def prepare_zscore_output(zscore_data):
    unique_data = zscore_data \
        .groupBy("timestamp", "symbol", "window") \
        .agg(
            max("zscore_price").alias("zscore_price")
        )
    
    output_data = unique_data \
        .groupBy("timestamp", "symbol") \
        .agg(
            collect_list(
                struct(
                    col("window"),
                    col("zscore_price")
                )
            ).alias("zscores")
        )
    
    final_data = output_data.select(
        col("timestamp"),
        col("symbol"),
        col("zscores"),
        to_json(
            struct(
                col("timestamp"),
                col("symbol"),
                col("zscores")
            )
        ).alias("value")
    )
    
    return final_data

def run_pipeline():
    spark_session = init_spark_session()
    price_data = load_price_stream(spark_session)
    moving_data = load_moving_stream(spark_session)
    zscore_data = compute_zscores(price_data, moving_data)
    final_data = prepare_zscore_output(zscore_data)

    kafka_stream = stream_to_kafka(
        final_data,
        checkpoint_dir="../checkpoints/zscore",
        topic=ZSCORE_OUTPUT_TOPIC
    )
    console_stream = stream_to_console(final_data)

    kafka_stream.awaitTermination()
    console_stream.awaitTermination()

if __name__ == "__main__":
    run_pipeline()