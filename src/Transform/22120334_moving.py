from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, window, to_json, struct, collect_list, lit, coalesce, to_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging

# Thiết lập logging để ghi lại thông tin xử lý
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def init_spark_session(app_name="BTCPriceMovingAverage"):
    """
    Khởi tạo SparkSession với cấu hình cần thiết cho streaming Kafka.
    """
    logger.info("Khởi tạo SparkSession...")
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoint_data") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def get_data_schema_and_windows():
    """
    Định nghĩa schema dữ liệu đầu vào và các cửa sổ thời gian.
    """
    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), False)
    ])

    time_windows = {
        "30s": "30 SECONDS",
        "1m": "1 MINUTE",
        "5m": "5 MINUTES",
        "15m": "15 MINUTES",
        "30m": "30 MINUTES",
        "1h": "1 HOUR"
    }

    slide_durations = {
        "30 SECONDS": "5 SECONDS",
        "1 MINUTE": "10 SECONDS",
        "5 MINUTES": "50 SECONDS",
        "15 MINUTES": "2 MINUTES",
        "30 MINUTES": "5 MINUTES",
        "1 HOUR": "10 MINUTES"
    }

    return schema, time_windows, slide_durations

def fetch_kafka_stream(spark, kafka_server="localhost:9092", input_topic="btc-price"):
    """
    Đọc dữ liệu streaming từ Kafka topic.
    """
    logger.info(f"Đọc stream từ Kafka topic: {input_topic}")
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 100) \
        .option("failOnDataLoss", "false") \
        .load()
    return kafka_stream

def parse_kafka_data(stream_df, schema):
    """
    Phân tích dữ liệu JSON từ Kafka thành DataFrame có cấu trúc.
    """
    logger.info("Phân tích dữ liệu JSON từ Kafka...")
    parsed_df = stream_df \
        .select(from_json(col("value").cast("string"), schema).alias("parsed")) \
        .select(
            to_timestamp(col("parsed.timestamp")).alias("timestamp"),
            col("parsed.symbol").alias("symbol"),
            col("parsed.price").alias("price")
        )
    return parsed_df

def compute_window_stats(input_df, window_name, window_duration, slide_duration):
    """
    Tính trung bình và độ lệch chuẩn cho một cửa sổ thời gian cụ thể.
    """
    logger.info(f"Tính toán thống kê cho cửa sổ: {window_name}")
    watermarked_df = input_df.withWatermark("timestamp", "10 seconds")
    stats_df = watermarked_df \
        .groupBy(
            window(col("timestamp"), window_duration, slide_duration),
            col("symbol")
        ) \
        .agg(
            avg(col("price")).alias("mean_price"),
            stddev(col("price")).alias("std_dev_price")
        ) \
        .select(
            col("window.end").alias("timestamp"),
            col("symbol"),
            lit(window_name).alias("window"),
            col("mean_price"),
            coalesce(col("std_dev_price"), lit(0.0)).alias("std_dev_price")
        )
    return stats_df

def aggregate_windows(parsed_df, windows, slide_durations):
    """
    Gộp dữ liệu từ nhiều cửa sổ thời gian và định dạng đầu ra.
    """
    logger.info("Gộp dữ liệu từ các cửa sổ thời gian")
    combined_df = None
    for win_name, win_duration in windows.items():
        slide = slide_durations.get(win_duration, "10 SECONDS")
        window_stats_df = compute_window_stats(parsed_df, win_name, win_duration, slide)
        if combined_df is None:
            combined_df = window_stats_df
        else:
            combined_df = combined_df.unionByName(window_stats_df)

    formatted_df = combined_df \
        .groupBy("timestamp", "symbol") \
        .agg(
            collect_list(
                struct(
                    col("window"),
                    col("mean_price").alias("avg_price"),
                    col("std_dev_price").alias("std_price")
                )
            ).alias("windows")
        ) \
        .select("timestamp", "symbol", "windows")
    return formatted_df

def publish_to_kafka(output_df, kafka_server="localhost:9092", output_topic="btc-price-moving"):
    """
    Ghi dữ liệu streaming vào Kafka topic.
    """
    logger.info(f"Ghi dữ liệu vào Kafka topic: {output_topic}")
    kafka_output = output_df \
        .select(
            to_json(
                struct("timestamp", "symbol", "windows")
            ).alias("value")
        ) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("topic", output_topic) \
        .outputMode("update") \
        .start()
    kafka_output.awaitTermination()

def execute_pipeline():
    """
    Thực thi toàn bộ pipeline xử lý dữ liệu.
    """
    try:
        # Bước 1: Khởi tạo SparkSession
        spark_session = init_spark_session()

        # Bước 2: Lấy schema và cửa sổ thời gian
        data_schema, time_windows, slide_durations = get_data_schema_and_windows()

        # Bước 3: Đọc stream từ Kafka
        kafka_stream = fetch_kafka_stream(spark_session)

        # Bước 4: Phân tích dữ liệu từ Kafka
        parsed_data = parse_kafka_data(kafka_stream, data_schema)

        # Bước 5: Tính toán và gộp dữ liệu từ các cửa sổ
        final_output = aggregate_windows(parsed_data, time_windows, slide_durations)

        # Bước 6: Ghi kết quả vào Kafka
        publish_to_kafka(final_output)

    except Exception as e:
        logger.error(f"Lỗi trong pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    execute_pipeline()