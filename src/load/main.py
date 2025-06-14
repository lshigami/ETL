from pyspark.sql import SparkSession
from .stream_writer import StreamWriter
from .config import get_mongodb_config, get_kafka_config, get_spark_config

def main():
    # Load configurations
    spark_config = get_spark_config()
    mongo_config = get_mongodb_config()
    kafka_config = get_kafka_config()

    # Initialize Spark Session
    spark_builder = SparkSession.builder \
        .appName(spark_config['spark']['app_name'])

    # Add packages (join all packages with comma)
    if spark_config['spark']['packages']:
        packages = ",".join(spark_config['spark']['packages'])
        spark_builder = spark_builder.config("spark.jars.packages", packages)

    # Add additional configs
    for key, value in spark_config['spark']['configs'].items():
        spark_builder = spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()

    # Create and start stream writer
    writer = StreamWriter(
        spark=spark,
        kafka_bootstrap_servers=kafka_config['kafka']['bootstrap_servers'],
        mongo_uri=mongo_config['mongodb']['uri'],
        database=mongo_config['mongodb']['database']
    )

    # Start streaming
    query = writer.start_streaming()
    query.awaitTermination()

if __name__ == "__main__":
    main()
