from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from .schema_handler import get_collection_name

class MongoDBConnector:
    def __init__(self, spark: SparkSession, mongo_uri: str, database: str):
        self.spark = spark
        self.mongo_uri = mongo_uri
        self.database = database

    def write_to_collections(self, df):
        """Write Z-score data to appropriate MongoDB collections"""
        # Explode the zscores array to get individual window records
        exploded_df = df.select(
            "timestamp",
            "symbol",
            explode("zscores").alias("zscore_data")
        ).select(
            "timestamp",
            "symbol",
            col("zscore_data.window").alias("window"),
            col("zscore_data.zscore_price").alias("zscore_price")
        )

        # Write to each collection based on window
        for window in ["30s", "1m", "5m", "15m", "30m", "1h"]:
            collection_name = get_collection_name(window)
            window_df = exploded_df.filter(col("window") == window)
            
            window_df.write \
                .format("com.mongodb.spark.sql") \
                .mode("append") \
                .option("uri", self.mongo_uri) \
                .option("database", self.database) \
                .option("collection", collection_name) \
                .save()
