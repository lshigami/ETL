from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
from .config import get_mongodb_config

def get_zscore_schema():
    """Define schema for Z-score data"""
    return StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("symbol", StringType(), True),
        StructField("zscores", ArrayType(
            StructType([
                StructField("window", StringType(), True),
                StructField("zscore_price", DoubleType(), True)
            ])
        ), True)
    ])

def get_collection_name(window: str) -> str:
    """Get MongoDB collection name for a specific window"""
    config = get_mongodb_config()
    for collection in config['mongodb']['collections']:
        if collection['window'] == window:
            return collection['name']
    raise ValueError(f"No collection found for window: {window}")

def get_all_collections() -> list:
    """Get list of all collection names"""
    config = get_mongodb_config()
    return [collection['name'] for collection in config['mongodb']['collections']]
