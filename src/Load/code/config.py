import yaml
import os

def load_yaml_config(config_file):
    """Load configuration from YAML file"""
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', config_file)
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_mongodb_config():
    """Get MongoDB configuration"""
    return load_yaml_config('mongodb.yaml')

def get_kafka_config():
    """Get Kafka configuration"""
    return load_yaml_config('kafka.yaml')

def get_spark_config():
    """Get Spark configuration"""
    return load_yaml_config('spark.yaml') 