import yaml
import os

def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def get_kafka_config():
    return load_config(os.path.join(os.path.dirname(__file__), '../config/kafka.yaml'))

def get_app_config():
    return load_config(os.path.join(os.path.dirname(__file__), '../config/app.yaml'))