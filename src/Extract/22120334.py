import threading
import time
from queue import Queue
from .code.binance_client import fetch_btcusdt_price
from .code.kafka_producer import KafkaProducer
from .code.timestamp_handler import get_rounded_timestamp
from .code.config import get_app_config

def fetch_and_produce(queue, frequency_ms, producer):
    """Hàm chạy trong mỗi thread để fetch dữ liệu và gửi vào Kafka."""
    interval = frequency_ms / 1000.0  
    while True:
        start_time = time.time()
        price_data = fetch_btcusdt_price()
        if price_data:
            price_data['timestamp'] = get_rounded_timestamp(frequency_ms)
            queue.put(price_data)
        elapsed_time = time.time() - start_time
        sleep_time = max(0, interval - elapsed_time)
        time.sleep(sleep_time)

def main():
    config = get_app_config()
    frequency_ms = config['frequency_ms']
    producer = KafkaProducer()
    data_queue = Queue()
    num_threads = 5  

    threads = []
    for _ in range(num_threads):
        thread = threading.Thread(target=fetch_and_produce, args=(data_queue, frequency_ms, producer))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    last_timestamp = None
    expected_next_time = time.time()
    interval = frequency_ms / 1000.0
    while True:
        current_time = time.time()
        if current_time >= expected_next_time:
            if not data_queue.empty():
                price_data = data_queue.get()
                current_timestamp = price_data['timestamp']
                if last_timestamp != current_timestamp:
                    producer.send_message(price_data)
                    last_timestamp = current_timestamp
                else:
                    print("Skipping duplicate timestamp")
            expected_next_time += interval
        time.sleep(frequency_ms / 5000.0) 

if __name__ == "__main__":
    main()