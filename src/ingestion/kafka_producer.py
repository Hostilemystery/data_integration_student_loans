# Kafka producer to stream data

import os
import time
import json
from kafka import KafkaProducer
from config.kafka_config import KAFKA_BROKER_URL, TOPIC_NAME_FL_DASHBOARD, BATCH_SIZE, POLL_INTERVAL

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
)

def read_data_in_batches(file_path, batch_size):
    """Reads data from file in batches."""
    with open(file_path, 'r') as file:
        batch = []
        for line in file:
            batch.append(line.strip())
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch  # Yield any remaining data

def send_data_to_kafka(topic, file_path):
    """Sends data from the file to Kafka in batches."""
    for batch in read_data_in_batches(file_path, BATCH_SIZE):
        for record in batch:
            producer.send(topic, value=record)  # Send each record to Kafka
        producer.flush()  # Ensure data is sent before sleeping
        print(f"Sent batch of {len(batch)} records to Kafka topic '{topic}'")
        time.sleep(POLL_INTERVAL)

def main():
    # Path to raw data files
    raw_data_dir = os.path.join("data", "raw")
    
    # Send FL_Dashboard files
    for filename in os.listdir(raw_data_dir):
        if filename.startswith("FL_Dashboard"):
            file_path = os.path.join(raw_data_dir, filename)
            print(f"Streaming data from file: {file_path}")
            send_data_to_kafka(TOPIC_NAME_FL_DASHBOARD, file_path)

if __name__ == "__main__":
    main()
    producer.close()