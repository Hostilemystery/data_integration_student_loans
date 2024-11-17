from kafka import KafkaConsumer
import json

# Configure the consumer to connect to Kafka and consume JSON messages
consumer = KafkaConsumer(
    'quickstart',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8') if x else None
)

print("En attente de messages...")
for message in consumer:
    if message.value:  # Check if the message is not empty or null
        try:
            data = json.loads(message.value)
            print("Message reçu:")
            for key, value in data.items():
                print(f" - {key}: {value}")
        except json.JSONDecodeError as e:
            print(f"Erreur de décodage JSON: {e}")
            print(f"Message non valide: {message.value}")
    else:
        print("Message vide ou nul ignoré.")

