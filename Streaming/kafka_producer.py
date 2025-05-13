from kafka import KafkaProducer
import json
import time
from datetime import datetime
import pandas as pd

excel_file_path = r"Streaming/1617fedschoolcodelist.xls"

df = pd.read_excel(excel_file_path)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

topic_name = 'excel_data'

batch_size = 100

sleep_time = 10

if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer
    )

    total_rows = len(df)
    print(f"Nombre total de lignes à envoyer: {total_rows}")

    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        batch = df.iloc[start:end]
        batch_number = (start // batch_size) + 1
        print(f"\nEnvoi du batch {batch_number}: lignes {start + 1} à {min(end, total_rows)}")

        for index, row in batch.iterrows():
            message = {
                "SchoolCode": row["SchoolCode"], 
                "SchoolName": row["SchoolName"],
                "Address": row["Address"],
                "City": row["City"],
                "StateCode" : row["StateCode"],
                "ZipCode" : row["ZipCode"],
                "Country" : row["Country"],
                "timestamp": datetime.now().timestamp()
            }
            producer.send(topic_name, value=message)
            print(f"Message envoyé pour {row['SchoolCode']} (Batch {batch_number})")

        producer.flush()
        print(f"Batch {batch_number} envoyé. Pause de {sleep_time} secondes.")
        time.sleep(sleep_time)

    producer.close()
    print("\nTous les messages ont été envoyés et le producteur Kafka est fermé.")
