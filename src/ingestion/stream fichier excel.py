#lis les données du fichier excel et les envoie vers kafka
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import threading
import pandas as pd

df = pd.read_excel(r"/Users/anthonycormeaux/Downloads/1617fedschoolcodelist.xlsx")


def json_serializer(data):
    return json.dumps(data).encode('utf-8')

topic_name = 'quickstart'

if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer
    )

    for index, row in df.iterrows():
        for column_name, value in row.items():
            message = {
                "SchoolCode": row["SchoolCode"], 
                "SchoolName": row["SchoolName"],
                "Address": row["Address"],
                "City": row["City"],
                "StateCode" : row["StateCode"],
                "ZipCode" : row["ZipCode"],
                "Country" : row["Country"],
                "Country" : row["Country"],
                "timestamp": datetime.now().timestamp()
            }
            producer.send(topic_name, value=message)
            print(f"Message envoyé pour {column_name} de {row['SchoolCode']}: {message}")

    producer.flush()
    producer.close()
