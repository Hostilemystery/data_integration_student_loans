#envoie de données de crédit vers kafka
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import threading
import pandas as pd
import random

df = pd.read_excel("/Users/anthonycormeaux/Documents/Projet_data_integration/data_integration_student_loans/data/cleaned_FFEL_data.xlsx")

df = df.drop_duplicates(subset='School')

colonnes = ["School", "State","Zip Code", "School Type"]

df = df[colonnes]

def json_serializer(data):
    return json.dumps(data).encode('utf-8')
    

topic_name = 'quickstart'

if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer
    )

    for index, row in df.iterrows():
            message = {
                    "School": row["School"], 
                    "State": row["State"],
                    "Zip Code": row["Zip Code"],
                    "School Type": row["School Type"],
                    "timestamp": datetime.now().timestamp(),
                    "ffel_subsidized_recipients" : random.randint(100,5000),
                    "ffel_subsidized_number_of_loans_originated" : random.randint(500,8000),
                    "ffel_subsidized_amount_of_loans_originated" : random.randint(100000, 999999999),
                    "ffel_subsidized_number_of_disbursements" : random.randint(1000, 16000),

                    "ffel_unsubsidized_recipients" : random.randint(100,5000),
                    "ffel_unsubsidized_number_of_loans_originated" : random.randint(500,8000),
                    "ffel_unsubsidized_amount_of_loans_originated" : random.randint(100000, 999999999),
                    "ffel_unsubsidizednumber_of_disbursements" : random.randint(1000, 16000),

                    "ffel_stafford_recipients" : random.randint(100,5000),
                    "ffel_stafford_number_of_loans_originated" : random.randint(500,8000),
                    "ffel_stafford_amount_of_loans_originated" : random.randint(100000, 999999999),
                    "ffel_stafford_of_disbursements" : random.randint(1000, 16000),

                    "fffel_plus_recipients" : random.randint(100,5000),
                    "fffel_plus_number_of_loans_originated" : random.randint(500,8000),
                    "fffel_plus_amount_of_loans_originated" : random.randint(100000, 999999999),
                    "fffel_plus_of_disbursements" : random.randint(1000, 16000),

                    "Quarter_Start" : "04/01/2010",
                    "Quarter_End" : "06/30/2010"
                }
            producer.send(topic_name, value=message)
            print(f"Message envoyé pour {row}")


    producer.flush()
    producer.close()