import pandas as pd
import csv
import json
import logging
from kafka import KafkaProducer

# Set up logging
logging.basicConfig(level=logging.INFO)

# Kafka producer setup
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'churn'


def on_send_success(record_metadata):
    logging.info(
        f"Message sent to topic {record_metadata.topic} partition {record_metadata.partition} at offset {record_metadata.offset}")


def on_send_error(excp):
    logging.error('Message delivery failed: ', exc_info=excp)


def stream_csv_file(file_name):
    # Define the column names
    column_names = ["Account length", "International plan", "Voice mail plan",
                    "Number vmail messages", "Total day minutes", "Total day calls",
                    "Total eve minutes", "Total eve calls", "Total night minutes",
                    "Total night calls", "Total intl minutes", "Total intl calls",
                    "Customer service calls", "Churn"]

    # Read the CSV file and assign column names
    df = pd.read_csv(file_name, names=column_names)

    # Iterate over each row in the dataframe
    for index, row in df.iterrows():
        # Print the data being sent
        print("Data being sent:", row.to_dict())

        # Convert the row to JSON and send it to Kafka
        data = {
            'Account length': row['Account length'],
            'International plan': row['International plan'],
            'Voice mail plan': row['Voice mail plan'],
            'Number vmail messages': row['Number vmail messages'],
            'Total day minutes': row['Total day minutes'],
            'Total day calls': row['Total day calls'],
            'Total eve minutes': row['Total eve minutes'],
            'Total eve calls': row['Total eve calls'],
            'Total night minutes': row['Total night minutes'],
            'Total night calls': row['Total night calls'],
            'Total intl minutes': row['Total intl minutes'],
            'Total intl calls': row['Total intl calls'],
            'Customer service calls': row['Customer service calls'],
            'Churn': row['Churn'],
            'columns': column_names
        }
        producer.send(topic_name, value=json.dumps(data).encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)



if __name__ == '__main__':
    stream_csv_file('churn-bigml-20.csv')
